import heapq
import queue
import time
import collections
import multiprocessing
from core.functional_core import verify_signature, compute_average

class CoreWorker:

    SENTINEL = None

    def __init__(
        self,
        worker_id: int,
        config: dict,
        raw_queue: multiprocessing.Queue,
        intermediate_queue: multiprocessing.Queue,
    ):
        self.worker_id          = worker_id
        sig                     = config["processing"]["stateless_tasks"]
        self.secret_key         = sig["secret_key"]
        self.iterations         = sig["iterations"]
        self.raw_queue          = raw_queue
        self.intermediate_queue = intermediate_queue

    def run(self) -> None:
        verified_count = 0
        dropped_count  = 0

        print(f"[Worker {self.worker_id}] Started.")

        while True:
            packet = self.raw_queue.get()

            if packet is self.SENTINEL:
                print(f"[Worker {self.worker_id}] Sentinel received — "
                      f"verified={verified_count}, dropped={dropped_count}")

                self.intermediate_queue.put(self.SENTINEL)
                break

            priority_index = packet["priority_index"]

            if verify_signature(packet, self.secret_key, self.iterations):

                self.intermediate_queue.put((priority_index, packet))
                verified_count += 1
                print(f"[Worker {self.worker_id}] Verified   #{priority_index}")
            else:
                dropped_count += 1
                print(f"[Worker {self.worker_id}] Dropped    #{priority_index} "
                      f"(signature mismatch)")

CUTOFF_SECONDS = 2.0

class Aggregator:

    def __init__(
        self,
        config: dict,
        intermediate_queue: multiprocessing.Queue,
        processed_queue: multiprocessing.Queue,
        num_workers: int,
        start_event=None,
    ):
        window_size = config["processing"]["stateful_tasks"]["running_average_window_size"]

        self.intermediate_queue = intermediate_queue
        self.processed_queue    = processed_queue
        self.num_workers        = num_workers

        self.heap           = []
        self.window         = collections.deque(maxlen=window_size)
        self.next_expected  = 1
        self.last_seen_time = None
        self.sentinels_seen = 0
        self._start_event   = start_event

    def _try_release(self) -> None:
        while self.heap and self.heap[0][0] == self.next_expected:
            _, packet = heapq.heappop(self.heap)

            self.window.append(packet["metric_value"])

            avg = compute_average(list(self.window))

            packet["computed_metric"] = round(avg, 4)
            self.processed_queue.put(packet)

            print(f"[Aggregator] Released #{self.next_expected:>4} | "
                  f"metric={packet['metric_value']:.2f} | "
                  f"avg={packet['computed_metric']:.4f} | "
                  f"window={len(self.window)}")

            self.next_expected  += 1
            self.last_seen_time  = time.time()

    def _apply_cutoff(self) -> None:
        if self.last_seen_time is not None and time.time() - self.last_seen_time > CUTOFF_SECONDS:
            print(f"[Aggregator] Timeout — skipping #{self.next_expected} "
                  f"(assumed dropped by worker)")
            self.next_expected  += 1
            self.last_seen_time  = time.time()
            self._try_release()

    def _drain_remaining(self) -> None:

        time.sleep(0.5)

        while True:
            try:
                item = self.intermediate_queue.get_nowait()
                if item is not None:
                    priority_index, packet = item
                    if priority_index >= self.next_expected:
                        heapq.heappush(self.heap, (priority_index, packet))
            except queue.Empty:
                break

        while self.heap:
            next_in_heap = self.heap[0][0]

            while self.next_expected < next_in_heap:
                print(f"[Aggregator] Drain — skipping missing #{self.next_expected}")
                self.next_expected += 1

            if self.heap and self.heap[0][0] < self.next_expected:
                heapq.heappop(self.heap)
                continue

            self._try_release()

    def run(self) -> None:
        print(f"[Aggregator] Started — waiting for {self.num_workers} workers.")

        if self._start_event is not None:
            self._start_event.wait()

        self.last_seen_time = time.time()

        while True:

            try:
                item = self.intermediate_queue.get(timeout=0.1)
            except queue.Empty:

                if self.sentinels_seen < self.num_workers:
                    self._apply_cutoff()
                continue

            if item is None:
                self.sentinels_seen += 1
                print(f"[Aggregator] Worker done "
                      f"({self.sentinels_seen}/{self.num_workers})")

                if self.sentinels_seen == self.num_workers:
                    print("[Aggregator] All workers done — draining heap ...")
                    self._drain_remaining()
                    break
                continue

            priority_index, packet = item

            if priority_index < self.next_expected:
                print(f"[Aggregator] Ignoring stale #{priority_index} "
                      f"(already skipped by timeout)")
                continue

            heapq.heappush(self.heap, (priority_index, packet))
            self._try_release()

        self.processed_queue.put(None)
        print("[Aggregator] Done — sentinel sent to Output.")