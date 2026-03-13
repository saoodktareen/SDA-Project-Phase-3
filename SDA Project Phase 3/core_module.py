"""
core_module.py — Generic Core: Verification Workers + Aggregator
=================================================================
This module is completely domain-agnostic. It never imports anything
related to sensors, GDP, or any specific dataset.

Two classes live here:

1. CoreWorker  (Stateless, Parallel)
   - Pulls packets from raw_queue (Queue 1)
   - Verifies the cryptographic signature using PBKDF2-HMAC
   - Drops packets that fail verification
   - Pushes verified (packet_id, packet) into intermediate_queue (Queue 2)
   - Implements the Scatter-Gather pattern — N workers run in parallel

2. Aggregator  (Stateful, Single Process)
   - Pulls verified packets from intermediate_queue (Queue 2)
   - Re-sequences them using a min-heap priority queue (teacher's solution)
   - Applies a timeout/cutoff for packets that never arrive (dropped by workers)
   - Computes sliding window running average using Functional Core pattern
   - Pushes fully processed packets into processed_queue (Queue 3)

Patterns implemented:
  - Scatter-Gather         : N parallel CoreWorker processes
  - Functional Core        : pure compute_average() function
  - Imperative Shell       : Aggregator class manages all state
  - Priority Queue         : heapq for re-sequencing
  - Timeout/Cutoff         : skips missing packet IDs after CUTOFF_SECONDS
"""

import hashlib
import heapq
import queue
import time
import collections
import multiprocessing


# ─────────────────────────────────────────────────────────────
#  FUNCTIONAL CORE — Pure Functions (no side effects, no state)
# ─────────────────────────────────────────────────────────────

def verify_signature(packet: dict, secret_key: str, iterations: int) -> bool:
    """
    Pure function — verifies the cryptographic signature of a packet.

    Signature scheme (verified against sample_sensor_data.csv):
        password = secret_key (encoded as UTF-8 bytes)
        salt     = metric_value formatted to 2 decimal places (encoded as UTF-8 bytes)
        hash_fn  = sha256
        iters    = iterations (100,000)

    Args:
        packet      : generic data packet from InputModule
        secret_key  : secret key string from config
        iterations  : number of PBKDF2 iterations from config

    Returns:
        True if computed hash matches packet's security_hash, False otherwise.
    """
    key           = secret_key.encode("utf-8")
    raw_value_str = f"{packet['metric_value']:.2f}"
    salt          = raw_value_str.encode("utf-8")
    expected_hash = packet["security_hash"]

    computed_hash = hashlib.pbkdf2_hmac(
        "sha256",
        key,
        salt,
        iterations
    ).hex()

    return computed_hash == expected_hash


def compute_average(window: list) -> float:
    """
    Pure function — computes the arithmetic mean of a list of numbers.

    This is the Functional Core of the running average calculation.
    It has no side effects, no state, and is fully testable in isolation.

    Args:
        window : list of float values (the current sliding window)

    Returns:
        The arithmetic mean as a float. Returns 0.0 for empty window.
    """
    if not window:
        return 0.0
    return sum(window) / len(window)


# ─────────────────────────────────────────────────────────────
#  CoreWorker — Stateless Parallel Verification Process
# ─────────────────────────────────────────────────────────────

class CoreWorker:
    """
    Stateless parallel worker — implements the Scatter part of Scatter-Gather.

    Each CoreWorker runs as an independent multiprocessing.Process.
    Multiple workers pull from the same raw_queue concurrently, so
    packets are processed in parallel across CPU cores.

    The worker is completely unaware of:
      - What the data represents (sensor, climate, health metrics, etc.)
      - How many other workers exist
      - What happens to packets after verification

    It only knows: pull packet → verify → forward or drop.
    """

    SENTINEL = None  # Poison pill signal sent by main.py to stop workers

    def __init__(
        self,
        worker_id: int,
        config: dict,
        raw_queue: multiprocessing.Queue,
        intermediate_queue: multiprocessing.Queue,
    ):
        """
        Args:
            worker_id          : integer ID for logging (0, 1, 2, 3)
            config             : full config.json dict
            raw_queue          : Queue 1 — pulls packets from here
            intermediate_queue : Queue 2 — pushes verified packets here
        """
        self.worker_id          = worker_id
        sig                     = config["processing"]["stateless_tasks"]
        self.secret_key         = sig["secret_key"]
        self.iterations         = sig["iterations"]
        self.raw_queue          = raw_queue
        self.intermediate_queue = intermediate_queue

    def run(self) -> None:
        """
        Main loop — runs inside a multiprocessing.Process.

        Continuously pulls packets from raw_queue until it receives
        the sentinel (None), which signals the stream has ended.

        For each packet:
          - Calls verify_signature() (pure function)
          - If valid: forwards (packet_id, packet) to intermediate_queue
          - If invalid: logs and drops the packet
        
        On receiving sentinel, forwards one sentinel downstream so the
        Aggregator also knows this worker is done.
        """
        verified_count = 0
        dropped_count  = 0

        print(f"[Worker {self.worker_id}] Started.")

        while True:
            packet = self.raw_queue.get()

            # Sentinel received — stream is over for this worker
            if packet is self.SENTINEL:
                print(f"[Worker {self.worker_id}] Sentinel received. "
                      f"Verified={verified_count}, Dropped={dropped_count}")
                # Forward sentinel so Aggregator knows one worker finished
                self.intermediate_queue.put(self.SENTINEL)
                break

            packet_id = packet["packet_id"]

            if verify_signature(packet, self.secret_key, self.iterations):
                self.intermediate_queue.put((packet_id, packet))
                verified_count += 1
                print(f"[Worker {self.worker_id}] Verified packet {packet_id}")
            else:
                dropped_count += 1
                print(f"[Worker {self.worker_id}] Dropped packet {packet_id} (bad signature)")


# ─────────────────────────────────────────────────────────────
#  Aggregator — Stateful Re-sequencing + Sliding Window
#  Implements: Imperative Shell (state) + Functional Core (math)
# ─────────────────────────────────────────────────────────────

# How many seconds to wait for a missing packet_id before skipping it.
# If a packet was dropped by a worker (bad signature), we'd wait forever
# without this cutoff.
CUTOFF_SECONDS = 2.0


class Aggregator:
    """
    Single-process Aggregator — implements the Gather part of Scatter-Gather.

    Responsibilities:
      1. Collect verified packets from intermediate_queue (Queue 2)
      2. Re-sequence them in strict packet_id order using a min-heap
      3. Skip missing IDs after CUTOFF_SECONDS (dropped packets)
      4. Maintain a sliding window of the last N metric_values
      5. Call compute_average() (pure function) to get running average
      6. Attach computed_metric to each packet
      7. Push completed packets to processed_queue (Queue 3)

    Why a single process?
      The sliding window average is inherently stateful and sequential —
      average of packets 0..9 must be computed before 1..10. Parallelising
      this would require synchronisation that defeats the purpose.

    Imperative Shell:
      This class IS the imperative shell. It owns all mutable state:
        - self.heap          (the priority queue)
        - self.window        (the sliding window deque)
        - self.next_expected (which packet_id we want next)
        - self.last_seen_time (for cutoff timing)

    Functional Core:
      compute_average(window) — called here but defined above as a
      pure function with no access to self or any shared state.
    """

    def __init__(
        self,
        config: dict,
        intermediate_queue: multiprocessing.Queue,
        processed_queue: multiprocessing.Queue,
        num_workers: int,
    ):
        """
        Args:
            config             : full config.json dict
            intermediate_queue : Queue 2 — pulls (packet_id, packet) from here
            processed_queue    : Queue 3 — pushes completed packets here
            num_workers        : how many CoreWorkers exist (to count sentinels)
        """
        window_size = config["processing"]["stateful_tasks"]["running_average_window_size"]

        # ── Imperative Shell State ────────────────────────────────────────
        self.intermediate_queue = intermediate_queue
        self.processed_queue    = processed_queue
        self.num_workers        = num_workers

        self.heap           = []                              # min-heap: (packet_id, packet)
        self.window         = collections.deque(maxlen=window_size)  # sliding window
        self.next_expected  = 0                               # next packet_id to release
        self.last_seen_time = time.time()                     # for cutoff detection
        self.sentinels_seen = 0                               # count worker completions

    def _try_release(self) -> None:
        """
        Imperative Shell method — attempts to release packets from the
        heap in strict packet_id order.

        A packet is released when:
          heap[0].packet_id == self.next_expected

        After release, calls the Functional Core (compute_average) and
        attaches the result to the packet before forwarding downstream.
        """
        while self.heap and self.heap[0][0] == self.next_expected:
            _, packet = heapq.heappop(self.heap)

            # Add metric_value to sliding window (Imperative Shell manages state)
            self.window.append(packet["metric_value"])

            # Call Functional Core — pure function, no side effects
            avg = compute_average(list(self.window))

            # Attach computed result to packet
            packet["computed_metric"] = round(avg, 4)

            # Forward to output
            self.processed_queue.put(packet)

            print(f"[Aggregator] Released #{self.next_expected:>4} | "
                  f"metric={packet['metric_value']:.2f} | "
                  f"avg={packet['computed_metric']:.4f} | "
                  f"window_size={len(self.window)}")

            self.next_expected  += 1
            self.last_seen_time  = time.time()

    def _apply_cutoff(self) -> None:
        """
        Imperative Shell method — handles the timeout/cutoff logic.

        If CUTOFF_SECONDS have passed since we last released a packet,
        the next expected packet_id is assumed to have been dropped by
        a worker (failed signature verification). We skip it and move on.

        This prevents the pipeline from stalling indefinitely waiting
        for a packet that will never arrive.
        """
        if time.time() - self.last_seen_time > CUTOFF_SECONDS:
            print(f"[Aggregator] Timeout — skipping packet #{self.next_expected} (assumed dropped)")
            self.next_expected  += 1
            self.last_seen_time  = time.time()   # reset so we don't fire again immediately
            self._try_release()

    def run(self) -> None:
        """
        Main loop — runs inside a multiprocessing.Process.

        Continuously pulls from intermediate_queue.
        Items can be:
          - (packet_id, packet) tuple  → verified packet from a CoreWorker
          - None (sentinel)            → one CoreWorker has finished

        When all num_workers sentinels are received, drains any remaining
        packets from the heap (with cutoff for any final missing IDs),
        then sends a sentinel downstream to signal the Output module.
        """
        print(f"[Aggregator] Started. Expecting packets from {self.num_workers} workers.")

        while True:
            # Non-blocking poll with short timeout so we can check cutoff
            try:
                item = self.intermediate_queue.get(timeout=0.1)
            except queue.Empty:
                # Queue was empty — check cutoff, then loop
                if self.sentinels_seen < self.num_workers:
                    self._apply_cutoff()
                continue

            # ── Sentinel received from a worker ──────────────────────────
            if item is None:
                self.sentinels_seen += 1
                print(f"[Aggregator] Worker sentinel {self.sentinels_seen}/{self.num_workers} received.")

                if self.sentinels_seen == self.num_workers:
                    # All workers done — drain remaining heap
                    print("[Aggregator] All workers done. Draining remaining packets...")
                    self._drain_remaining()
                    break
                continue

            # ── Normal verified packet ────────────────────────────────────
            packet_id, packet = item
            # Ignore packets already skipped by cutoff timeout
            if packet_id < self.next_expected:
                print(f"[Aggregator] Ignoring stale packet #{packet_id} (already skipped by timeout)")
                continue
            heapq.heappush(self.heap, (packet_id, packet))
            self._try_release()

        # Signal Output module that the stream is over
        self.processed_queue.put(None)
        print("[Aggregator] Done. Sentinel sent to Output.")

    def _drain_remaining(self) -> None:
        """
        After all workers finish, some packets may still be sitting
        in the heap waiting for a missing predecessor ID.
        This drains them with cutoff logic so nothing is left behind.
        """
        # Give a short window for any last items still in transit
        time.sleep(0.5)

        # Drain remaining items using try/except — .empty() is unreliable in multiprocessing
        while True:
            try:
                item = self.intermediate_queue.get_nowait()
                if item is not None:
                    packet_id, packet = item
                    if packet_id >= self.next_expected:
                        heapq.heappush(self.heap, (packet_id, packet))
            except queue.Empty:
                break

        # Now release everything left in heap, skipping missing IDs
        while self.heap:
            next_in_heap = self.heap[0][0]

            # Skip any missing IDs between next_expected and next in heap
            while self.next_expected < next_in_heap:
                print(f"[Aggregator] Drain — skipping missing #{self.next_expected}")
                self.next_expected += 1

            # Discard stale packets that fell behind next_expected
            if self.heap and self.heap[0][0] < self.next_expected:
                heapq.heappop(self.heap)
                continue

            self._try_release()