"""
imperative_shell.py — Generic Core: Verification Workers + Aggregator
======================================================================
This module is completely domain-agnostic. It never imports anything
related to sensors, GDP, or any specific dataset.

Two classes live here:

1. CoreWorker  (Stateless, Parallel)
   - Pulls packets from raw_queue (Queue 1)
   - Verifies the cryptographic signature using PBKDF2-HMAC
   - Drops packets that fail verification
   - Pushes verified (priority_index, packet) into intermediate_queue (Queue 2)
   - Implements the Scatter part of Scatter-Gather — N workers run in parallel

2. Aggregator  (Stateful, Single Process)
   - Pulls verified packets from intermediate_queue (Queue 2)
   - Re-sequences them using a min-heap (priority queue) on priority_index
   - Applies a timeout/cutoff for packets dropped by workers (bad signature)
   - Computes sliding window running average — Functional Core, Imperative Shell
   - Pushes fully processed packets into processed_queue (Queue 3)

Patterns implemented (per PDF):
  - Scatter-Gather         : N parallel CoreWorker processes
  - Functional Core        : pure verify_signature() and compute_average()
  - Imperative Shell       : Aggregator owns ALL mutable state
  - Priority Queue / heap  : heapq for strict re-sequencing
  - Timeout / Cutoff       : skips missing priority_index after CUTOFF_SECONDS

DIP compliance:
  - CoreWorker depends only on two Queues and config values (strings/ints).
    It has zero knowledge of InputModule, Aggregator, or OutputModule.
  - Aggregator depends only on two Queues and config values.
    It has zero knowledge of CoreWorker internals or OutputModule.
  - Both are instantiated and wired by main.py (the orchestrator).
"""

import heapq
import queue
import time
import collections
import multiprocessing
from core.functional_core import verify_signature, compute_average


# ─────────────────────────────────────────────────────────────────────────────
#  CoreWorker — Stateless Parallel Verification (Scatter)
# ─────────────────────────────────────────────────────────────────────────────

class CoreWorker:
    """
    Stateless parallel worker — implements the Scatter part of Scatter-Gather.

    Each CoreWorker runs as an independent multiprocessing.Process.
    Multiple workers pull from the same raw_queue concurrently so packets
    are verified in parallel across CPU cores.

    The worker is completely domain-agnostic:
      - It does not know what the data represents.
      - It does not know how many other workers exist.
      - It does not know what happens to packets after verification.

    Contract: pull packet → verify signature → forward or drop.
    """

    SENTINEL = None  # Poison pill sent by main.py — one per worker

    def __init__(
        self,
        worker_id: int,
        config: dict,
        raw_queue: multiprocessing.Queue,
        intermediate_queue: multiprocessing.Queue,
    ):
        """
        Parameters
        ----------
        worker_id          : integer label for logging (0, 1, 2, ...)
        config             : full validated config dict from main.py
        raw_queue          : Queue 1 — pulls raw packets from here
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

        Pulls packets from raw_queue until a sentinel (None) is received.

        For each packet:
          - Reads priority_index (set by InputModule, 1-based)
          - Calls verify_signature() — pure function from functional_core
          - Verified  → pushes (priority_index, packet) to intermediate_queue
          - Unverified → logs and drops the packet

        On sentinel: forwards one sentinel to intermediate_queue so the
        Aggregator can count how many workers have finished.
        """
        verified_count = 0
        dropped_count  = 0

        print(f"[Worker {self.worker_id}] Started.")

        while True:
            packet = self.raw_queue.get()  # blocks until item available

            # ── Sentinel: stream is over for this worker ──────────────────
            if packet is self.SENTINEL:
                print(f"[Worker {self.worker_id}] Sentinel received — "
                      f"verified={verified_count}, dropped={dropped_count}")
                # Forward one sentinel so Aggregator knows this worker finished
                self.intermediate_queue.put(self.SENTINEL)
                break

            # ── Normal packet ─────────────────────────────────────────────
            # Use priority_index (1-based, set by InputModule) as the
            # ordering key for the Aggregator's min-heap re-sequencing.
            priority_index = packet["priority_index"]

            if verify_signature(packet, self.secret_key, self.iterations):
                # Push as (priority_index, packet) tuple so Aggregator can heap-sort
                self.intermediate_queue.put((priority_index, packet))
                verified_count += 1
                print(f"[Worker {self.worker_id}] Verified   #{priority_index}")
            else:
                dropped_count += 1
                print(f"[Worker {self.worker_id}] Dropped    #{priority_index} "
                      f"(signature mismatch)")


# ─────────────────────────────────────────────────────────────────────────────
#  Aggregator — Stateful Re-sequencing + Sliding Window  (Gather)
#  Pattern: Functional Core, Imperative Shell
# ─────────────────────────────────────────────────────────────────────────────

# Seconds to wait for a missing priority_index before skipping it.
# Without this, a dropped packet (bad signature) would stall the pipeline forever.
CUTOFF_SECONDS = 2.0


class Aggregator:
    """
    Single-process Aggregator — implements the Gather part of Scatter-Gather.

    Responsibilities:
      1. Collect verified (priority_index, packet) tuples from intermediate_queue
      2. Re-sequence them in strict priority_index order using a min-heap
      3. Skip missing indices after CUTOFF_SECONDS (packets dropped by workers)
      4. Maintain a sliding window of the last N metric_values (Imperative Shell)
      5. Call compute_average(window) — pure Functional Core — to get running avg
      6. Attach computed_metric to each packet before forwarding
      7. Push completed packets to processed_queue (Queue 3)

    Why a single process?
      The sliding window average is inherently sequential — the average of
      packets 1..10 must be computed before 2..11. Parallelising this would
      break ordering and require synchronisation that defeats the purpose.

    Imperative Shell (this class owns ALL mutable state):
      self.heap           — min-heap for re-sequencing
      self.window         — sliding window deque
      self.next_expected  — which priority_index to release next (starts at 1)
      self.last_seen_time — timestamp of last successful release (for cutoff)
      self.sentinels_seen — count of worker-done signals received

    Functional Core (pure functions, no state):
      compute_average(window) — imported from functional_core.py
      verify_signature(...)   — used by CoreWorker, also from functional_core.py
    """

    def __init__(
        self,
        config: dict,
        intermediate_queue: multiprocessing.Queue,
        processed_queue: multiprocessing.Queue,
        num_workers: int,
    ):
        """
        Parameters
        ----------
        config             : full validated config dict from main.py
        intermediate_queue : Queue 2 — pulls (priority_index, packet) from here
        processed_queue    : Queue 3 — pushes completed packets here
        num_workers        : number of CoreWorkers (to know when all are done)
        """
        window_size = config["processing"]["stateful_tasks"]["running_average_window_size"]

        # ── Queues (passed in — Aggregator does not create them) ──────────
        self.intermediate_queue = intermediate_queue
        self.processed_queue    = processed_queue
        self.num_workers        = num_workers

        # ── Imperative Shell State ────────────────────────────────────────
        self.heap           = []                                     # min-heap (priority_index, packet)
        self.window         = collections.deque(maxlen=window_size)  # sliding window
        self.next_expected  = 1          # MUST start at 1 — InputModule priority_index is 1-based
        self.last_seen_time = time.time()
        self.sentinels_seen = 0

    # ── Private helpers (Imperative Shell methods) ────────────────────────

    def _try_release(self) -> None:
        """
        Release packets from the heap in strict priority_index order.

        Pops from the heap only when heap[0][0] == self.next_expected.
        For each released packet:
          1. Appends metric_value to the sliding window  (Imperative Shell — state)
          2. Calls compute_average(window)               (Functional Core  — pure)
          3. Attaches computed_metric to the packet
          4. Forwards to processed_queue
        """
        while self.heap and self.heap[0][0] == self.next_expected:
            _, packet = heapq.heappop(self.heap)

            # Imperative Shell: update mutable sliding window state
            self.window.append(packet["metric_value"])

            # Functional Core: pure function — no side effects, no self
            avg = compute_average(list(self.window))

            # Attach result and forward downstream
            packet["computed_metric"] = round(avg, 4)
            self.processed_queue.put(packet)

            print(f"[Aggregator] Released #{self.next_expected:>4} | "
                  f"metric={packet['metric_value']:.2f} | "
                  f"avg={packet['computed_metric']:.4f} | "
                  f"window={len(self.window)}")

            self.next_expected  += 1
            self.last_seen_time  = time.time()

    def _apply_cutoff(self) -> None:
        """
        If CUTOFF_SECONDS have elapsed since the last successful release,
        assume the next expected packet was dropped (bad signature) and skip it.

        Increments next_expected by 1 and immediately calls _try_release()
        in case packets further ahead are already sitting in the heap.
        """
        if time.time() - self.last_seen_time > CUTOFF_SECONDS:
            print(f"[Aggregator] Timeout — skipping #{self.next_expected} "
                  f"(assumed dropped by worker)")
            self.next_expected  += 1
            self.last_seen_time  = time.time()  # reset to avoid rapid-fire skips
            self._try_release()

    def _drain_remaining(self) -> None:
        """
        Called after all CoreWorkers have finished.

        Drains any last packets still in transit in intermediate_queue,
        pushes them onto the heap, then releases everything with cutoff
        logic for any gaps left by dropped packets.
        """
        # Brief pause — last packets may still be in transit through the queue
        time.sleep(0.5)

        # Drain all remaining items (.empty() is unreliable in multiprocessing)
        while True:
            try:
                item = self.intermediate_queue.get_nowait()
                if item is not None:
                    priority_index, packet = item
                    if priority_index >= self.next_expected:
                        heapq.heappush(self.heap, (priority_index, packet))
            except queue.Empty:
                break

        # Release all remaining heap items, skipping any missing indices
        while self.heap:
            next_in_heap = self.heap[0][0]

            # Skip over any gaps (dropped packets between next_expected and heap top)
            while self.next_expected < next_in_heap:
                print(f"[Aggregator] Drain — skipping missing #{self.next_expected}")
                self.next_expected += 1

            # Discard any stale packets that somehow ended up below next_expected
            if self.heap and self.heap[0][0] < self.next_expected:
                heapq.heappop(self.heap)
                continue

            self._try_release()

    # ── Public entry point ────────────────────────────────────────────────

    def run(self) -> None:
        """
        Main loop — runs inside a multiprocessing.Process.

        Continuously pulls from intermediate_queue. Items are either:
          - (priority_index, packet) tuple  → verified packet from a CoreWorker
          - None (sentinel)                 → one CoreWorker has finished

        Uses get(timeout=0.1) instead of blocking get() so the cutoff
        timer can be checked even when the queue is temporarily empty.

        When all num_workers sentinels are received:
          1. Drains any remaining in-flight packets
          2. Sends a None sentinel to processed_queue to signal Output
        """
        print(f"[Aggregator] Started — waiting for {self.num_workers} workers.")

        while True:
            # Non-blocking poll with short timeout to allow cutoff checks
            try:
                item = self.intermediate_queue.get(timeout=0.1)
            except queue.Empty:
                # No item available — check if a packet is overdue
                if self.sentinels_seen < self.num_workers:
                    self._apply_cutoff()
                continue

            # ── Sentinel: one CoreWorker has finished ─────────────────────
            if item is None:
                self.sentinels_seen += 1
                print(f"[Aggregator] Worker done "
                      f"({self.sentinels_seen}/{self.num_workers})")

                if self.sentinels_seen == self.num_workers:
                    print("[Aggregator] All workers done — draining heap ...")
                    self._drain_remaining()
                    break
                continue

            # ── Normal verified packet ────────────────────────────────────
            priority_index, packet = item

            # Ignore packets already skipped by the cutoff timer
            if priority_index < self.next_expected:
                print(f"[Aggregator] Ignoring stale #{priority_index} "
                      f"(already skipped by timeout)")
                continue

            heapq.heappush(self.heap, (priority_index, packet))
            self._try_release()

        # Signal Output module that all processed data has been sent
        self.processed_queue.put(None)
        print("[Aggregator] Done — sentinel sent to Output.")