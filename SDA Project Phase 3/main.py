"""
main.py — Central Orchestrator
================================
Wires all pipeline components together using multiprocessing.
This file contains ZERO business logic — it only:
  1. Loads config.json
  2. Creates bounded multiprocessing Queues
  3. Instantiates all module objects
  4. Wraps them in multiprocessing.Process workers
  5. Starts the telemetry monitor (daemon thread)
  6. Starts all processes concurrently
  7. Waits for clean shutdown

Process map:
  ┌─────────────────────────────────────────────────────────┐
  │  main.py (Main Process)                                  │
  │    └── TelemetryThread (daemon thread)                   │
  │                                                          │
  │  input_proc    → [Queue 1] → worker_0                    │
  │                              worker_1  → [Queue 2]       │
  │                              worker_2        ↓           │
  │                              worker_3   aggregator_proc  │
  │                                              ↓           │
  │                                         [Queue 3]        │
  │                                              ↓           │
  │                                         output_proc      │
  └─────────────────────────────────────────────────────────┘

Sentinel strategy:
  - main.py sends N sentinels (None) into Queue 1 after Input finishes,
    one per CoreWorker, so every worker gets exactly one stop signal.
  - Each CoreWorker forwards one sentinel to Queue 2 when it stops.
  - Aggregator counts N sentinels from Queue 2, then sends one to Queue 3.
  - OutputModule sees sentinel in Queue 3 and marks stream complete.
"""

import sys
import json
import time
import multiprocessing

from input_module  import InputModule
from core_module   import CoreWorker, Aggregator
from output_module import OutputModule
from telemetry     import PipelineTelemetry


# ─────────────────────────────────────────────────────────────
#  Config loader
# ─────────────────────────────────────────────────────────────

def load_config(path: str = "config.json") -> dict:
    """Load and return the pipeline configuration from config.json."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[Main] ERROR — config file not found: '{path}'")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[Main] ERROR — invalid JSON in config: {e}")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────
#  Process target functions
#  (top-level functions required by multiprocessing on Windows)
# ─────────────────────────────────────────────────────────────

def run_input(config, raw_queue):
    """Target for the Input producer process."""
    module = InputModule(config, raw_queue)
    module.run()
    # After all rows sent, push one sentinel per CoreWorker
    num_workers = config["pipeline_dynamics"]["core_parallelism"]
    print(f"[Input] Sending {num_workers} sentinels to stop workers...")
    for _ in range(num_workers):
        raw_queue.put(None)


def run_worker(worker_id, config, raw_queue, intermediate_queue):
    """Target for each CoreWorker process."""
    worker = CoreWorker(worker_id, config, raw_queue, intermediate_queue)
    worker.run()


def run_aggregator(config, intermediate_queue, processed_queue, num_workers):
    """Target for the Aggregator process."""
    aggregator = Aggregator(config, intermediate_queue, processed_queue, num_workers)
    aggregator.run()


def run_output(config, processed_queue, raw_queue, intermediate_queue):
    """
    Target for the Output (dashboard) process.
    Also sets up and starts the telemetry monitor inside this process
    so it can access the queues directly.
    """
    queue_max = config["pipeline_dynamics"]["stream_queue_max_size"]

    # Create output module (also a TelemetryObserver)
    output = OutputModule(config, processed_queue)

    # Create telemetry subject and subscribe the dashboard
    telemetry = PipelineTelemetry(
        raw_queue          = raw_queue,
        intermediate_queue = intermediate_queue,
        processed_queue    = processed_queue,
        queue_max_size     = queue_max,
        config             = config,
    )
    telemetry.subscribe(output)
    telemetry.start()

    # Run the dashboard (blocks until window is closed)
    output.run()

    telemetry.stop()


# ─────────────────────────────────────────────────────────────
#  Bootstrap — main entry point
# ─────────────────────────────────────────────────────────────

def bootstrap():
    """
    Wire and launch the full pipeline.

    Step-by-step:
      1. Load config
      2. Create three bounded queues
      3. Create all process objects
      4. Start output first (so dashboard is ready before data flows)
      5. Start aggregator
      6. Start all core workers
      7. Start input last (it drives the whole pipeline)
      8. Join all processes in reverse order (input → workers → aggregator → output)
    """
    print("=" * 60)
    print("  Phase 3 — Generic Concurrent Real-Time Pipeline")
    print("=" * 60)

    config      = load_config("config.json")
    num_workers = config["pipeline_dynamics"]["core_parallelism"]
    queue_max   = config["pipeline_dynamics"]["stream_queue_max_size"]

    print(f"[Main] Workers      : {num_workers}")
    print(f"[Main] Queue max    : {queue_max}")
    print(f"[Main] Dataset      : {config['dataset_path']}")
    print(f"[Main] Input delay  : {config['pipeline_dynamics']['input_delay_seconds']}s")
    print("=" * 60)

    # ── 1. Create bounded queues ──────────────────────────────
    raw_queue          = multiprocessing.Queue(maxsize=queue_max)
    intermediate_queue = multiprocessing.Queue(maxsize=queue_max)
    processed_queue    = multiprocessing.Queue(maxsize=queue_max)

    # ── 2. Create process objects ─────────────────────────────
    output_proc = multiprocessing.Process(
        target = run_output,
        args   = (config, processed_queue, raw_queue, intermediate_queue),
        name   = "OutputProcess",
        daemon = False,
    )

    aggregator_proc = multiprocessing.Process(
        target = run_aggregator,
        args   = (config, intermediate_queue, processed_queue, num_workers),
        name   = "AggregatorProcess",
        daemon = False,
    )

    worker_procs = [
        multiprocessing.Process(
            target = run_worker,
            args   = (i, config, raw_queue, intermediate_queue),
            name   = f"CoreWorker-{i}",
            daemon = False,
        )
        for i in range(num_workers)
    ]

    input_proc = multiprocessing.Process(
        target = run_input,
        args   = (config, raw_queue),
        name   = "InputProcess",
        daemon = False,
    )

    # ── 3. Start processes ────────────────────────────────────
    # Output first — dashboard needs to be ready
    output_proc.start()
    print(f"[Main] Started : {output_proc.name}")

    # Small delay so the dashboard window opens before data flows
    time.sleep(1.5)

    # Aggregator before workers (so it's ready to receive)
    aggregator_proc.start()
    print(f"[Main] Started : {aggregator_proc.name}")

    # Workers before input (so they're ready to consume)
    for w in worker_procs:
        w.start()
        print(f"[Main] Started : {w.name}")

    # Input last — it drives everything
    input_proc.start()
    print(f"[Main] Started : {input_proc.name}")

    print("[Main] All processes running. Pipeline is live.")
    print("[Main] Close the dashboard window to stop.")

    # ── 4. Join all processes ─────────────────────────────────
    # Join input first — wait for all data to be sent
    input_proc.join()
    print(f"[Main] {input_proc.name} finished.")

    # Join workers — wait for all verification to complete
    for w in worker_procs:
        w.join()
        print(f"[Main] {w.name} finished.")

    # Join aggregator — wait for all averages to be computed
    aggregator_proc.join()
    print(f"[Main] {aggregator_proc.name} finished.")

    # Join output last — wait for dashboard to be closed
    output_proc.join()
    print(f"[Main] {output_proc.name} finished.")

    print("=" * 60)
    print("  Pipeline complete. All processes finished cleanly.")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────
#  Entry point guard — required for multiprocessing on Windows
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Required on Windows/macOS — prevents recursive subprocess spawning
    multiprocessing.freeze_support()
    bootstrap()