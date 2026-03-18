"""
main.py — Central Orchestrator
================================
Wires the full pipeline together:
  config -> input -> core workers -> aggregator -> output dashboard

Process map:
  ┌─────────────────────────────────────────────────────────┐
  │  main.py (Main Process)                                  │
  │                                                          │
  │  input_proc   -> [raw_stream Q1] -> worker_0             │
  │                                     worker_1  -> [Q2]    │
  │                                     worker_2       |     │
  │                                     worker_3  aggregator │
  │                                                    |     │
  │                                               [Q3]       │
  │                                                    |     │
  │                                            output_proc   │
  │                                         (+ telemetry     │
  │                                            thread)       │
  └─────────────────────────────────────────────────────────┘

Sentinel strategy:
  - main.py sends N sentinels (None) into raw_stream after Input finishes,
    one per CoreWorker, so every worker receives exactly one stop signal.
  - Each CoreWorker forwards one sentinel to intermediate_queue when done.
  - Aggregator counts N sentinels, then sends one sentinel to processed_queue.
  - OutputModule sees the sentinel and marks the stream as complete.

Startup order (important):
  Output first  -> dashboard window opens before data flows
  Aggregator    -> ready to receive before workers send
  Workers       -> ready to consume before input sends
  Input last    -> drives the whole pipeline
"""

import time
import multiprocessing

from config.read_config    import load_config
from core.imperative_shell import CoreWorker, Aggregator
from plugins.input_module  import InputModule
from plugins.output_module import OutputModule
from plugins.telemetry     import PipelineTelemetry


# ─────────────────────────────────────────────────────────────
#  Process target functions
#  Must be top-level (not methods) for multiprocessing on Windows
# ─────────────────────────────────────────────────────────────

def run_input(config, raw_stream):
    """
    Target for the Input producer process.
    Runs InputModule then pushes one sentinel per CoreWorker into
    raw_stream so every worker knows when to stop.
    """
    module = InputModule(config, raw_stream)
    module.run()

    num_workers = config["pipeline_dynamics"]["core_parallelism"]
    print(f"[Input] Sending {num_workers} sentinels to stop workers...")
    for _ in range(num_workers):
        raw_stream.put(None)


def run_worker(worker_id, config, raw_stream, intermediate_queue):
    """Target for each CoreWorker process."""
    worker = CoreWorker(
        worker_id,
        config,
        raw_stream,
        intermediate_queue,
    )
    worker.run()


def run_aggregator(config, intermediate_queue, processed_queue, num_workers):
    """Target for the single Aggregator process."""
    aggregator = Aggregator(
        config,
        intermediate_queue,
        processed_queue,
        num_workers,
    )
    aggregator.run()


def run_output(config, processed_queue, raw_stream, intermediate_queue):
    """
    Target for the Output (dashboard) process.

    Also creates and starts the PipelineTelemetry subject inside this
    same process so it can call qsize() directly on the queue objects.
    The OutputModule subscribes itself as the Observer before the
    telemetry thread starts polling.
    """
    queue_max = config["pipeline_dynamics"]["stream_queue_max_size"]

    # Create the Output module — it is also a TelemetryObserver
    output = OutputModule(config, processed_queue)

    # Create the Telemetry subject and subscribe the dashboard to it
    telemetry = PipelineTelemetry(
        raw_stream         = raw_stream,
        intermediate_queue = intermediate_queue,
        processed_queue    = processed_queue,
        queue_max_size     = queue_max,
        config             = config,
    )
    telemetry.subscribe(output)   # Observer pattern — dashboard receives updates
    telemetry.start()             # daemon thread starts polling queue sizes

    # Run the dashboard — blocks until the window is closed
    output.run()

    telemetry.stop()


# ─────────────────────────────────────────────────────────────
#  Bootstrap — main entry point
# ─────────────────────────────────────────────────────────────

def bootstrap():
    """
    Loads config, creates queues, wires all processes, starts them
    in the correct order, and joins them cleanly on completion.
    """
    print("=" * 60)
    print("  Phase 3 — Generic Concurrent Real-Time Pipeline")
    print("=" * 60)

    config      = load_config("config.json")
    num_workers = config["pipeline_dynamics"]["core_parallelism"]
    queue_max   = config["pipeline_dynamics"]["stream_queue_max_size"]

    print(f"[Main] Workers    : {num_workers}")
    print(f"[Main] Queue max  : {queue_max}")
    print(f"[Main] Dataset    : {config['dataset_path']}")
    print(f"[Main] Delay      : {config['pipeline_dynamics']['input_delay_seconds']}s per packet")
    print("=" * 60)

    # ── Create bounded queues ─────────────────────────────────
    raw_stream         = multiprocessing.Queue(maxsize=queue_max)
    intermediate_queue = multiprocessing.Queue(maxsize=queue_max)
    processed_queue    = multiprocessing.Queue(maxsize=queue_max)

    # ── Create all process objects ────────────────────────────

    # Output process — starts first so dashboard is ready before data flows
    output_proc = multiprocessing.Process(
        target = run_output,
        args   = (config, processed_queue, raw_stream, intermediate_queue),
        name   = "OutputProcess",
        daemon = False,
    )

    # Aggregator — single process, gathers from all workers
    aggregator_proc = multiprocessing.Process(
        target = run_aggregator,
        args   = (config, intermediate_queue, processed_queue, num_workers),
        name   = "AggregatorProcess",
        daemon = False,
    )

    # Core workers — N parallel verification processes
    worker_procs = [
        multiprocessing.Process(
            target = run_worker,
            args   = (i, config, raw_stream, intermediate_queue),
            name   = f"CoreWorker-{i}",
            daemon = False,
        )
        for i in range(num_workers)
    ]

    # Input process — started last, it drives everything
    input_proc = multiprocessing.Process(
        target = run_input,
        args   = (config, raw_stream),
        name   = "InputProcess",
        daemon = False,
    )

    # ── Start processes in correct order ──────────────────────

    # 1. Output first — dashboard window opens before data flows
    output_proc.start()
    print(f"[Main] Started : {output_proc.name}")

    # Brief pause so the dashboard has time to open before data arrives
    time.sleep(1.5)

    # 2. Aggregator — ready to receive before workers start sending
    aggregator_proc.start()
    print(f"[Main] Started : {aggregator_proc.name}")

    # 3. Workers — ready to consume before input starts producing
    for w in worker_procs:
        w.start()
        print(f"[Main] Started : {w.name}")

    # 4. Input last — starts the data flowing through the pipeline
    input_proc.start()
    print(f"[Main] Started : {input_proc.name}")

    print("[Main] All processes running.")
    print("[Main] Close the dashboard window to exit.")

    # ── Join processes in completion order ────────────────────

    # Input finishes first — all rows have been pushed to raw_stream
    input_proc.join()
    print(f"[Main] {input_proc.name} finished.")

    # Workers finish after all packets are verified or dropped
    for w in worker_procs:
        w.join()
        print(f"[Main] {w.name} finished.")

    # Safety net — force-terminate any worker that hung
    for w in worker_procs:
        if w.is_alive():
            print(f"[Main] Force-terminating {w.name}...")
            w.terminate()

    # Aggregator finishes after all averages are computed
    aggregator_proc.join()
    print(f"[Main] {aggregator_proc.name} finished.")

    # Output finishes when the dashboard window is closed by the user
    output_proc.join()
    print(f"[Main] {output_proc.name} finished.")

    print("=" * 60)
    print("  Pipeline complete. All processes finished cleanly.")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────
#  Entry point guard — required for multiprocessing on Windows
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # freeze_support() is required on Windows to prevent recursive
    # subprocess spawning when the script is packaged as an executable
    multiprocessing.freeze_support()
    bootstrap()