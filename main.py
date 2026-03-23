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
  - main.py joins InputProcess, THEN pushes N sentinels into raw_stream.
    One sentinel per CoreWorker so every worker receives exactly one stop
    signal. This is done by main.py — NOT inside run_input() — because
    only main.py knows core_parallelism AND needs to pass error data back
    from InputModule via the shared Manager dict.
  - Each CoreWorker receives one sentinel → forwards one to intermediate_queue.
  - Aggregator counts N sentinels → drains heap → sends one to processed_queue.
  - OutputModule receives that sentinel → marks stream complete → closes.

Startup order (important — consumers before producers):
  Output first  → dashboard window opens before data flows
  Aggregator    → ready to receive before workers send
  Workers       → ready to consume before input sends
  Input last    → drives the whole pipeline

DIP compliance:
  - main.py is the ONLY file that imports all modules and wires them.
  - No module imports any other module — they only receive queues + config.
  - Inter-module communication happens exclusively through the three queues
    and the shared Manager dict.

All iteration uses functional programming — map(), filter() —
instead of imperative for loops.
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
#  Must be module-level (not methods/lambdas) for multiprocessing
#  on Windows which uses the 'spawn' start method and pickles targets.
# ─────────────────────────────────────────────────────────────

def run_input(config, raw_stream, error_dict, ready_event, start_event):
    """
    Target for the Input producer process.

    Runs InputModule.run() to load, validate, and stream all packets.
    After run() completes, writes fatal_errors and skipped_rows into
    the shared Manager dict so OutputModule can display them.

    Sets ready_event AFTER writing errors so Screen 1 on the dashboard
    waits for the real error state before drawing — fixing the timing
    bug where Screen 1 always showed green because it drew before Input
    had a chance to detect missing columns.

    Sentinels are NOT pushed here. main.py pushes them after
    input_proc.join() because only main.py knows core_parallelism.
    """
    module = InputModule(config, raw_stream, ready_event, start_event)
    module.run()

    # Write errors after run() — ready_event was already set inside run()
    # right before streaming started, so error_dict is populated before set()
    error_dict["fatal_errors"] = module.fatal_errors
    error_dict["skipped_rows"] = module.skipped_rows


def run_worker(worker_id, config, raw_stream, intermediate_queue):
    """Target for each CoreWorker process."""
    worker = CoreWorker(
        worker_id,
        config,
        raw_stream,
        intermediate_queue,
    )
    worker.run()


def run_aggregator(config, intermediate_queue, processed_queue,
                   num_workers, start_event):
    """Target for the single Aggregator process."""
    aggregator = Aggregator(
        config,
        intermediate_queue,
        processed_queue,
        num_workers,
        start_event,
    )
    aggregator.run()


def run_output(config, processed_queue, raw_stream, intermediate_queue,
               error_dict, ready_event, start_event):
    """
    Target for the Output (dashboard) process.

    Creates PipelineTelemetry (Subject) inside this same process so it
    can call qsize() directly on the queue objects.

    Passes ready_event to OutputModule so Screen 1 waits for Input to
    finish validation before drawing the error/status screen.
    """
    queue_max = config["pipeline_dynamics"]["stream_queue_max_size"]

    # OutputModule — Observer, reads Q3 and renders dashboard
    output = OutputModule(config, processed_queue, error_dict,
                          ready_event, start_event)

    # PipelineTelemetry — Subject, polls queue sizes on a background thread
    telemetry = PipelineTelemetry(
        raw_stream         = raw_stream,
        intermediate_queue = intermediate_queue,
        processed_queue    = processed_queue,
        queue_max_size     = queue_max,
        config             = config,
    )

    # Observer Pattern: dashboard subscribes to telemetry subject
    telemetry.subscribe(output)

    # Start polling — daemon thread, dies when process exits
    telemetry.start()

    # Run dashboard — blocks until window is closed by user
    output.run()

    telemetry.stop()


# ─────────────────────────────────────────────────────────────
#  Bootstrap — main entry point
# ─────────────────────────────────────────────────────────────

def bootstrap():
    """
    Loads config, creates queues and shared Manager objects,
    wires all processes, starts them in the correct order,
    and joins them cleanly on completion.
    """
    print("=" * 60)
    print("  Phase 3 — Generic Concurrent Real-Time Pipeline")
    print("=" * 60)

    # ── Step 1: Load and validate configuration ───────────────
    config      = load_config("config.json")
    num_workers = config["pipeline_dynamics"]["core_parallelism"]
    queue_max   = config["pipeline_dynamics"]["stream_queue_max_size"]

    print(f"[Main] Workers    : {num_workers}")
    print(f"[Main] Queue max  : {queue_max}")
    print(f"[Main] Dataset    : {config['dataset_path']}")
    print(f"[Main] Delay      : {config['pipeline_dynamics']['input_delay_seconds']}s per packet")
    print("=" * 60)

    # ── Step 2: Create bounded queues ────────────────────────
    raw_stream         = multiprocessing.Queue(maxsize=queue_max)  # Q1
    intermediate_queue = multiprocessing.Queue(maxsize=queue_max)  # Q2
    processed_queue    = multiprocessing.Queue(maxsize=queue_max)  # Q3

    # ── Step 3: Create shared Manager dict and events ─────────
    manager    = multiprocessing.Manager()
    error_dict = manager.dict()
    error_dict["fatal_errors"] = []
    error_dict["skipped_rows"] = []

    # ready_event: set by InputModule after validation, before streaming.
    # Screen 1 waits on this before drawing the real error state.
    ready_event = multiprocessing.Event()

    # start_event: set by button click, unblocks Input + Aggregator streaming.
    start_event = multiprocessing.Event()

    # ── Step 4: Create all process objects ───────────────────

    output_proc = multiprocessing.Process(
        target = run_output,
        args   = (config, processed_queue, raw_stream, intermediate_queue,
                  error_dict, ready_event, start_event),
        name   = "OutputProcess",
        daemon = False,
    )

    aggregator_proc = multiprocessing.Process(
        target = run_aggregator,
        args   = (config, intermediate_queue, processed_queue,
                  num_workers, start_event),
        name   = "AggregatorProcess",
        daemon = False,
    )

    # map() builds all worker Process objects — replaces list comprehension:
    #   for i in range(num_workers): ...
    worker_procs = list(map(
        lambda i: multiprocessing.Process(
            target = run_worker,
            args   = (i, config, raw_stream, intermediate_queue),
            name   = f"CoreWorker-{i}",
            daemon = False,
        ),
        range(num_workers)
    ))

    input_proc = multiprocessing.Process(
        target = run_input,
        args   = (config, raw_stream, error_dict, ready_event, start_event),
        name   = "InputProcess",
        daemon = False,
    )

    # ── Step 5: Start in correct order ───────────────────────
    # Rule: Output → Aggregator → Workers → Input

    output_proc.start()
    print(f"[Main] Started : {output_proc.name} (PID {output_proc.pid})")

    time.sleep(1.5)   # let dashboard window open before data arrives

    aggregator_proc.start()
    print(f"[Main] Started : {aggregator_proc.name} (PID {aggregator_proc.pid})")

    # map() starts every worker and prints its PID —
    # replaces: for w in worker_procs: w.start(); print(...)
    def _start_worker(w: multiprocessing.Process) -> None:
        w.start()
        print(f"[Main] Started : {w.name} (PID {w.pid})")

    list(map(_start_worker, worker_procs))

    input_proc.start()
    print(f"[Main] Started : {input_proc.name} (PID {input_proc.pid})")

    print("[Main] All processes running.")
    print("[Main] Close the dashboard window to exit.")

    # ── Step 6: Join Input then push sentinels ────────────────

    input_proc.join()
    print(f"[Main] {input_proc.name} finished.")

    # map() pushes one None per worker into raw_stream —
    # replaces: for _ in range(num_workers): raw_stream.put(None)
    print(f"[Main] Pushing {num_workers} sentinel(s) into raw_stream...")
    list(map(lambda _: raw_stream.put(None), range(num_workers)))

    # ── Step 7: Join remaining processes ─────────────────────

    # map() joins every worker and prints completion —
    # replaces: for w in worker_procs: w.join(); print(...)
    def _join_worker(w: multiprocessing.Process) -> None:
        w.join()
        print(f"[Main] {w.name} finished.")

    list(map(_join_worker, worker_procs))

    # Safety net — filter() finds any still-alive worker,
    # map() terminates them.
    # Replaces: for w in worker_procs: if w.is_alive(): w.terminate()
    list(map(
        lambda w: (print(f"[Main] WARNING — force-terminating {w.name}..."),
                   w.terminate()),
        filter(lambda w: w.is_alive(), worker_procs)
    ))

    aggregator_proc.join()
    print(f"[Main] {aggregator_proc.name} finished.")

    output_proc.join()
    print(f"[Main] {output_proc.name} finished.")

    # Safety net: if the user closed the window WITHOUT clicking the button,
    # start_event was never set. Setting it here unblocks any process still
    # waiting on start_event.wait() so they exit cleanly.
    start_event.set()

    # ── Step 8: Clean up Manager ──────────────────────────────
    manager.shutdown()

    print("=" * 60)
    print("  Pipeline complete. All processes finished cleanly.")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────
#  Entry point guard — required for multiprocessing on Windows
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    multiprocessing.freeze_support()
    bootstrap()