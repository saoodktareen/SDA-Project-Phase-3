"""
main.py — Central Orchestrator
================================
Wires the pipeline together: config -> input -> core workers -> aggregator.
No output module or telemetry — results are printed to terminal only.

Process map:
  input_proc  -> [raw_stream Q1] -> worker_0
                                    worker_1  -> [Q2]
                                    worker_2       |
                                    worker_3  aggregator_proc
                                                   |
                                              [Q3] printed to terminal
"""

import time
import multiprocessing

from config.read_config    import load_config
from core.imperative_shell import CoreWorker, Aggregator
from plugins.input_module  import InputModule


# ─────────────────────────────────────────────────────────────
#  Process target functions
# ─────────────────────────────────────────────────────────────

def run_input(config, raw_stream):
    """Target for the Input producer process."""
    module = InputModule(config, raw_stream)
    module.run()
    num_workers = config["pipeline_dynamics"]["core_parallelism"]
    print(f"[Input] Sending {num_workers} sentinels to stop workers...")
    for _ in range(num_workers):
        raw_stream.put(None)


def run_worker(worker_id, config, raw_stream, intermediate_queue, packet_counter, counter_lock):
    """Target for each CoreWorker process."""
    worker = CoreWorker(
        worker_id,
        config,
        raw_stream,
        intermediate_queue,
        packet_counter,
        counter_lock,
    )
    worker.run()


def run_aggregator(config, intermediate_queue, processed_queue, num_workers):
    """Target for the Aggregator process — prints results to terminal."""
    aggregator = Aggregator(config, intermediate_queue, processed_queue, num_workers)
    aggregator.run()

    # Drain and print everything from processed_queue
    print("\n" + "=" * 60)
    print("  PIPELINE RESULTS")
    print("=" * 60)
    while True:
        try:
            packet = processed_queue.get(timeout=1.0)
            if packet is None:
                break
            print(
                f"  entity={packet.get('entity_name', '?'):15s} | "
                f"time={packet.get('time_period', '?')} | "
                f"value={packet.get('metric_value', 0):.2f} | "
                f"avg={packet.get('computed_metric', 0):.4f}"
            )
        except Exception:
            break
    print("=" * 60)


# ─────────────────────────────────────────────────────────────
#  Bootstrap
# ─────────────────────────────────────────────────────────────

def bootstrap():
    print("=" * 60)
    print("  Phase 3 — Generic Concurrent Real-Time Pipeline")
    print("=" * 60)

    config      = load_config("config.json")
    num_workers = config["pipeline_dynamics"]["core_parallelism"]
    queue_max   = config["pipeline_dynamics"]["stream_queue_max_size"]

    print(f"[Main] Workers    : {num_workers}")
    print(f"[Main] Queue max  : {queue_max}")
    print(f"[Main] Dataset    : {config['dataset_path']}")
    print("=" * 60)

    # Queues
    raw_stream         = multiprocessing.Queue(maxsize=queue_max)
    intermediate_queue = multiprocessing.Queue(maxsize=queue_max)
    processed_queue    = multiprocessing.Queue(maxsize=queue_max)

    # Shared packet_id counter + lock
    packet_counter = multiprocessing.Value('i', 0)
    counter_lock   = multiprocessing.Lock()

    # Processes
    aggregator_proc = multiprocessing.Process(
        target = run_aggregator,
        args   = (config, intermediate_queue, processed_queue, num_workers),
        name   = "AggregatorProcess",
        daemon = False,
    )

    worker_procs = [
        multiprocessing.Process(
            target = run_worker,
            args   = (i, config, raw_stream, intermediate_queue, packet_counter, counter_lock),
            name   = f"CoreWorker-{i}",
            daemon = False,
        )
        for i in range(num_workers)
    ]

    input_proc = multiprocessing.Process(
        target = run_input,
        args   = (config, raw_stream),
        name   = "InputProcess",
        daemon = False,
    )

    # Start — aggregator first, then workers, then input
    aggregator_proc.start()
    print(f"[Main] Started : {aggregator_proc.name}")

    for w in worker_procs:
        w.start()
        print(f"[Main] Started : {w.name}")

    input_proc.start()
    print(f"[Main] Started : {input_proc.name}")

    print("[Main] Pipeline is live — results printing below...")

    # Join
    input_proc.join()
    print(f"[Main] {input_proc.name} finished.")

    for w in worker_procs:
        w.join()
        print(f"[Main] {w.name} finished.")

    for w in worker_procs:
        if w.is_alive():
            print(f"[Main] Force-terminating {w.name}...")
            w.terminate()

    aggregator_proc.join()
    print(f"[Main] {aggregator_proc.name} finished.")

    print("=" * 60)
    print("  Pipeline complete.")
    print("=" * 60)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    bootstrap()