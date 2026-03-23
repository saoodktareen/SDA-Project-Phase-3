import time
import multiprocessing

from config.read_config    import load_config
from core.imperative_shell import CoreWorker, Aggregator
from plugins.input_module  import InputModule
from plugins.output_module import OutputModule
from plugins.telemetry     import PipelineTelemetry

def run_input(config, raw_stream, error_dict, ready_event, start_event):

    module = InputModule(config, raw_stream, ready_event, start_event)
    module.run()

    error_dict["fatal_errors"] = module.fatal_errors
    error_dict["skipped_rows"] = module.skipped_rows

def run_worker(worker_id, config, raw_stream, intermediate_queue):

    worker = CoreWorker(
        worker_id,
        config,
        raw_stream,
        intermediate_queue,
    )
    worker.run()

def run_aggregator(config, intermediate_queue, processed_queue,
                   num_workers, start_event):

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

    queue_max = config["pipeline_dynamics"]["stream_queue_max_size"]

    output = OutputModule(config, processed_queue, error_dict,
                          ready_event, start_event)

    telemetry = PipelineTelemetry(
        raw_stream         = raw_stream,
        intermediate_queue = intermediate_queue,
        processed_queue    = processed_queue,
        queue_max_size     = queue_max,
        config             = config,
    )

    telemetry.subscribe(output)

    telemetry.start()

    output.run()

    telemetry.stop()

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
    print(f"[Main] Delay      : {config['pipeline_dynamics']['input_delay_seconds']}s per packet")
    print("=" * 60)

    raw_stream         = multiprocessing.Queue(maxsize=queue_max)
    intermediate_queue = multiprocessing.Queue(maxsize=queue_max)
    processed_queue    = multiprocessing.Queue(maxsize=queue_max)

    manager    = multiprocessing.Manager()
    error_dict = manager.dict()
    error_dict["fatal_errors"] = []
    error_dict["skipped_rows"] = []

    ready_event = multiprocessing.Event()

    start_event = multiprocessing.Event()

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

    output_proc.start()
    print(f"[Main] Started : {output_proc.name} (PID {output_proc.pid})")

    time.sleep(1.5)

    aggregator_proc.start()
    print(f"[Main] Started : {aggregator_proc.name} (PID {aggregator_proc.pid})")

    def _start_worker(w: multiprocessing.Process) -> None:
        w.start()
        print(f"[Main] Started : {w.name} (PID {w.pid})")

    list(map(_start_worker, worker_procs))

    input_proc.start()
    print(f"[Main] Started : {input_proc.name} (PID {input_proc.pid})")

    print("[Main] All processes running.")
    print("[Main] Close the dashboard window to exit.")

    input_proc.join()
    print(f"[Main] {input_proc.name} finished.")

    print(f"[Main] Pushing {num_workers} sentinel(s) into raw_stream...")
    list(map(lambda _: raw_stream.put(None), range(num_workers)))

    def _join_worker(w: multiprocessing.Process) -> None:
        w.join()
        print(f"[Main] {w.name} finished.")

    list(map(_join_worker, worker_procs))

    list(map(
        lambda w: (print(f"[Main] WARNING — force-terminating {w.name}..."),
                   w.terminate()),
        filter(lambda w: w.is_alive(), worker_procs)
    ))

    aggregator_proc.join()
    print(f"[Main] {aggregator_proc.name} finished.")

    output_proc.join()
    print(f"[Main] {output_proc.name} finished.")

    start_event.set()

    manager.shutdown()

    print("=" * 60)
    print("  Pipeline complete. All processes finished cleanly.")
    print("=" * 60)

if __name__ == "__main__":
    multiprocessing.freeze_support()
    bootstrap()