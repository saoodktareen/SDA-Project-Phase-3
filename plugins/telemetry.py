import time
import threading
from abc import ABC, abstractmethod

THRESHOLD_GREEN  = 0.40
THRESHOLD_YELLOW = 0.75

def get_queue_color(fill_ratio: float) -> str:
    if fill_ratio < THRESHOLD_GREEN:
        return "green"
    elif fill_ratio < THRESHOLD_YELLOW:
        return "yellow"
    else:
        return "red"

def build_telemetry_state(
    q1_size: int, q1_max: int,
    q2_size: int, q2_max: int,
    q3_size: int, q3_max: int,
) -> dict:
    q1_fill = min(q1_size / q1_max, 1.0) if q1_max > 0 else 0.0
    q2_fill = min(q2_size / q2_max, 1.0) if q2_max > 0 else 0.0
    q3_fill = min(q3_size / q3_max, 1.0) if q3_max > 0 else 0.0

    return {
        "q1": {
            "size":       q1_size,
            "max":        q1_max,
            "fill_ratio": q1_fill,
            "color":      get_queue_color(q1_fill),
            "label":      "Raw Stream (Input → Core)",
        },
        "q2": {
            "size":       q2_size,
            "max":        q2_max,
            "fill_ratio": q2_fill,
            "color":      get_queue_color(q2_fill),
            "label":      "Intermediate Stream (Core → Aggregator)",
        },
        "q3": {
            "size":       q3_size,
            "max":        q3_max,
            "fill_ratio": q3_fill,
            "color":      get_queue_color(q3_fill),
            "label":      "Processed Stream (Aggregator → Output)",
        },
    }

class TelemetryObserver(ABC):

    @abstractmethod
    def update(self, telemetry_state: dict) -> None:
        ...

class PipelineTelemetry:

    def __init__(
        self,
        raw_stream,
        intermediate_queue,
        processed_queue,
        queue_max_size: int,
        config: dict,
    ):
        self._raw_stream         = raw_stream
        self._intermediate_queue = intermediate_queue
        self._processed_queue    = processed_queue
        self._queue_max_size     = queue_max_size
        self._telemetry_config   = config["visualizations"]["telemetry"]
        self._poll_interval      = config["pipeline_dynamics"].get(
                                       "telemetry_poll_interval", 0.2)

        self._observers: list = []
        self._running         = False
        self._thread          = None

        self.latest_state: dict = {}

    def subscribe(self, observer: TelemetryObserver) -> None:
        if observer not in self._observers:
            self._observers.append(observer)
            print(f"[Telemetry] Observer registered: {type(observer).__name__}")

    def unsubscribe(self, observer: TelemetryObserver) -> None:
        if observer in self._observers:
            self._observers.remove(observer)

    def _notify_all(self, state: dict) -> None:
        def _notify_one(observer: TelemetryObserver) -> None:
            try:
                observer.update(state)
            except Exception as e:
                print(f"[Telemetry] Observer update error: {e}")

        list(map(_notify_one, self._observers))

    def _poll_loop(self) -> None:
        while self._running:
            try:
                q1_size = (self._raw_stream.qsize()
                           if self._telemetry_config.get("show_raw_stream", True)
                           else 0)
                q2_size = (self._intermediate_queue.qsize()
                           if self._telemetry_config.get("show_intermediate_stream", True)
                           else 0)
                q3_size = (self._processed_queue.qsize()
                           if self._telemetry_config.get("show_processed_stream", True)
                           else 0)

                state = build_telemetry_state(
                    q1_size, self._queue_max_size,
                    q2_size, self._queue_max_size,
                    q3_size, self._queue_max_size,
                )

                self.latest_state = state
                self._notify_all(state)

            except Exception as e:

                print(f"[Telemetry] Poll error: {e}")

            time.sleep(self._poll_interval)

    def start(self) -> None:
        self._running = True
        self._thread  = threading.Thread(
            target=self._poll_loop,
            name="TelemetryThread",
            daemon=True
        )
        self._thread.start()
        print(f"[Telemetry] Started — polling every {self._poll_interval:.2f}s")

    def stop(self) -> None:
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        print("[Telemetry] Stopped.")