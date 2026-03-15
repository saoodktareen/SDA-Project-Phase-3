"""
telemetry.py — Observer Pattern: Pipeline Health Monitoring
===========================================================
Implements the Observer Design Pattern to monitor queue health
without violating the Dependency Inversion Principle.

Two classes:

1. PipelineTelemetry  (Subject / Observable)
   - Holds references to all three queues
   - Polls qsize() on a regular interval
   - Notifies all registered observers with current fill ratios
   - Runs as a daemon thread in main.py (not a separate process,
     so it can access queue objects directly)

2. TelemetryObserver  (Observer / Abstract Base)
   - Interface that any observer must implement
   - Only one concrete observer exists: the Dashboard
   - But the Subject never knows which concrete observer it talks to

The dashboard (output_module.py) subscribes its update method to the
PipelineTelemetry subject. When telemetry polls, it calls observer.update()
with a state dict. The dashboard uses this to color-code progress bars.

Color coding thresholds:
   Green  → fill ratio < 0.40   (queue flowing smoothly)
   Yellow → fill ratio 0.40–0.75 (queue filling up, watch out)
   Red    → fill ratio > 0.75   (heavy backpressure)
"""

import time
import threading
from abc import ABC, abstractmethod


# ─────────────────────────────────────────────────────────────
#  Thresholds for color coding
# ─────────────────────────────────────────────────────────────

THRESHOLD_GREEN  = 0.40   # below this → green
THRESHOLD_YELLOW = 0.75   # below this → yellow, above → red


def get_queue_color(fill_ratio: float) -> str:
    """
    Pure function — maps a fill ratio (0.0 to 1.0) to a color string.

    Args:
        fill_ratio : current queue size / max queue size

    Returns:
        'green', 'yellow', or 'red'
    """
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
    """
    Pure function — builds the telemetry state dict from raw queue sizes.

    Returns a dict with fill ratios and colors for all three queues,
    ready to be passed to observers.
    """
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


# ─────────────────────────────────────────────────────────────
#  TelemetryObserver — Abstract Observer Interface
# ─────────────────────────────────────────────────────────────

class TelemetryObserver(ABC):
    """
    Abstract base class for all telemetry observers.

    Any class that wants to receive telemetry updates must:
      1. Inherit from TelemetryObserver
      2. Implement the update() method

    The Subject (PipelineTelemetry) only ever calls update() —
    it never knows what concrete class is on the other end.
    This preserves the Dependency Inversion Principle.
    """

    @abstractmethod
    def update(self, telemetry_state: dict) -> None:
        """
        Called by PipelineTelemetry every time it polls the queues.

        Args:
            telemetry_state : dict produced by build_telemetry_state()
                              Contains 'q1', 'q2', 'q3' keys, each with:
                                - size       : current number of items
                                - max        : queue capacity
                                - fill_ratio : size/max (0.0 to 1.0)
                                - color      : 'green', 'yellow', or 'red'
                                - label      : human-readable stream name
        """
        ...


# ─────────────────────────────────────────────────────────────
#  PipelineTelemetry — Subject
# ─────────────────────────────────────────────────────────────

class PipelineTelemetry:
    """
    Subject in the Observer pattern.

    Holds references to the three pipeline queues and polls their
    sizes on a configurable interval. Notifies all registered
    TelemetryObserver instances with the latest state.

    Runs as a daemon thread started by main.py so it can directly
    access the multiprocessing.Queue objects (which are shared
    between processes via main.py — not imported by this module).

    The dashboard subscribes itself by calling:
        telemetry.subscribe(dashboard_observer)

    Threading:
        Runs in a background daemon thread — automatically killed
        when the main process exits. No manual cleanup needed.
    """

    POLL_INTERVAL_SECONDS = 0.2   # how often to poll queue sizes

    def __init__(
        self,
        raw_queue,
        intermediate_queue,
        processed_queue,
        queue_max_size: int,
        config: dict,
    ):
        """
        Args:
            raw_queue          : Queue 1 (Input → Core workers)
            intermediate_queue : Queue 2 (Core workers → Aggregator)
            processed_queue    : Queue 3 (Aggregator → Output)
            queue_max_size     : max size for all queues (from config)
            config             : full config.json dict (for show_* flags)
        """
        self._raw_queue          = raw_queue
        self._intermediate_queue = intermediate_queue
        self._processed_queue    = processed_queue
        self._queue_max_size     = queue_max_size
        self._telemetry_config   = config["visualizations"]["telemetry"]
        self._poll_interval      = config["pipeline_dynamics"].get("telemetry_poll_interval", 0.2)

        self._observers: list = []
        self._running    = False
        self._thread: threading.Thread = None

        # Latest snapshot — readable by observers anytime
        self.latest_state: dict = {}

    # ── Observer registration ─────────────────────────────────

    def subscribe(self, observer: TelemetryObserver) -> None:
        """
        Register an observer to receive telemetry updates.

        Args:
            observer : any object implementing TelemetryObserver
        """
        if observer not in self._observers:
            self._observers.append(observer)
            print(f"[Telemetry] Observer registered: {type(observer).__name__}")

    def unsubscribe(self, observer: TelemetryObserver) -> None:
        """Remove a previously registered observer."""
        if observer in self._observers:
            self._observers.remove(observer)

    def _notify_all(self, state: dict) -> None:
        """
        Push the latest telemetry state to all registered observers.
        Called internally after each poll.
        """
        for observer in self._observers:
            try:
                observer.update(state)
            except Exception as e:
                print(f"[Telemetry] Observer update error: {e}")

    # ── Polling loop ──────────────────────────────────────────

    def _poll_loop(self) -> None:
        """
        Internal thread target — runs continuously until stop() is called.

        On each iteration:
          1. Reads qsize() from all three queues
          2. Builds telemetry state (only for streams enabled in config)
          3. Stores in self.latest_state
          4. Notifies all observers
          5. Sleeps POLL_INTERVAL_SECONDS
        """
        while self._running:
            try:
                q1_size = self._raw_queue.qsize()          if self._telemetry_config.get("show_raw_stream", True)          else 0
                q2_size = self._intermediate_queue.qsize() if self._telemetry_config.get("show_intermediate_stream", True) else 0
                q3_size = self._processed_queue.qsize()    if self._telemetry_config.get("show_processed_stream", True)    else 0

                state = build_telemetry_state(
                    q1_size, self._queue_max_size,
                    q2_size, self._queue_max_size,
                    q3_size, self._queue_max_size,
                )

                self.latest_state = state
                self._notify_all(state)

            except Exception as e:
                # qsize() can raise on some platforms — handle gracefully
                print(f"[Telemetry] Poll error: {e}")

            time.sleep(self._poll_interval)

    # ── Lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        """
        Start the telemetry polling loop in a background daemon thread.
        Called by main.py after all queues and observers are set up.
        """
        self._running = True
        self._thread  = threading.Thread(
            target=self._poll_loop,
            name="TelemetryThread",
            daemon=True      # dies automatically when main process exits
        )
        self._thread.start()
        print(f"[Telemetry] Started — polling every {self._poll_interval:.2f}s")

    def stop(self) -> None:
        """Stop the polling loop gracefully."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        print("[Telemetry] Stopped.")