"""
output_module.py — Generic Real-Time Dashboard (Matplotlib)
============================================================
Consumes processed packets from Queue 3 and renders a live
matplotlib dashboard driven entirely by config.json.

This module is completely domain-agnostic:
  - Chart titles, axis labels, and fields to plot all come from
    config["visualizations"]["data_charts"]
  - Telemetry bars are driven by config["visualizations"]["telemetry"]
  - No hardcoded references to sensors, GDP, or any specific domain

Implements TelemetryObserver (from plugins/telemetry.py):
  - Subscribes to PipelineTelemetry subject
  - Receives queue state updates via update() method
  - Renders color-coded progress bars for backpressure visualization

Backend auto-detection:
  Tries backends in order: TkAgg -> Qt5Agg -> Qt6Agg -> WxAgg -> Agg
  This makes the dashboard portable across Windows, macOS, and Linux
  without any manual configuration from the user.

Dashboard layout (3 panels via GridSpec):
  +---------------------------------------------+
  |  [Chart 1] Live metric_value line graph      |
  +---------------------------------------------+
  |  [Chart 2] Running average line graph        |
  +---------------------------------------------+
  |  [Telemetry] Q1 / Q2 / Q3 health bars        |
  +---------------------------------------------+
"""

import threading
import collections
import multiprocessing


# ─────────────────────────────────────────────────────────────
#  Backend auto-detection
#  Tries each backend in order, uses the first one that works.
#  This means the dashboard works on any machine without the user
#  needing to install or configure anything extra.
# ─────────────────────────────────────────────────────────────

def _set_matplotlib_backend() -> str:
    """
    Try matplotlib backends in order of preference and use the first
    one that imports successfully.

    Returns the name of the backend that was selected.
    """
    import matplotlib
    backends = ["TkAgg", "Qt5Agg", "Qt6Agg", "WxAgg", "MacOSX", "Agg"]
    for backend in backends:
        try:
            matplotlib.use(backend)
            # Force a test import to confirm the backend actually works
            import matplotlib.pyplot as plt
            plt.switch_backend(backend)
            print(f"[Output] Using matplotlib backend: {backend}")
            return backend
        except Exception:
            continue
    # Agg is always available as a last resort (no window — saves to file)
    matplotlib.use("Agg")
    print("[Output] WARNING — using Agg backend (no display). "
          "Install tkinter or PyQt5 for a live window.")
    return "Agg"


_BACKEND = _set_matplotlib_backend()

import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.gridspec as gridspec
from matplotlib.ticker import MaxNLocator

from plugins.telemetry import TelemetryObserver


# ─────────────────────────────────────────────────────────────
#  Color palette
# ─────────────────────────────────────────────────────────────

BG         = "#0f1117"
PANEL      = "#1a1d27"
BORDER     = "#2a2d3e"
TEXT       = "#e2e8f0"
DIM        = "#64748b"
BLUE       = "#3b82f6"
CYAN       = "#06b6d4"
GREEN      = "#10b981"
BAR_GREEN  = "#22c55e"
BAR_YELLOW = "#f59e0b"
BAR_RED    = "#ef4444"

QUEUE_BAR_COLORS = {
    "green":  BAR_GREEN,
    "yellow": BAR_YELLOW,
    "red":    BAR_RED,
}

MAX_POINTS = 120   # rolling window of data points on each chart


# ─────────────────────────────────────────────────────────────
#  OutputModule
# ─────────────────────────────────────────────────────────────

class OutputModule(TelemetryObserver):
    """
    Live real-time matplotlib dashboard — Consumer of Queue 3.

    Inherits TelemetryObserver so it can be subscribed to
    PipelineTelemetry without violating DIP. The Subject only ever
    calls update() and never imports OutputModule directly.

    Uses matplotlib FuncAnimation to drive live chart updates every
    150ms. Packet consumption happens inside the animation callback
    so the chart data buffers are only ever touched on the main thread
    — no extra locking needed for them.

    The telemetry state IS written from a background thread, so
    self._tel_state is protected by self._tel_lock.
    """

    def __init__(self, config: dict, processed_queue: multiprocessing.Queue):
        """
        Args:
            config          : full validated config dict
            processed_queue : Queue 3 — reads completed packets from here
        """
        self.processed_queue = processed_queue
        self._charts_cfg     = config["visualizations"]["data_charts"]
        self._tel_cfg        = config["visualizations"]["telemetry"]

        # Chart config lookups
        self._val_chart = self._find_chart("real_time_line_graph_values")
        self._avg_chart = self._find_chart("real_time_line_graph_average")

        # Rolling data buffers — only touched by animation callback (main thread)
        self._x_vals = collections.deque(maxlen=MAX_POINTS)
        self._y_vals = collections.deque(maxlen=MAX_POINTS)
        self._avg_x  = collections.deque(maxlen=MAX_POINTS)
        self._avg_y  = collections.deque(maxlen=MAX_POINTS)

        # Stats
        self._packets_received = 0
        self._stream_done      = False

        # Telemetry state — written by background thread, read by animation
        self._tel_lock  = threading.Lock()
        self._tel_state = {}

        # Matplotlib objects — built in run()
        self._fig       = None
        self._ax_val    = None
        self._ax_avg    = None
        self._ax_tel    = None
        self._line_val  = None
        self._line_avg  = None
        self._scatter   = None
        self._animation = None

    # ─────────────────────────────────────────────────────────
    #  TelemetryObserver interface
    # ─────────────────────────────────────────────────────────

    def update(self, telemetry_state: dict) -> None:
        """
        Called by PipelineTelemetry (Subject) on every poll cycle.
        Stores state thread-safely for the animation loop to render.
        """
        with self._tel_lock:
            self._tel_state = telemetry_state

    # ─────────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────────

    def _find_chart(self, chart_type: str) -> dict:
        """Return chart config dict for a given type, or a safe default."""
        for chart in self._charts_cfg:
            if chart["type"] == chart_type:
                return chart
        return {"title": chart_type, "x_axis": "x", "y_axis": "y"}

    # ─────────────────────────────────────────────────────────
    #  Figure construction
    # ─────────────────────────────────────────────────────────

    def _build_figure(self) -> None:
        """
        Build the dark-themed matplotlib figure with 3 stacked panels.

        Uses figsize=(13, 9) as a sensible default that fits most laptop
        screens. The window is also set to a maximized state where the
        backend supports it — this is done safely so it never crashes on
        backends that don't support maximize.
        """
        plt.style.use("dark_background")
        plt.rcParams.update({
            "font.family":      "monospace",
            "font.size":        10,
            "axes.titlesize":   11,
            "axes.titleweight": "bold",
            "axes.labelsize":   9,
            "axes.edgecolor":   BORDER,
            "axes.linewidth":   1.2,
            "figure.facecolor": BG,
            "axes.facecolor":   PANEL,
            "grid.color":       DIM,
            "grid.linewidth":   0.6,
            "xtick.color":      DIM,
            "ytick.color":      DIM,
            "text.color":       TEXT,
        })

        self._fig = plt.figure(figsize=(13, 9), facecolor=BG)

        # ── Set window title safely ─────────────────────────── REMOVED TITLE
        # try:
        #     self._fig.canvas.manager.set_window_title(
        #         "Phase 3 — Real-Time Sensor Pipeline Dashboard"
        #     )
        # except Exception:
        #     pass   # some backends don't support window titles

        # ── Maximize window safely ────────────────────────────
        # Each backend uses a different API for this — try them all
        # and silently ignore failures.
        try:
            mgr = plt.get_current_fig_manager()
            try:
                mgr.window.state("zoomed")          # TkAgg on Windows
            except Exception:
                try:
                    mgr.window.showMaximized()       # Qt5Agg / Qt6Agg
                except Exception:
                    try:
                        mgr.frame.Maximize(True)     # WxAgg
                    except Exception:
                        pass                         # Agg / MacOSX — no window
        except Exception:
            pass

        # GridSpec: 2 tall chart rows + 1 short telemetry row
        gs = gridspec.GridSpec(
            3, 1,
            figure=self._fig,
            height_ratios=[3, 3, 2],
            hspace=0.55,
            left=0.08, right=0.97,
            top=0.92,  bottom=0.06,
        )

        # ── Chart 1: live metric values ───────────────────────
        self._ax_val = self._fig.add_subplot(gs[0])
        self._ax_val.set_facecolor(PANEL)
        self._ax_val.set_title(self._val_chart["title"], color=BLUE, pad=8)
        self._ax_val.set_xlabel(self._val_chart["x_axis"], color=DIM, labelpad=4)
        self._ax_val.set_ylabel(self._val_chart["y_axis"], color=DIM, labelpad=4)
        self._ax_val.grid(True, alpha=0.35)
        self._ax_val.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=6))

        self._line_val, = self._ax_val.plot(
            [], [], color=BLUE, linewidth=1.8, alpha=0.9, label="Verified value"
        )
        self._scatter = self._ax_val.scatter(
            [], [], color=CYAN, s=20, zorder=5, alpha=0.8
        )
        self._ax_val.legend(
            loc="upper left", fontsize=8,
            facecolor=PANEL, edgecolor=BORDER,
        )

        # ── Chart 2: running average ──────────────────────────
        self._ax_avg = self._fig.add_subplot(gs[1])
        self._ax_avg.set_facecolor(PANEL)
        self._ax_avg.set_title(self._avg_chart["title"], color=GREEN, pad=8)
        self._ax_avg.set_xlabel(self._avg_chart["x_axis"], color=DIM, labelpad=4)
        self._ax_avg.set_ylabel(self._avg_chart["y_axis"], color=DIM, labelpad=4)
        self._ax_avg.grid(True, alpha=0.35)
        self._ax_avg.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=6))

        self._line_avg, = self._ax_avg.plot(
            [], [], color=GREEN, linewidth=2.2, alpha=0.9, label="Running avg"
        )
        self._ax_avg.legend(
            loc="upper left", fontsize=8,
            facecolor=PANEL, edgecolor=BORDER,
        )

        # ── Telemetry panel ───────────────────────────────────
        self._ax_tel = self._fig.add_subplot(gs[2])
        self._ax_tel.set_facecolor(BG)
        self._ax_tel.axis("off")

        # Dashboard title REMOVED
        # self._fig.text(
        #     0.5, 0.965,
        #     "REAL-TIME SENSOR PIPELINE",
        #     ha="center", va="top",
        #     fontsize=13, fontweight="bold",
        #     color=BLUE, fontfamily="monospace",
        # )

    # ─────────────────────────────────────────────────────────
    #  Animation frame callback
    # ─────────────────────────────────────────────────────────

    def _animate(self, frame: int):
        """
        Called by FuncAnimation every 150ms.

        1. Drains up to 25 packets from processed_queue
        2. Updates chart 1 (metric values)
        3. Updates chart 2 (running average)
        4. Redraws telemetry health bars
        """
        # Step 1: consume new packets
        for _ in range(25):
            try:
                packet = self.processed_queue.get_nowait()
            except Exception:
                break

            if packet is None:
                self._stream_done = True
                self._mark_complete()
                break

            # Field names come from config — never hardcoded
            x_key   = self._val_chart["x_axis"]    # "time_period"
            y_key   = self._val_chart["y_axis"]    # "metric_value"
            avg_key = self._avg_chart["y_axis"]    # "computed_metric"

            if x_key in packet and y_key in packet:
                self._x_vals.append(packet[x_key])
                self._y_vals.append(packet[y_key])

            if x_key in packet and avg_key in packet:
                self._avg_x.append(packet[x_key])
                self._avg_y.append(packet[avg_key])

            self._packets_received += 1

        # Step 2: update chart 1
        if self._x_vals:
            xs = list(self._x_vals)
            ys = list(self._y_vals)
            self._line_val.set_data(xs, ys)
            self._scatter.set_offsets([[xs[-1], ys[-1]]])
            self._ax_val.relim()
            self._ax_val.autoscale_view()

        # Step 3: update chart 2
        if self._avg_x:
            self._line_avg.set_data(list(self._avg_x), list(self._avg_y))
            self._ax_avg.relim()
            self._ax_avg.autoscale_view()

        # Step 4: redraw telemetry bars
        self._draw_telemetry()

        return self._line_val, self._line_avg

    # ─────────────────────────────────────────────────────────
    #  Telemetry bar renderer
    # ─────────────────────────────────────────────────────────

    def _draw_telemetry(self) -> None:
        """
        Redraws the three queue health bars on every animation frame.

        Green  = fill < 40%   (flowing smoothly)
        Yellow = fill 40-75%  (filling up)
        Red    = fill > 75%   (heavy backpressure)
        """
        ax = self._ax_tel
        ax.cla()
        ax.set_facecolor(BG)
        ax.axis("off")
        ax.set_xlim(0, 1)
        ax.set_ylim(-0.1, 1)
        ax.set_title(
            f"Pipeline Telemetry — Queue Health"
            f"   |   Packets Processed: {self._packets_received}",
            color=DIM, fontsize=9, pad=4,
        )

        with self._tel_lock:
            state = dict(self._tel_state)

        if not state:
            ax.text(
                0.5, 0.5, "Waiting for telemetry...",
                ha="center", va="center", color=DIM, fontsize=9,
            )
            return

        bar_left   = 0.22
        bar_right  = 0.76
        bar_width  = bar_right - bar_left
        bar_height = 0.18
        y_slots    = [0.70, 0.42, 0.14]

        for key, y in zip(["q1", "q2", "q3"], y_slots):
            if key not in state:
                continue

            q     = state[key]
            fill  = q["fill_ratio"]
            color = QUEUE_BAR_COLORS.get(q["color"], BAR_GREEN)
            pct   = int(fill * 100)

            # Background track
            ax.barh(
                y, bar_width, left=bar_left, height=bar_height,
                color=PANEL, edgecolor=BORDER, linewidth=0.8,
            )

            # Filled portion
            if fill > 0:
                ax.barh(
                    y, bar_width * fill, left=bar_left, height=bar_height,
                    color=color, alpha=0.85, edgecolor="none",
                )

            # Label left
            ax.text(
                bar_left - 0.01, y, q["label"],
                ha="right", va="center",
                color=TEXT, fontsize=7.5, fontfamily="monospace",
            )

            # Size right
            ax.text(
                bar_right + 0.01, y, f"{q['size']}/{q['max']}",
                ha="left", va="center",
                color=color, fontsize=8.5,
                fontweight="bold", fontfamily="monospace",
            )

            # Percentage inside bar
            if fill > 0.12:
                ax.text(
                    bar_left + (bar_width * fill) / 2, y,
                    f"{pct}%",
                    ha="center", va="center",
                    color="white", fontsize=8, fontweight="bold",
                )

        # Legend
        for label, color, xpos in [
            ("● Flowing",      BAR_GREEN,  0.28),
            ("● Filling",      BAR_YELLOW, 0.50),
            ("● Backpressure", BAR_RED,    0.72),
        ]:
            ax.text(
                xpos, -0.08, label,
                ha="center", va="bottom",
                color=color, fontsize=7.5,
            )

    def _mark_complete(self) -> None:
        """Update chart titles when the stream finishes."""
        self._ax_val.set_title(
            self._val_chart["title"] + "  [Complete]",
            color=GREEN, pad=8,
        )
        self._ax_avg.set_title(
            self._avg_chart["title"] + "  [Complete]",
            color=GREEN, pad=8,
        )

    # ─────────────────────────────────────────────────────────
    #  Entry point
    # ─────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        Entry point — called inside a multiprocessing.Process by main.py.

        Builds the figure, starts FuncAnimation, then calls plt.show()
        which blocks until the user closes the window.
        """
        print(f"[Output] Dashboard starting (backend: {_BACKEND})...")
        self._build_figure()

        # blit=False — required because telemetry does ax.cla() each frame
        self._animation = animation.FuncAnimation(
            self._fig,
            self._animate,
            interval=150,
            blit=False,
            cache_frame_data=False,
        )

        plt.show()
        print("[Output] Dashboard closed.")