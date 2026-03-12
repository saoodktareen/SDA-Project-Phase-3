"""
output_module.py — Generic Real-Time Dashboard (Output Module)
==============================================================
Consumes processed packets from Queue 3 and renders a live
matplotlib dashboard driven entirely by config.json.

This module is completely domain-agnostic:
  - Chart titles, axis labels, and which fields to plot all come
    from config["visualizations"]["data_charts"]
  - Telemetry bars are driven by config["visualizations"]["telemetry"]
  - No hardcoded references to sensors, GDP, or any specific domain

Implements TelemetryObserver (from telemetry.py):
  - Subscribes to PipelineTelemetry subject
  - Receives queue state updates via update() method
  - Renders color-coded progress bars for backpressure visualization

Dashboard layout (4 subplots):
  ┌─────────────────────────────────────────┐
  │  [Chart 1] Live metric_value line graph │
  ├─────────────────────────────────────────┤
  │  [Chart 2] Running average line graph   │
  ├─────────────────────────────────────────┤
  │  [Telemetry] Queue 1 fill bar           │
  │  [Telemetry] Queue 2 fill bar           │
  │  [Telemetry] Queue 3 fill bar           │
  └─────────────────────────────────────────┘
"""

import multiprocessing
import threading
import collections
import matplotlib
matplotlib.use("TkAgg")   # explicit backend — avoids display issues on some systems
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.ticker import MaxNLocator

from telemetry import TelemetryObserver


# ─────────────────────────────────────────────────────────────
#  Color constants
# ─────────────────────────────────────────────────────────────

COLORS = {
    "background":   "#0f1117",
    "panel":        "#1a1d27",
    "border":       "#2a2d3e",
    "text_primary": "#e2e8f0",
    "text_dim":     "#64748b",
    "accent_blue":  "#3b82f6",
    "accent_cyan":  "#06b6d4",
    "accent_green": "#10b981",
    "green":        "#22c55e",
    "yellow":       "#f59e0b",
    "red":          "#ef4444",
    "grid":         "#1e2130",
}

QUEUE_COLORS = {
    "green":  COLORS["green"],
    "yellow": COLORS["yellow"],
    "red":    COLORS["red"],
}

# Max data points to keep on charts before scrolling
MAX_DISPLAY_POINTS = 100


# ─────────────────────────────────────────────────────────────
#  OutputModule — Dashboard + TelemetryObserver
# ─────────────────────────────────────────────────────────────

class OutputModule(TelemetryObserver):
    """
    Live real-time dashboard — Consumer of Queue 3.

    Inherits TelemetryObserver so it can be subscribed to
    PipelineTelemetry without violating DIP.

    Runs matplotlib FuncAnimation in the main thread (required by
    matplotlib on most platforms). Packet consumption from Queue 3
    happens inside the animation callback, keeping everything
    thread-safe without explicit locks.

    Multiprocessing role:
        Runs as a separate multiprocessing.Process started by main.py.
        The telemetry thread runs inside this same process.
    """

    def __init__(self, config: dict, processed_queue: multiprocessing.Queue):
        """
        Args:
            config          : full config.json dict
            processed_queue : Queue 3 — reads completed packets from here
        """
        self.processed_queue = processed_queue
        self.viz_config      = config["visualizations"]
        self.charts_config   = self.viz_config["data_charts"]
        self.tel_config      = self.viz_config["telemetry"]

        # ── Data buffers (scrolling windows) ─────────────────
        self.x_values    = collections.deque(maxlen=MAX_DISPLAY_POINTS)
        self.y_values    = collections.deque(maxlen=MAX_DISPLAY_POINTS)
        self.avg_x       = collections.deque(maxlen=MAX_DISPLAY_POINTS)
        self.avg_y       = collections.deque(maxlen=MAX_DISPLAY_POINTS)

        # ── Telemetry state (updated by Observer.update()) ────
        # Protected by a threading.Lock because telemetry runs
        # in a background thread while animation runs in main thread
        self._tel_lock   = threading.Lock()
        self._tel_state  = {}

        # ── Stats counters ────────────────────────────────────
        self.packets_received = 0
        self.stream_done      = False

        # ── Chart config lookups ──────────────────────────────
        self._values_chart = self._find_chart("real_time_line_graph_values")
        self._avg_chart    = self._find_chart("real_time_line_graph_average")

        # ── Matplotlib figure — built in run() ───────────────
        self.fig  = None
        self.axes = {}

    def _find_chart(self, chart_type: str) -> dict:
        """Return the chart config dict for a given type, or a safe default."""
        for chart in self.charts_config:
            if chart["type"] == chart_type:
                return chart
        return {"title": chart_type, "x_axis": "x", "y_axis": "y"}

    # ── TelemetryObserver interface ───────────────────────────

    def update(self, telemetry_state: dict) -> None:
        """
        Called by PipelineTelemetry (Subject) on every poll cycle.
        Stores state thread-safely for the animation loop to render.
        """
        with self._tel_lock:
            self._tel_state = telemetry_state

    # ── Figure construction ───────────────────────────────────

    def _build_figure(self):
        """
        Build the matplotlib figure with a professional dark theme.
        Uses GridSpec for precise layout control.
        """
        plt.style.use("dark_background")
        plt.rcParams.update({
            "font.family":       "monospace",
            "font.size":         10,
            "axes.titlesize":    12,
            "axes.titleweight":  "bold",
            "axes.labelsize":    10,
            "axes.edgecolor":    COLORS["border"],
            "axes.linewidth":    1.2,
            "figure.facecolor":  COLORS["background"],
            "axes.facecolor":    COLORS["panel"],
            "grid.color":        COLORS["grid"],
            "grid.linewidth":    0.8,
            "xtick.color":       COLORS["text_dim"],
            "ytick.color":       COLORS["text_dim"],
            "text.color":        COLORS["text_primary"],
        })

        self.fig = plt.figure(figsize=(14, 9), facecolor=COLORS["background"])
        self.fig.canvas.manager.set_window_title("Phase 3 — Real-Time Pipeline Dashboard")

        # GridSpec: 2 chart rows + 1 telemetry row
        # Chart rows are tall, telemetry row is short
        gs = gridspec.GridSpec(
            3, 1,
            figure=self.fig,
            height_ratios=[3, 3, 2],
            hspace=0.45,
            left=0.08, right=0.97,
            top=0.93,  bottom=0.06,
        )

        # ── Chart 1: Live metric values ───────────────────────
        ax_values = self.fig.add_subplot(gs[0])
        ax_values.set_title(
            self._values_chart["title"],
            color=COLORS["accent_blue"], pad=8
        )
        ax_values.set_xlabel(self._values_chart["x_axis"], color=COLORS["text_dim"])
        ax_values.set_ylabel(self._values_chart["y_axis"], color=COLORS["text_dim"])
        ax_values.grid(True, alpha=0.4)
        ax_values.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=8))
        self.line_values, = ax_values.plot(
            [], [], color=COLORS["accent_blue"],
            linewidth=1.8, alpha=0.9, label="Verified Value"
        )
        self.scatter_values = ax_values.scatter(
            [], [], color=COLORS["accent_cyan"],
            s=18, zorder=5, alpha=0.7
        )
        ax_values.legend(loc="upper left", fontsize=9,
                         facecolor=COLORS["panel"], edgecolor=COLORS["border"])

        # ── Chart 2: Running average ──────────────────────────
        ax_avg = self.fig.add_subplot(gs[1])
        ax_avg.set_title(
            self._avg_chart["title"],
            color=COLORS["accent_green"], pad=8
        )
        ax_avg.set_xlabel(self._avg_chart["x_axis"], color=COLORS["text_dim"])
        ax_avg.set_ylabel(self._avg_chart["y_axis"], color=COLORS["text_dim"])
        ax_avg.grid(True, alpha=0.4)
        ax_avg.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=8))
        self.line_avg, = ax_avg.plot(
            [], [], color=COLORS["accent_green"],
            linewidth=2.0, alpha=0.9, label="Running Avg"
        )
        ax_avg.legend(loc="upper left", fontsize=9,
                      facecolor=COLORS["panel"], edgecolor=COLORS["border"])

        # ── Telemetry panel ───────────────────────────────────
        ax_tel = self.fig.add_subplot(gs[2])
        ax_tel.set_facecolor(COLORS["background"])
        ax_tel.set_title("Pipeline Telemetry — Queue Health", 
                         color=COLORS["text_dim"], pad=6, fontsize=10)
        ax_tel.axis("off")

        # Title bar
        self.fig.text(
            0.5, 0.97,
            "⬡  REAL-TIME SENSOR PIPELINE  ⬡",
            ha="center", va="top",
            fontsize=13, fontweight="bold",
            color=COLORS["accent_blue"],
            fontfamily="monospace"
        )

        self.axes = {
            "values": ax_values,
            "avg":    ax_avg,
            "tel":    ax_tel,
        }

    # ── Animation frame update ────────────────────────────────

    def _animate(self, frame):
        """
        Called by FuncAnimation on every interval tick.

        1. Drains up to 20 packets from Queue 3 (batched for smoothness)
        2. Updates both line charts
        3. Redraws telemetry bars from latest Observer state
        """
        # ── 1. Consume new packets ────────────────────────────
        packets_this_frame = 0
        while packets_this_frame < 20:
            try:
                packet = self.processed_queue.get_nowait()
            except Exception:
                break

            if packet is None:
                # Sentinel — stream is complete
                self.stream_done = True
                self._mark_stream_complete()
                break

            # Extract x and y fields from packet using chart config
            x_field   = self._values_chart["x_axis"]    # e.g. "time_period"
            y_field   = self._values_chart["y_axis"]    # e.g. "metric_value"
            avg_field = self._avg_chart["y_axis"]       # e.g. "computed_metric"

            if x_field in packet and y_field in packet:
                self.x_values.append(packet[x_field])
                self.y_values.append(packet[y_field])

            if x_field in packet and avg_field in packet:
                self.avg_x.append(packet[x_field])
                self.avg_y.append(packet[avg_field])

            self.packets_received += 1
            packets_this_frame    += 1

        # ── 2. Update line charts ─────────────────────────────
        if self.x_values:
            x_list = list(self.x_values)
            y_list = list(self.y_values)

            self.line_values.set_data(x_list, y_list)

            # Update scatter (last point highlighted)
            self.scatter_values.set_offsets(
                [[x_list[-1], y_list[-1]]]
            )

            ax = self.axes["values"]
            ax.relim()
            ax.autoscale_view()

        if self.avg_x:
            self.line_avg.set_data(list(self.avg_x), list(self.avg_y))
            ax = self.axes["avg"]
            ax.relim()
            ax.autoscale_view()

        # ── 3. Redraw telemetry bars ──────────────────────────
        self._redraw_telemetry()

        return (self.line_values, self.line_avg, self.scatter_values)

    def _redraw_telemetry(self):
        """
        Redraws the three queue health bars using latest telemetry state.
        Color-coded: green / yellow / red based on fill ratio.
        """
        ax = self.axes["tel"]
        ax.cla()
        ax.set_facecolor(COLORS["background"])
        ax.axis("off")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_title(
            f"Pipeline Telemetry — Queue Health  |  Packets Processed: {self.packets_received}",
            color=COLORS["text_dim"], pad=6, fontsize=10
        )

        with self._tel_lock:
            state = dict(self._tel_state)

        if not state:
            ax.text(0.5, 0.5, "Waiting for telemetry...",
                    ha="center", va="center",
                    color=COLORS["text_dim"], fontsize=10)
            return

        queue_keys = ["q1", "q2", "q3"]
        bar_height = 0.18
        bar_gap    = 0.28
        bar_left   = 0.22
        bar_right  = 0.75

        for i, key in enumerate(queue_keys):
            if key not in state:
                continue

            q        = state[key]
            y_pos    = 0.75 - i * bar_gap
            fill     = q["fill_ratio"]
            color    = QUEUE_COLORS.get(q["color"], COLORS["green"])
            label    = q["label"]
            size_str = f"{q['size']}/{q['max']}"

            # Background track
            ax.barh(
                y_pos, bar_right - bar_left,
                left=bar_left, height=bar_height,
                color=COLORS["panel"], edgecolor=COLORS["border"],
                linewidth=1.0
            )

            # Filled portion
            fill_width = (bar_right - bar_left) * fill
            if fill_width > 0:
                ax.barh(
                    y_pos, fill_width,
                    left=bar_left, height=bar_height,
                    color=color, alpha=0.85, edgecolor="none"
                )

            # Label (left)
            ax.text(
                bar_left - 0.01, y_pos,
                label,
                ha="right", va="center",
                color=COLORS["text_primary"],
                fontsize=8, fontfamily="monospace"
            )

            # Size (right)
            ax.text(
                bar_right + 0.01, y_pos,
                size_str,
                ha="left", va="center",
                color=color,
                fontsize=9, fontweight="bold", fontfamily="monospace"
            )

            # Percent inside bar
            pct_text = f"{fill*100:.0f}%"
            ax.text(
                bar_left + fill_width / 2 if fill_width > 0.05 else bar_left + 0.02,
                y_pos,
                pct_text,
                ha="center", va="center",
                color="white" if fill > 0.15 else COLORS["text_dim"],
                fontsize=8, fontweight="bold"
            )

        # Legend
        legend_y = 0.03
        for status, color in [("● Flowing", "green"), ("● Filling", "yellow"), ("● Backpressure", "red")]:
            ax.text(
                {"● Flowing": 0.25, "● Filling": 0.50, "● Backpressure": 0.72}[status],
                legend_y, status,
                ha="center", va="bottom",
                color=QUEUE_COLORS[color], fontsize=8
            )

    def _mark_stream_complete(self):
        """Add a 'Stream Complete' annotation to both charts."""
        for key in ["values", "avg"]:
            ax = self.axes[key]
            ax.set_title(
                ax.get_title() + "  ✓ Complete",
                color=COLORS["accent_green"]
            )

    # ── Entry point ───────────────────────────────────────────

    def run(self) -> None:
        """
        Entry point — called inside a multiprocessing.Process by main.py.

        Builds the figure and starts FuncAnimation. Blocks until the
        window is closed (plt.show() is blocking).
        """
        print("[Output] Dashboard starting...")
        self._build_figure()

        ani = animation.FuncAnimation(
            self.fig,
            self._animate,
            interval=150,        # ms between frames
            blit=False,          # blit=False needed for telemetry redraws
            cache_frame_data=False,
        )

        # Keep reference so GC doesn't kill the animation
        self._ani = ani

        plt.show()
        print("[Output] Dashboard closed.")