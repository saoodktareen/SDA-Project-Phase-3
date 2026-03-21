"""
output_module.py — Two-Screen Matplotlib Dashboard
====================================================
Screen 1 — Status Screen (shown first, always):
  Shows pipeline status, errors, and warnings.
  ┌──────────────────────────────────────────────┐
  │  Phase 3 — Generic Concurrent Pipeline        │
  │                                              │
  │  [RED]   FATAL errors (config / CSV issues)  │
  │    → No button shown. Program exits.         │
  │                                              │
  │  [BLUE]  Skipped row warnings (non-fatal)    │
  │    → "▶  Start Pipeline" button shown.       │
  │                                              │
  │  [GREEN] No errors at all                    │
  │    → "▶  Start Pipeline" button shown.       │
  │                                              │
  │          [ ▶  Start Pipeline ]               │  ← bottom-left button
  └──────────────────────────────────────────────┘

Screen 2 — Live Dashboard (shown after button click):
  Processing starts when button is clicked.
  ┌──────────────────────────────────────────────┐
  │  [Chart 0] — from config data_charts[0]      │
  │  [Chart 1] — from config data_charts[1]      │
  │  ...one panel per config entry...            │
  │  [Telemetry] Q1 / Q2 / Q3 health bars        │
  └──────────────────────────────────────────────┘

Screen size: figsize=(10, 7) inches — fits 12-inch, 14-inch, and 16-inch
laptops without overflowing. Change FIGURE_SIZE to adjust if needed.

DIP compliance:
  OutputModule depends only on processed_queue + config + error_dict.
  It never imports InputModule, CoreWorker, or Aggregator.
  It implements TelemetryObserver — Subject never imports it directly.
"""

import threading
import collections
import multiprocessing

# ─────────────────────────────────────────────────────────────
#  Backend auto-detection — must happen before pyplot import
# ─────────────────────────────────────────────────────────────

def _set_matplotlib_backend() -> str:
    import matplotlib
    backends = ["TkAgg", "Qt5Agg", "Qt6Agg", "WxAgg", "MacOSX", "Agg"]
    for backend in backends:
        try:
            matplotlib.use(backend)
            import matplotlib.pyplot as plt
            plt.switch_backend(backend)
            print(f"[Output] Using matplotlib backend: {backend}")
            return backend
        except Exception:
            continue
    matplotlib.use("Agg")
    print("[Output] WARNING — Agg backend (no display window).")
    return "Agg"


# Matplotlib imports are deferred inside run() — NOT at module import time.
# On Windows multiprocessing uses "spawn" which re-imports every module in
# every child process. If matplotlib is imported here, _set_matplotlib_backend()
# runs in ALL 7 child processes and prints "[Output] Using backend" 7 times.
# Deferring to run() means it only runs once, in OutputProcess only.

from plugins.telemetry import TelemetryObserver
from config.validate_config import SUPPORTED_CHART_TYPES

# Filled in by _init_matplotlib() inside run() — do not use before that
plt         = None
animation   = None
gridspec    = None
Button      = None
MaxNLocator = None
_BACKEND    = None


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
AMBER      = "#f59e0b"
BAR_GREEN  = "#22c55e"
BAR_YELLOW = "#f59e0b"
BAR_RED    = "#ef4444"

CHART_COLORS = [BLUE, GREEN, AMBER, CYAN, "#a78bfa", "#fb923c"]

QUEUE_BAR_COLORS = {
    "green":  BAR_GREEN,
    "yellow": BAR_YELLOW,
    "red":    BAR_RED,
}

MAX_POINTS = 120


# ─────────────────────────────────────────────────────────────
#  y-axis key — driven by chart type, not config y_axis string
# ─────────────────────────────────────────────────────────────

_Y_KEY = {
    "real_time_line_graph_values":  "metric_value",
    "real_time_line_graph_average": "computed_metric",
    "real_time_bar_chart_values":   "metric_value",
    "real_time_bar_chart_average":  "computed_metric",
    "real_time_scatter_values":     "metric_value",
    "real_time_scatter_average":    "computed_metric",
}


# ─────────────────────────────────────────────────────────────
#  Window size
#  Width is fixed at 10 inches.
#  Height is calculated dynamically based on number of chart rows:
#    1 row  of charts → 7  inches  (1-2 charts)
#    2 rows of charts → 8  inches  (3-4 charts)
#    3 rows of charts → 9.5 inches (5-6 charts)
#  This keeps every panel readable on any laptop screen.
# ─────────────────────────────────────────────────────────────

FIGURE_WIDTH = 11   # fixed width in inches — fits 12/14/16-inch laptops

def _figure_height(n_chart_rows: int) -> float:
    """Return figure height in inches based on number of chart rows."""
    # 2.5 inches per chart row + 2 inches base for telemetry and padding
    return min(2.0 * n_chart_rows + 1.5, 8.0)   # cap at 8.0 for small screens


def _init_matplotlib():
    """
    Import matplotlib and select backend.
    Called once at the start of run() — only executes in OutputProcess.
    """
    global plt, animation, gridspec, Button, MaxNLocator, _BACKEND
    _BACKEND = _set_matplotlib_backend()
    import matplotlib.pyplot as _plt
    import matplotlib.animation as _anim
    import matplotlib.gridspec as _gs
    from matplotlib.widgets import Button as _Btn
    from matplotlib.ticker import MaxNLocator as _MNL
    plt         = _plt
    animation   = _anim
    gridspec    = _gs
    Button      = _Btn
    MaxNLocator = _MNL


# ─────────────────────────────────────────────────────────────
#  OutputModule
# ─────────────────────────────────────────────────────────────

class OutputModule(TelemetryObserver):
    """
    Two-screen matplotlib dashboard.

    Screen 1: status/error screen — always shown first.
    Screen 2: live charts + telemetry — shown after button click.

    Processing (consuming from processed_queue) starts only when
    the user clicks the button on Screen 1. This guarantees errors
    are always visible before any graphs appear.
    """

    def __init__(self, config: dict,
                 processed_queue: multiprocessing.Queue,
                 error_dict=None,
                 ready_event=None,
                 start_event=None):
        """
        Parameters
        ----------
        config          : full validated config dict
        processed_queue : Queue 3 — reads completed packets from here
        error_dict      : Manager dict with 'fatal_errors' and 'skipped_rows'
                          Written by InputModule via run_input() in main.py.
        ready_event     : multiprocessing.Event set by run_input() after
                          error_dict is populated. Screen 1 waits on this
                          so it always shows the real error state, not the
                          empty initial state.
        """
        self._processed_queue = processed_queue
        self._error_dict      = error_dict
        self._ready_event     = ready_event   # wait on this before drawing Screen 1
        self._start_event     = start_event   # set on button click
        self._charts_cfg      = config["visualizations"]["data_charts"]
        self._tel_cfg         = config["visualizations"]["telemetry"]

        # Chart data buffers — one per config entry
        self._chart_buffers: list[dict] = [
            {
                "x":   collections.deque(maxlen=MAX_POINTS),
                "y":   collections.deque(maxlen=MAX_POINTS),
                "cfg": chart_cfg,
            }
            for chart_cfg in self._charts_cfg
        ]

        self._packets_received = 0
        self._stream_done      = False

        # Telemetry — written from background thread, protected by lock
        self._tel_lock  = threading.Lock()
        self._tel_state = {}

        # Screen state — False = Screen 1, True = Screen 2
        self._pipeline_started = False

        # Matplotlib objects
        self._fig       = None
        self._animation = None
        self._btn       = None      # Button widget

        # Screen 1 axes
        self._ax_status = None

        # Screen 2 axes
        self._axes      = []
        self._lines     = []
        self._scatters  = []
        self._ax_tel    = None

    # ── TelemetryObserver interface ───────────────────────────

    def update(self, telemetry_state: dict) -> None:
        """Called by PipelineTelemetry on every poll cycle."""
        with self._tel_lock:
            self._tel_state = telemetry_state

    def _get_errors(self):
        """Read errors from Manager dict safely."""
        if self._error_dict is None:
            return [], []
        fatal   = list(self._error_dict.get("fatal_errors", []))
        skipped = list(self._error_dict.get("skipped_rows",  []))
        return fatal, skipped

    # ─────────────────────────────────────────────────────────
    #  Screen 1 — Status / Error screen
    # ─────────────────────────────────────────────────────────

    def _build_screen1(self) -> None:
        """Build the status screen layout."""
        plt.style.use("dark_background")
        plt.rcParams.update({
            "font.family":      "monospace",
            "figure.facecolor": BG,
        })

        # Screen 1 is always a fixed compact size — no charts here
        self._fig = plt.figure(figsize=(FIGURE_WIDTH, 6), facecolor=BG)

        # Status occupies the full figure
        # Button sits at bottom-left in figure coordinates
        self._ax_status = self._fig.add_axes([0, 0.08, 1, 0.92])
        self._ax_status.set_facecolor(BG)
        self._ax_status.axis("off")

        # Button area — bottom left, small
        ax_btn = self._fig.add_axes([0.02, 0.01, 0.18, 0.06])
        self._btn = Button(ax_btn, "▶  Start Pipeline",
                           color="#1e3a5f", hovercolor="#2563eb")
        self._btn.label.set_color(TEXT)
        self._btn.label.set_fontsize(11)
        self._btn.on_clicked(self._on_start_clicked)

        # Hide button immediately at creation time.
        # It must NOT be visible or clickable during 'Validating...'.
        # It is only made visible inside _do_draw_screen1() after
        # ready_event fires and the real error state is known.
        self._btn.ax.set_visible(False)
        self._fig.canvas.draw_idle()

    def _draw_screen1(self) -> None:
        """
        Render the status screen content.

        Waits for ready_event before reading error_dict — this ensures
        Screen 1 shows the REAL error state from InputModule, not the
        empty initial state that exists before Input runs validation.
        The wait is done on a background thread so the matplotlib window
        stays responsive (not frozen) while waiting.
        """
        # Show a "Validating..." message while waiting for Input to finish
        ax = self._ax_status
        ax.cla()
        ax.set_facecolor(BG)
        ax.axis("off")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.text(0.5, 0.55,
                "Phase 3 — Generic Concurrent Real-Time Pipeline",
                ha="center", va="center",
                color=BLUE, fontsize=16, fontweight="bold")
        ax.text(0.5, 0.45,
                "Validating dataset and configuration...",
                ha="center", va="center",
                color=DIM, fontsize=11)
        self._fig.canvas.draw_idle()

        # Wait in a background thread — keeps window responsive
        import threading
        def _wait_then_draw():
            if self._ready_event is not None:
                self._ready_event.wait()   # blocks until run_input sets it
            # Schedule the real draw on the main thread
            self._fig.canvas.flush_events()
            self._do_draw_screen1()

        threading.Thread(target=_wait_then_draw, daemon=True).start()

    def _do_draw_screen1(self) -> None:
        """Draw Screen 1 content after error_dict is populated."""
        ax = self._ax_status
        ax.cla()
        ax.set_facecolor(BG)
        ax.axis("off")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)

        fatal, skipped = self._get_errors()

        # Title
        ax.text(0.5, 0.93,
                "Phase 3 — Generic Concurrent Real-Time Pipeline",
                ha="center", va="top",
                color=BLUE, fontsize=16, fontweight="bold")

        ax.text(0.5, 0.86,
                "Pipeline Status",
                ha="center", va="top",
                color=DIM, fontsize=11)

        # Divider line
        ax.axhline(0.83, color=BORDER, linewidth=1.0, xmin=0.05, xmax=0.95)

        y = 0.78

        if not fatal and not skipped:
            # All clear
            ax.text(0.5, y, "✔  No errors or warnings detected.",
                    ha="center", va="top",
                    color=BAR_GREEN, fontsize=13)
            ax.text(0.5, y - 0.08,
                    "All configuration checks passed.",
                    ha="center", va="top",
                    color=DIM, fontsize=10)
            ax.text(0.5, y - 0.18,
                    "Click  ▶ Start Pipeline  to begin processing.",
                    ha="center", va="top",
                    color=TEXT, fontsize=10)
            # Button is shown — pipeline can start
            self._btn.ax.set_visible(True)

        elif fatal:
            # FATAL errors — button is hidden, pipeline cannot start
            ax.text(0.5, y,
                    f"✗  {len(fatal)} FATAL error(s) — pipeline cannot start.",
                    ha="center", va="top",
                    color=BAR_RED, fontsize=13, fontweight="bold")
            y -= 0.10
            for err in fatal:
                # Wrap long lines
                max_chars = 90
                lines = [err[i:i+max_chars] for i in range(0, len(err), max_chars)]
                for line in lines:
                    ax.text(0.08, y, line,
                            va="top", color=BAR_RED,
                            fontsize=9, fontfamily="monospace")
                    y -= 0.07

            ax.text(0.5, 0.12,
                    "Close this window. Fix the error(s) above, then re-run.",
                    ha="center", va="top",
                    color=DIM, fontsize=9)
            # Hide button — fatal errors mean no pipeline
            self._btn.ax.set_visible(False)

        else:
            # Skipped rows — non-fatal, button is shown
            ax.text(0.5, y,
                    f"⚠  {len(skipped)} row(s) skipped due to type-cast failures.",
                    ha="center", va="top",
                    color=AMBER, fontsize=13)
            y -= 0.09

            ax.text(0.5, y,
                    "Pipeline will continue — affected rows are excluded.",
                    ha="center", va="top",
                    color=DIM, fontsize=10)
            y -= 0.09

            # Show up to 6 skipped rows as a table
            headers = ["Row", "Column", "Raw Value", "Reason"]
            col_x   = [0.08, 0.20, 0.40, 0.58]
            for hdr, cx in zip(headers, col_x):
                ax.text(cx, y, hdr, va="top",
                        color=DIM, fontsize=9, fontweight="bold")
            y -= 0.06

            ax.axhline(y + 0.01, color=BORDER, linewidth=0.8,
                       xmin=0.06, xmax=0.95)
            y -= 0.01

            for skip in skipped[:6]:
                vals = [
                    str(skip.get("row_index", "")),
                    str(skip.get("column",    "")),
                    str(skip.get("raw_value", ""))[:18],
                    str(skip.get("reason",    ""))[:40],
                ]
                for val, cx in zip(vals, col_x):
                    ax.text(cx, y, val, va="top",
                            color=AMBER, fontsize=8,
                            fontfamily="monospace")
                y -= 0.06

            if len(skipped) > 6:
                ax.text(0.08, y,
                        f"... and {len(skipped) - 6} more skipped row(s).",
                        va="top", color=DIM, fontsize=8)

            ax.text(0.5, 0.12,
                    "Click  ▶ Start Pipeline  to begin processing.",
                    ha="center", va="top",
                    color=TEXT, fontsize=10)
            self._btn.ax.set_visible(True)

        self._fig.canvas.draw_idle()

    # ─────────────────────────────────────────────────────────
    #  Button callback — switch to Screen 2
    # ─────────────────────────────────────────────────────────

    def _on_start_clicked(self, event) -> None:
        """
        Called when the user clicks ▶ Start Pipeline.
        Hides Screen 1, builds Screen 2, starts the animation.
        Processing begins here — not before.
        """
        self._pipeline_started = True

        # Unblock Input + Aggregator
        if self._start_event is not None:
            self._start_event.set()

        # Disconnect button mouse events BEFORE removing axes.
        # Without this, matplotlib's internal hover handler fires
        # after removal and raises AttributeError: NoneType.canvas.
        try:
            self._btn.disconnect_events()
        except Exception:
            pass

        # Remove Screen 1 elements
        self._ax_status.remove()
        self._btn.ax.remove()

        # Build Screen 2 layout on the same figure
        self._build_screen2()

        # Start FuncAnimation — this drives live updates
        self._animation = animation.FuncAnimation(
            self._fig,
            self._animate,
            interval=150,
            blit=False,
            cache_frame_data=False,
        )

        self._fig.canvas.draw_idle()

    # ─────────────────────────────────────────────────────────
    #  Screen 2 — Live charts + telemetry
    # ─────────────────────────────────────────────────────────

    def _build_screen2(self) -> None:
        """
        Build the live dashboard layout on the existing figure.

        Layout strategy — automatically adapts to how many charts config has:
          1 or 2 charts → single column, stacked vertically
          3 or 4 charts → 2×2 grid (2 columns, up to 2 rows)
          5+ charts     → 2 columns, as many rows as needed

        Telemetry bar always spans the full width at the bottom.

        This means 4 charts + telemetry all fit on one (10,7) screen
        without any panel being too small to read.
        """
        self._fig.set_facecolor(BG)
        self._fig.clear()

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

        n_charts = len(self._charts_cfg)

        # ── Decide column count ───────────────────────────────
        # 1-2 charts → 1 column (vertical stack, tall panels)
        # 3+ charts  → 2 columns (grid layout)
        n_cols = 1 if n_charts <= 2 else 2
        n_rows = -(-n_charts // n_cols)   # ceiling division

        # ── Resize figure height for this chart count ─────────
        # More chart rows = taller figure so every panel stays readable.
        # Width stays fixed; only height grows.
        self._fig.set_size_inches(FIGURE_WIDTH, _figure_height(n_rows))

        # ── Build GridSpec ────────────────────────────────────
        # Chart rows get height ratio 3, telemetry row gets ratio 2.
        height_ratios = [3] * n_rows + [2]
        total_rows    = n_rows + 1

        gs = gridspec.GridSpec(
            total_rows, n_cols,
            figure=self._fig,
            height_ratios=height_ratios,
            hspace=0.55,
            wspace=0.35,
            left=0.07, right=0.97,
            top=0.95,  bottom=0.06,
        )

        self._axes     = []
        self._lines    = []
        self._scatters = []

        for i, buf in enumerate(self._chart_buffers):
            cfg   = buf["cfg"]
            color = CHART_COLORS[i % len(CHART_COLORS)]

            # Place chart in correct grid cell
            row = i // n_cols
            col = i  % n_cols
            ax  = self._fig.add_subplot(gs[row, col])

            ax.set_facecolor(PANEL)
            ax.set_title(cfg["title"],   color=color, pad=8)
            ax.set_xlabel(cfg["x_axis"], color=DIM,   labelpad=4)
            ax.set_ylabel(cfg["y_axis"], color=DIM,   labelpad=4)
            ax.grid(True, alpha=0.35)
            ax.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=6))
            ax.set_xticks([])
            ax.set_yticks([])

            line, = ax.plot([], [], color=color, linewidth=1.8,
                            alpha=0.9, label=cfg["title"])
            scatter = ax.scatter([], [], color=CYAN, s=20, zorder=5, alpha=0.8)
            ax.legend(loc="upper left", fontsize=8,
                      facecolor=PANEL, edgecolor=BORDER)

            self._axes.append(ax)
            self._lines.append(line)
            self._scatters.append(scatter)

        # ── Telemetry — always spans ALL columns at the bottom ────────────
        self._ax_tel = self._fig.add_subplot(gs[n_rows, :])
        self._ax_tel.set_facecolor(BG)
        self._ax_tel.axis("off")

    # ─────────────────────────────────────────────────────────
    #  Animation callback (Screen 2 only)
    # ─────────────────────────────────────────────────────────

    def _animate(self, frame: int):
        """
        Called by FuncAnimation every 150ms (Screen 2 only).
        1. Drain up to 25 packets from processed_queue
        2. Update each chart
        3. Redraw telemetry bars
        """
        # ── Step 1: consume packets ───────────────────────────
        for _ in range(25):
            try:
                packet = self._processed_queue.get_nowait()
            except Exception:
                break

            if packet is None:
                self._stream_done = True
                self._mark_complete()
                break

            for buf in self._chart_buffers:
                cfg        = buf["cfg"]
                chart_type = cfg["type"]
                x_key      = cfg["x_axis"]
                y_key      = _Y_KEY.get(chart_type, "metric_value")

                if x_key in packet and y_key in packet:
                    buf["x"].append(packet[x_key])
                    buf["y"].append(packet[y_key])

            self._packets_received += 1

        # ── Step 2: update charts ─────────────────────────────
        for i, buf in enumerate(self._chart_buffers):
            if not buf["x"]:
                continue

            xs = list(buf["x"])
            ys = list(buf["y"])
            ax = self._axes[i]
            chart_type = buf["cfg"]["type"]

            if "bar_chart" in chart_type:
                ax.cla()
                ax.set_facecolor(PANEL)
                ax.set_title(buf["cfg"]["title"],
                             color=CHART_COLORS[i % len(CHART_COLORS)], pad=8)
                ax.set_xlabel(buf["cfg"]["x_axis"], color=DIM, labelpad=4)
                ax.set_ylabel(buf["cfg"]["y_axis"], color=DIM, labelpad=4)
                ax.grid(True, alpha=0.35, axis="y")
                ax.bar(xs, ys,
                       color=CHART_COLORS[i % len(CHART_COLORS)],
                       alpha=0.75, width=0.8)

            elif "scatter" in chart_type:
                self._lines[i].set_data([], [])
                self._scatters[i].set_offsets(list(zip(xs, ys)))
                ax.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=6))
                ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
                # ax.relim() does NOT include scatter artists.
                # Set limits manually from the data instead.
                x_pad = (max(xs) - min(xs)) * 0.05 or 1
                y_pad = (max(ys) - min(ys)) * 0.05 or 1
                ax.set_xlim(min(xs) - x_pad, max(xs) + x_pad)
                ax.set_ylim(min(ys) - y_pad, max(ys) + y_pad)

            else:
                # Line chart
                self._lines[i].set_data(xs, ys)
                self._scatters[i].set_offsets([[xs[-1], ys[-1]]])
                ax.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=6))
                ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
                ax.relim()
                ax.autoscale_view()

        # ── Step 3: telemetry bars ────────────────────────────
        self._draw_telemetry()

        return self._lines

    # ─────────────────────────────────────────────────────────
    #  Telemetry bar renderer (Screen 2)
    # ─────────────────────────────────────────────────────────

    def _draw_telemetry(self) -> None:
        ax = self._ax_tel
        ax.cla()
        ax.set_facecolor(BG)
        ax.axis("off")
        ax.set_xlim(0, 1)
        ax.set_ylim(-0.1, 1)
        ax.set_title(
            f"Pipeline Telemetry — Queue Health"
            f"   |   Packets processed: {self._packets_received}",
            color=DIM, fontsize=9, pad=4,
        )

        with self._tel_lock:
            state = dict(self._tel_state)

        if not state:
            ax.text(0.5, 0.5, "Waiting for telemetry...",
                    ha="center", va="center", color=DIM, fontsize=9)
            return

        bars_to_show = []
        if self._tel_cfg.get("show_raw_stream", True) and "q1" in state:
            bars_to_show.append(("q1", state["q1"]))
        if self._tel_cfg.get("show_intermediate_stream", True) and "q2" in state:
            bars_to_show.append(("q2", state["q2"]))
        if self._tel_cfg.get("show_processed_stream", True) and "q3" in state:
            bars_to_show.append(("q3", state["q3"]))

        n = len(bars_to_show)
        if n == 0:
            return

        spacing    = 1.0 / (n + 1)
        y_slots    = [spacing * (n - i) for i in range(n)]
        bar_left   = 0.22
        bar_right  = 0.76
        bar_width  = bar_right - bar_left
        bar_height = min(0.18, spacing * 0.5)

        for (key, q), y in zip(bars_to_show, y_slots):
            fill  = q["fill_ratio"]
            color = QUEUE_BAR_COLORS.get(q["color"], BAR_GREEN)
            pct   = int(fill * 100)

            ax.barh(y, bar_width, left=bar_left, height=bar_height,
                    color=PANEL, edgecolor=BORDER, linewidth=0.8)

            if fill > 0:
                ax.barh(y, bar_width * fill, left=bar_left,
                        height=bar_height, color=color,
                        alpha=0.85, edgecolor="none")

            ax.text(bar_left - 0.01, y, q["label"],
                    ha="right", va="center",
                    color=TEXT, fontsize=7.5, fontfamily="monospace")

            ax.text(bar_right + 0.01, y, f"{q['size']}/{q['max']}",
                    ha="left", va="center",
                    color=color, fontsize=8.5,
                    fontweight="bold", fontfamily="monospace")

            if fill > 0.12:
                ax.text(bar_left + (bar_width * fill) / 2, y, f"{pct}%",
                        ha="center", va="center",
                        color="white", fontsize=8, fontweight="bold")

        for label, color, xpos in [
            ("● Flowing",      BAR_GREEN,  0.28),
            ("● Filling",      BAR_YELLOW, 0.50),
            ("● Backpressure", BAR_RED,    0.72),
        ]:
            ax.text(xpos, -0.08, label, ha="center", va="bottom",
                    color=color, fontsize=7.5)

    # ─────────────────────────────────────────────────────────
    #  Complete marker
    # ─────────────────────────────────────────────────────────

    def _mark_complete(self) -> None:
        for i, buf in enumerate(self._chart_buffers):
            if i < len(self._axes):
                self._axes[i].set_title(
                    buf["cfg"]["title"] + "  ✔ Complete",
                    color=GREEN, pad=8,
                )

    # ─────────────────────────────────────────────────────────
    #  Entry point
    # ─────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        Entry point — called inside a multiprocessing.Process by main.py.

        1. Initialises matplotlib (deferred import — only runs here)
        2. Builds Screen 1 (status/error screen)
        3. Draws the current error state
        4. Calls plt.show() which blocks until user closes the window
           (FuncAnimation is only started after button click)
        """
        _init_matplotlib()   # import matplotlib only in OutputProcess
        print(f"[Output] Dashboard starting (backend: {_BACKEND}) ...")
        self._build_screen1()
        self._draw_screen1()
        plt.show()
        print("[Output] Dashboard closed.")