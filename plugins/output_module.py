"""
output_module.py — Two-Screen Matplotlib Dashboard
====================================================
Screen 1 — Status Screen (shown first, always):
  Shows pipeline status, errors, and warnings.
  ┌──────────────────────────────────────────────┐
  │  Phase 3 — Generic Concurrent Pipeline        │
  │                                              │
  │  [RED]   FATAL errors (config / CSV issues)  │
  │    → No button shown. User closes window.    │
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

Screen size: width=12 inches, height dynamic (6 for Screen 1, up to
8.0 for Screen 2). Fits 12/14/16-inch laptops without overflowing.

All iteration uses functional programming — map(), filter(), reduce(),
zip() with map(), and next(iter(filter())) — instead of for loops.

DIP compliance:
  OutputModule depends only on processed_queue + config + error_dict.
  It never imports InputModule, CoreWorker, or Aggregator.
  It implements TelemetryObserver — Subject never imports it directly.
"""

import threading
import collections
import multiprocessing
from functools import reduce

# ─────────────────────────────────────────────────────────────
#  Backend auto-detection — must happen before pyplot import
# ─────────────────────────────────────────────────────────────

def _set_matplotlib_backend() -> str:
    import matplotlib
    backends = ["TkAgg", "Qt5Agg", "Qt6Agg", "WxAgg", "MacOSX", "Agg"]

    def _try_backend(backend: str) -> str | None:
        """Pure function — tries one backend, returns name on success or None."""
        try:
            matplotlib.use(backend)
            import matplotlib.pyplot as plt
            plt.switch_backend(backend)
            print(f"[Output] Using matplotlib backend: {backend}")
            return backend
        except Exception:
            return None

    # filter() keeps the first successful backend, next() picks it
    result = next(
        filter(lambda b: b is not None, map(_try_backend, backends)),
        None
    )
    if result:
        return result

    matplotlib.use("Agg")
    print("[Output] WARNING — Agg backend (no display window).")
    return "Agg"


# Matplotlib imports are deferred inside run() — NOT at module import time.
# On Windows multiprocessing uses "spawn" which re-imports every module in
# every child process. Deferring to run() means it only runs once, in OutputProcess.

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
# ─────────────────────────────────────────────────────────────

FIGURE_WIDTH = 12   # fixed width in inches — fits 12/14/16-inch laptops

def _figure_height(n_chart_rows: int) -> float:
    """Return figure height in inches based on number of chart rows."""
    return min(2.2 * n_chart_rows + 1.8, 8.5)


def _init_matplotlib():
    """Import matplotlib and select backend. Called once inside run()."""
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
    """

    def __init__(self, config: dict,
                 processed_queue: multiprocessing.Queue,
                 error_dict=None,
                 ready_event=None,
                 start_event=None):
        self._processed_queue = processed_queue
        self._error_dict      = error_dict
        self._ready_event     = ready_event
        self._start_event     = start_event
        self._charts_cfg      = config["visualizations"]["data_charts"]
        self._tel_cfg         = config["visualizations"]["telemetry"]

        # Chart data buffers — map() replaces list comprehension with for
        self._chart_buffers: list[dict] = list(map(
            lambda chart_cfg: {
                "x":   collections.deque(maxlen=MAX_POINTS),
                "y":   collections.deque(maxlen=MAX_POINTS),
                "cfg": chart_cfg,
            },
            self._charts_cfg
        ))

        self._packets_received = 0
        self._stream_done      = False
        self._tel_lock         = threading.Lock()
        self._tel_state        = {}
        self._pipeline_started = False
        self._fig              = None
        self._animation        = None
        self._btn              = None
        self._ax_status        = None
        self._axes             = []
        self._lines            = []
        self._scatters         = []
        self._ax_tel           = None

    # ── TelemetryObserver interface ───────────────────────────

    def update(self, telemetry_state: dict) -> None:
        with self._tel_lock:
            self._tel_state = telemetry_state

    def _get_errors(self):
        if self._error_dict is None:
            return [], []
        return (list(self._error_dict.get("fatal_errors", [])),
                list(self._error_dict.get("skipped_rows",  [])))

    # ─────────────────────────────────────────────────────────
    #  Screen 1 — Status / Error screen
    # ─────────────────────────────────────────────────────────

    def _build_screen1(self) -> None:
        plt.style.use("dark_background")
        plt.rcParams.update({"font.family": "monospace", "figure.facecolor": BG})

        self._fig = plt.figure(figsize=(FIGURE_WIDTH, 6), facecolor=BG)
        self._ax_status = self._fig.add_axes([0, 0.08, 1, 0.92])
        self._ax_status.set_facecolor(BG)
        self._ax_status.axis("off")

        ax_btn = self._fig.add_axes([0.02, 0.01, 0.18, 0.06])
        self._btn = Button(ax_btn, "▶  Start Pipeline",
                           color="#1e3a5f", hovercolor="#2563eb")
        self._btn.label.set_color(TEXT)
        self._btn.label.set_fontsize(11)
        self._btn.on_clicked(self._on_start_clicked)
        self._btn.ax.set_visible(False)
        self._fig.canvas.draw_idle()

    def _draw_screen1(self) -> None:
        ax = self._ax_status
        ax.cla()
        ax.set_facecolor(BG)
        ax.axis("off")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.text(0.5, 0.55, "Phase 3 — Generic Concurrent Real-Time Pipeline",
                ha="center", va="center", color=BLUE, fontsize=16, fontweight="bold")
        ax.text(0.5, 0.45, "Validating dataset and configuration...",
                ha="center", va="center", color=DIM, fontsize=11)
        self._fig.canvas.draw_idle()

        import threading
        def _wait_then_draw():
            if self._ready_event is not None:
                self._ready_event.wait()
            self._fig.canvas.flush_events()
            self._do_draw_screen1()

        threading.Thread(target=_wait_then_draw, daemon=True).start()

    def _do_draw_screen1(self) -> None:
        ax = self._ax_status
        ax.cla()
        ax.set_facecolor(BG)
        ax.axis("off")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)

        fatal, skipped = self._get_errors()

        ax.text(0.5, 0.93, "Phase 3 — Generic Concurrent Real-Time Pipeline",
                ha="center", va="top", color=BLUE, fontsize=16, fontweight="bold")
        ax.text(0.5, 0.86, "Pipeline Status",
                ha="center", va="top", color=DIM, fontsize=11)
        ax.axhline(0.83, color=BORDER, linewidth=1.0, xmin=0.05, xmax=0.95)

        y = 0.78

        if not fatal and not skipped:
            ax.text(0.5, y, "✔  No errors or warnings detected.",
                    ha="center", va="top", color=BAR_GREEN, fontsize=13)
            ax.text(0.5, y - 0.08, "All configuration checks passed.",
                    ha="center", va="top", color=DIM, fontsize=10)
            ax.text(0.5, y - 0.18, "Click  ▶ Start Pipeline  to begin processing.",
                    ha="center", va="top", color=TEXT, fontsize=10)
            self._btn.ax.set_visible(True)

        elif fatal:
            ax.text(0.5, y,
                    f"✗  {len(fatal)} FATAL error(s) — pipeline cannot start.",
                    ha="center", va="top",
                    color=BAR_RED, fontsize=13, fontweight="bold")
            y -= 0.10

            def _draw_one_error(state: tuple) -> tuple:
                """
                Pure function — draws one error message (possibly wrapped).
                Takes (current_y, err_string), returns updated y after drawing.
                """
                current_y, err = state
                max_chars = 90
                # map() wraps the error into lines of max_chars length
                wrapped = list(map(
                    lambda i: err[i: i + max_chars],
                    range(0, len(err), max_chars)
                ))
                def _draw_line(line_state: tuple) -> float:
                    ly, line = line_state
                    ax.text(0.08, ly, line, va="top", color=BAR_RED,
                            fontsize=9, fontfamily="monospace")
                    return ly - 0.07
                # reduce() draws each wrapped line and accumulates y position
                new_y = reduce(_draw_line, wrapped, current_y)  # type: ignore
                # fix: reduce needs (acc, item) signature
                final_y = reduce(
                    lambda ly, line: (
                        ax.text(0.08, ly, line, va="top", color=BAR_RED,
                                fontsize=9, fontfamily="monospace") or ly - 0.07
                    ),
                    wrapped,
                    current_y
                )
                return final_y

            # reduce() threads y through every error, drawing each one
            reduce(
                lambda current_y, err: (
                    ax.text(0.08, current_y,
                            err if len(err) <= 90 else err[:90],
                            va="top", color=BAR_RED,
                            fontsize=9, fontfamily="monospace") or current_y - 0.07
                ),
                fatal,
                y
            )

            ax.text(0.5, 0.12,
                    "Close this window. Fix the error(s) above, then re-run.",
                    ha="center", va="top", color=DIM, fontsize=9)
            self._btn.ax.set_visible(False)

        else:
            ax.text(0.5, y,
                    f"⚠  {len(skipped)} row(s) skipped due to type-cast failures.",
                    ha="center", va="top", color=AMBER, fontsize=13)
            y -= 0.09
            ax.text(0.5, y, "Pipeline will continue — affected rows are excluded.",
                    ha="center", va="top", color=DIM, fontsize=10)
            y -= 0.09

            headers = ["Row", "Column", "Raw Value", "Reason"]
            col_x   = [0.08, 0.20, 0.40, 0.58]

            # map() + zip() draws each header — replaces: for hdr, cx in zip(...)
            list(map(
                lambda hdr_cx: ax.text(hdr_cx[1], y, hdr_cx[0], va="top",
                                       color=DIM, fontsize=9, fontweight="bold"),
                zip(headers, col_x)
            ))
            y -= 0.06
            ax.axhline(y + 0.01, color=BORDER, linewidth=0.8, xmin=0.06, xmax=0.95)
            y -= 0.01

            def _draw_one_skip(state: tuple) -> float:
                """
                Pure function — draws one skipped-row table row.
                Takes current y, returns y after drawing.
                Replaces: for skip in skipped[:6]: for val, cx in zip(vals, col_x)
                """
                current_y, skip = state
                vals = [
                    str(skip.get("row_index", "")),
                    str(skip.get("column",    "")),
                    str(skip.get("raw_value", ""))[:18],
                    str(skip.get("reason",    ""))[:40],
                ]
                # map() draws each cell in the row
                list(map(
                    lambda val_cx: ax.text(val_cx[1], current_y, val_cx[0],
                                           va="top", color=AMBER, fontsize=8,
                                           fontfamily="monospace"),
                    zip(vals, col_x)
                ))
                return current_y - 0.06

            # reduce() threads y through every skipped row
            # map() pairs each skip with the current y — no list comprehension
            y = reduce(_draw_one_skip, list(map(lambda s: (y, s), skipped[:6])), y)

            if len(skipped) > 6:
                ax.text(0.08, y,
                        f"... and {len(skipped) - 6} more skipped row(s).",
                        va="top", color=DIM, fontsize=8)

            ax.text(0.5, 0.12, "Click  ▶ Start Pipeline  to begin processing.",
                    ha="center", va="top", color=TEXT, fontsize=10)
            self._btn.ax.set_visible(True)

        self._fig.canvas.draw_idle()

    # ─────────────────────────────────────────────────────────
    #  Button callback — switch to Screen 2
    # ─────────────────────────────────────────────────────────

    def _on_start_clicked(self, event) -> None:
        self._pipeline_started = True
        if self._start_event is not None:
            self._start_event.set()
        try:
            self._btn.disconnect_events()
        except Exception:
            pass
        self._ax_status.remove()
        self._btn.ax.remove()
        self._build_screen2()
        self._animation = animation.FuncAnimation(
            self._fig, self._animate,
            interval=150, blit=False, cache_frame_data=False,
        )
        self._fig.canvas.draw_idle()

    # ─────────────────────────────────────────────────────────
    #  Screen 2 — Live charts + telemetry
    # ─────────────────────────────────────────────────────────

    def _build_screen2(self) -> None:
        """
        Adaptive grid layout.
        1-2 charts → 1 column. 3+ charts → 2 columns.
        Telemetry always spans full width at bottom.
        map() replaces the for loop that built each chart axes.
        """
        self._fig.set_facecolor(BG)
        self._fig.clear()

        plt.rcParams.update({
            "font.family": "monospace", "font.size": 10,
            "axes.titlesize": 11, "axes.titleweight": "bold",
            "axes.labelsize": 9, "axes.edgecolor": BORDER,
            "axes.linewidth": 1.2, "figure.facecolor": BG,
            "axes.facecolor": PANEL, "grid.color": DIM,
            "grid.linewidth": 0.6, "xtick.color": DIM,
            "ytick.color": DIM, "text.color": TEXT,
        })

        n_charts = len(self._charts_cfg)
        n_cols   = 1 if n_charts <= 2 else 2
        n_rows   = -(-n_charts // n_cols)   # ceiling division

        self._fig.set_size_inches(FIGURE_WIDTH, _figure_height(n_rows))

        height_ratios = [3] * n_rows + [2]
        gs = gridspec.GridSpec(
            n_rows + 1, n_cols,
            figure=self._fig,
            height_ratios=height_ratios,
            hspace=0.55, wspace=0.35,
            left=0.07, right=0.97, top=0.95, bottom=0.06,
        )

        def _build_one_chart(item: tuple) -> tuple:
            """
            Pure function — builds one chart axes panel.
            Returns (ax, line, scatter) tuple.
            Replaces: for i, buf in enumerate(self._chart_buffers)
            """
            i, buf = item
            cfg    = buf["cfg"]
            color  = CHART_COLORS[i % len(CHART_COLORS)]
            row    = i // n_cols
            col    = i  % n_cols
            ax     = self._fig.add_subplot(gs[row, col])

            ax.set_facecolor(PANEL)
            ax.set_title(cfg["title"],   color=color, pad=8)
            ax.set_xlabel(cfg["x_axis"], color=DIM,   labelpad=4)
            ax.set_ylabel(cfg["y_axis"], color=DIM,   labelpad=4)
            ax.grid(True, alpha=0.35)
            ax.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=6))

            line,   = ax.plot([], [], color=color, linewidth=1.8,
                              alpha=0.9, label=cfg["title"])
            scatter  = ax.scatter([], [], color=CYAN, s=20, zorder=5, alpha=0.8)
            ax.legend(loc="upper left", fontsize=8,
                      facecolor=PANEL, edgecolor=BORDER)
            return ax, line, scatter

        # map() builds every chart, zip(*...) unpacks into three lists
        results = list(map(_build_one_chart, enumerate(self._chart_buffers)))
        self._axes, self._lines, self._scatters = (
            list(map(lambda r: r[0], results)),
            list(map(lambda r: r[1], results)),
            list(map(lambda r: r[2], results)),
        )

        self._ax_tel = self._fig.add_subplot(gs[n_rows, :])
        self._ax_tel.set_facecolor(BG)
        self._ax_tel.axis("off")

    # ─────────────────────────────────────────────────────────
    #  Animation callback (Screen 2 only)
    # ─────────────────────────────────────────────────────────

    def _drain_packets(self, n: int) -> None:
        """
        Drain up to n packets from processed_queue using recursion —
        functional replacement for: for _ in range(25): ...
        Stops early if queue is empty or sentinel is received.
        """
        if n == 0 or self._stream_done:
            return
        try:
            packet = self._processed_queue.get_nowait()
        except Exception:
            return   # queue empty — stop draining

        if packet is None:
            self._stream_done = True
            self._mark_complete()
            return

        # map() feeds this packet into every chart buffer
        def _feed_buf(buf: dict) -> None:
            x_key = buf["cfg"]["x_axis"]
            y_key = _Y_KEY.get(buf["cfg"]["type"], "metric_value")
            if x_key in packet and y_key in packet:
                buf["x"].append(packet[x_key])
                buf["y"].append(packet[y_key])

        list(map(_feed_buf, self._chart_buffers))
        self._packets_received += 1
        self._drain_packets(n - 1)   # tail recursion for remaining slots

    def _update_one_chart(self, item: tuple) -> None:
        """
        Pure function — updates one chart panel from its buffer.
        Replaces: for i, buf in enumerate(self._chart_buffers) in _animate.
        """
        i, buf = item
        if not buf["x"]:
            return

        xs         = list(buf["x"])
        ys         = list(buf["y"])
        ax         = self._axes[i]
        chart_type = buf["cfg"]["type"]

        if "bar_chart" in chart_type:
            ax.cla()
            ax.set_facecolor(PANEL)
            done        = self._stream_done
            title_text  = buf["cfg"]["title"] + ("  ✔ Complete" if done else "")
            title_color = GREEN if done else CHART_COLORS[i % len(CHART_COLORS)]
            ax.set_title(title_text, color=title_color, pad=8)
            ax.set_xlabel(buf["cfg"]["x_axis"], color=DIM, labelpad=4)
            ax.set_ylabel(buf["cfg"]["y_axis"], color=DIM, labelpad=4)
            ax.grid(True, alpha=0.35, axis="y")
            positions  = list(range(len(xs)))
            ax.bar(positions, ys,
                   color=CHART_COLORS[i % len(CHART_COLORS)],
                   alpha=0.75, width=0.8)
            tick_step  = max(1, len(xs) // 6)
            ax.set_xticks(positions[::tick_step])
            ax.set_xticklabels(
                list(map(lambda j: str(xs[j])[-4:],
                         range(0, len(xs), tick_step))),
                color=DIM, fontsize=7, rotation=30
            )
            ax.tick_params(axis="y", colors=DIM, labelsize=8)
            ax.set_xlim(-0.5, max(len(xs) - 0.5, 1))

        elif "scatter" in chart_type:
            self._lines[i].set_data([], [])
            self._scatters[i].set_offsets(list(zip(xs, ys)))
            ax.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=6))
            ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
            x_pad = (max(xs) - min(xs)) * 0.05 or 1
            y_pad = (max(ys) - min(ys)) * 0.05 or 1
            ax.set_xlim(min(xs) - x_pad, max(xs) + x_pad)
            ax.set_ylim(min(ys) - y_pad, max(ys) + y_pad)

        else:   # line chart
            self._lines[i].set_data(xs, ys)
            self._scatters[i].set_offsets([[xs[-1], ys[-1]]])
            ax.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=6))
            ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
            ax.relim()
            ax.autoscale_view()

    def _animate(self, frame: int):
        """Called by FuncAnimation every 150ms."""
        self._drain_packets(25)
        # map() updates every chart — replaces: for i, buf in enumerate(...)
        list(map(self._update_one_chart, enumerate(self._chart_buffers)))
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

        # filter() picks only the enabled streams — replaces three if-appends
        all_streams = [
            ("show_raw_stream",          "q1"),
            ("show_intermediate_stream", "q2"),
            ("show_processed_stream",    "q3"),
        ]
        bars_to_show = list(map(
            lambda kq: (kq[1], state[kq[1]]),
            filter(
                lambda kq: self._tel_cfg.get(kq[0], True) and kq[1] in state,
                all_streams
            )
        ))

        n = len(bars_to_show)
        if n == 0:
            return

        spacing    = 1.0 / (n + 1)
        bar_left   = 0.22
        bar_right  = 0.76
        bar_width  = bar_right - bar_left
        bar_height = min(0.18, spacing * 0.5)

        # y_slots computed with map() — replaces list comprehension with range
        y_slots = list(map(lambda i: spacing * (n - i), range(n)))

        def _draw_one_bar(bar_item: tuple) -> None:
            """
            Pure function — draws one telemetry bar.
            Replaces: for (key, q), y in zip(bars_to_show, y_slots)
            """
            (key, q), y = bar_item
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

        # map() draws every bar — replaces: for (key, q), y in zip(...)
        list(map(_draw_one_bar, zip(bars_to_show, y_slots)))

        # Legend — map() replaces: for label, color, xpos in [...]
        legend_items = [
            ("● Flowing",      BAR_GREEN,  0.28),
            ("● Filling",      BAR_YELLOW, 0.50),
            ("● Backpressure", BAR_RED,    0.72),
        ]
        list(map(
            lambda item: ax.text(item[2], -0.08, item[0],
                                 ha="center", va="bottom",
                                 color=item[1], fontsize=7.5),
            legend_items
        ))

    # ─────────────────────────────────────────────────────────
    #  Complete marker
    # ─────────────────────────────────────────────────────────

    def _mark_complete(self) -> None:
        """
        Update all chart titles when stream finishes.
        map() replaces: for i, buf in enumerate(self._chart_buffers)
        """
        def _mark_one(item: tuple) -> None:
            i, buf = item
            if i < len(self._axes):
                self._axes[i].set_title(
                    buf["cfg"]["title"] + "  ✔ Complete",
                    color=GREEN, pad=8,
                )
        list(map(_mark_one, enumerate(self._chart_buffers)))

    # ─────────────────────────────────────────────────────────
    #  Entry point
    # ─────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        Entry point — called inside a multiprocessing.Process by main.py.
        1. Initialises matplotlib (deferred — only runs in OutputProcess)
        2. Builds Screen 1, draws validating state, calls plt.show()
        """
        _init_matplotlib()
        print(f"[Output] Dashboard starting (backend: {_BACKEND}) ...")
        self._build_screen1()
        self._draw_screen1()
        plt.show()
        print("[Output] Dashboard closed.")