"""
output_module.py — Generic Real-Time Dashboard (Dash/Plotly)
=============================================================
Consumes processed packets from Queue 3 and renders a live
browser-based dashboard driven entirely by config.json.

This module is completely domain-agnostic:
  - Chart titles, axis labels, and fields to plot come from
    config["visualizations"]["data_charts"]
  - Telemetry bars are driven by config["visualizations"]["telemetry"]
  - No hardcoded references to sensors, GDP, or any specific domain

Implements TelemetryObserver (from telemetry.py):
  - Subscribes to PipelineTelemetry subject
  - Receives queue state updates via update() method
  - Renders color-coded progress bars for backpressure visualization

Dashboard layout (browser, vertical stack):
  ┌──────────────────────────────────────────┐
  │  Title + Packet Counter                  │
  ├──────────────────────────────────────────┤
  │  [Chart 1] Live metric_value line graph  │
  ├──────────────────────────────────────────┤
  │  [Chart 2] Running average line graph    │
  ├──────────────────────────────────────────┤
  │  [Telemetry] Queue health bars           │
  └──────────────────────────────────────────┘
"""

import time
import socket
import threading
import traceback
import webbrowser
from collections import deque

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go

from plugins.telemetry import TelemetryObserver


# ─────────────────────────────────────────────────────────────
#  Color constants
# ─────────────────────────────────────────────────────────────

COLORS = {
    "background":   "#0f1117",
    "panel":        "#1a1d27",
    "text_primary": "#e2e8f0",
    "text_dim":     "#64748b",
    "accent_blue":  "#3b82f6",
    "accent_cyan":  "#06b6d4",
    "accent_green": "#10b981",
    "green":        "#22c55e",
    "yellow":       "#f59e0b",
    "red":          "#ef4444",
}

QUEUE_COLORS = {
    "green":  COLORS["green"],
    "yellow": COLORS["yellow"],
    "red":    COLORS["red"],
}


# ─────────────────────────────────────────────────────────────
#  OutputModule — Dashboard + TelemetryObserver
# ─────────────────────────────────────────────────────────────

class OutputModule(TelemetryObserver):
    """
    Live real-time browser dashboard — Consumer of Queue 3.

    Inherits TelemetryObserver so it can be subscribed to
    PipelineTelemetry without violating DIP. The Subject
    (PipelineTelemetry) only ever calls update() and never
    imports or knows about OutputModule.

    Uses Dash (Plotly) for browser-based real-time charts.
    Auto-opens the dashboard in the default browser.

    Multiprocessing role:
        Runs as a separate multiprocessing.Process started by main.py.
        The telemetry thread also runs inside this same process.
    """

    def __init__(self, config: dict, processed_queue):
        """
        Args:
            config          : full config.json dict
            processed_queue : Queue 3 — reads completed packets from here
        """
        self.processed_queue = processed_queue
        self.viz_config      = config["visualizations"]
        self.charts_config   = self.viz_config["data_charts"]

        # ── Chart config lookups ──────────────────────────────
        self._values_chart = self._find_chart("real_time_line_graph_values")
        self._avg_chart    = self._find_chart("real_time_line_graph_average")

        # ── Data buffers (scrolling windows) ─────────────────
        self.x_data = deque(maxlen=200)
        self.y_data = deque(maxlen=200)
        self.avg_x  = deque(maxlen=200)
        self.avg_y  = deque(maxlen=200)

        # ── Stats ─────────────────────────────────────────────
        self.packets_received = 0
        self.stream_done      = False

        # ── Telemetry state — updated by Observer.update() ───
        # Protected by a lock: telemetry writes from its thread,
        # Dash callback reads from the server thread
        self._tel_lock  = threading.Lock()
        self._tel_state = {}

    # ── Helpers ───────────────────────────────────────────────

    def _find_chart(self, chart_type: str) -> dict:
        """Return the chart config dict for a given type, or a safe default."""
        for chart in self.charts_config:
            if chart["type"] == chart_type:
                return chart
        return {"title": chart_type, "x_axis": "time_period", "y_axis": "metric_value"}

    def _find_free_port(self, start: int = 8050) -> int:
        """Find the first available TCP port starting from start."""
        for port in range(start, start + 10):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex(("127.0.0.1", port)) != 0:
                    return port
        return start

    def _open_browser(self, url: str) -> None:
        """Open the dashboard URL in the default browser after a short delay."""
        time.sleep(1.5)
        webbrowser.open(url)

    # ── TelemetryObserver interface ───────────────────────────

    def update(self, telemetry_state: dict) -> None:
        """
        Called by PipelineTelemetry (Subject) on every poll cycle.
        Stores state thread-safely for the Dash callback to render.
        """
        with self._tel_lock:
            self._tel_state = telemetry_state

    # ── Telemetry HTML builder ────────────────────────────────

    def _build_telemetry_html(self) -> html.Div:
        """
        Builds Dash HTML components for the three queue health bars.
        Color-coded green/yellow/red based on fill ratio from telemetry state.
        """
        with self._tel_lock:
            state = dict(self._tel_state)

        if not state:
            return html.Div(
                "Telemetry starting...",
                style={"color": COLORS["text_dim"], "textAlign": "center", "padding": "30px"}
            )

        bars = []
        for key in ["q1", "q2", "q3"]:
            if key not in state:
                continue

            q     = state[key]
            color = QUEUE_COLORS.get(q["color"], COLORS["green"])
            pct   = int(q["fill_ratio"] * 100)

            bars.append(html.Div([
                # Stream label
                html.Div(
                    q["label"],
                    style={
                        "fontWeight":   "bold",
                        "marginBottom": "8px",
                        "color":        COLORS["text_primary"],
                    }
                ),
                # Progress bar track
                html.Div(
                    html.Div(style={
                        "width":           f"{pct}%",
                        "height":          "28px",
                        "backgroundColor": color,
                        "borderRadius":    "8px",
                        "transition":      "width 0.4s ease",
                        "minWidth":        "4px",
                    }),
                    style={
                        "backgroundColor": COLORS["panel"],
                        "borderRadius":    "8px",
                        "overflow":        "hidden",
                        "marginBottom":    "10px",
                        "border":          f"1px solid {COLORS['text_dim']}",
                    }
                ),
                # Size / percent label
                html.Div(
                    f"{q['size']}/{q['max']} items  •  {pct}%",
                    style={
                        "fontSize":     "14px",
                        "color":        color,
                        "fontWeight":   "bold",
                        "marginBottom": "22px",
                    }
                ),
            ]))

        return html.Div(bars, style={"padding": "0 20px"})

    # ── Entry point ───────────────────────────────────────────

    def run(self) -> None:
        """
        Entry point — called inside a multiprocessing.Process by main.py.

        Builds the Dash app layout, registers the update callback,
        opens the browser, and starts the Dash server (blocking).
        """
        print("[Output] Starting browser dashboard...")
        port = self._find_free_port(8050)
        url  = f"http://127.0.0.1:{port}"
        print(f"[Output] Dashboard live at {url}")

        # Open browser in background thread so server starts first
        threading.Thread(target=self._open_browser, args=(url,), daemon=True).start()

        app = dash.Dash(__name__, title="Phase 3 — Real-Time Pipeline Dashboard")

        # ── Layout ────────────────────────────────────────────
        app.layout = html.Div(
            style={
                "backgroundColor": COLORS["background"],
                "color":           COLORS["text_primary"],
                "fontFamily":      "monospace",
                "padding":         "30px",
                "minHeight":       "100vh",
            },
            children=[
                # Title
                html.H1(
                    "REAL-TIME PIPELINE DASHBOARD",
                    style={
                        "textAlign":    "center",
                        "color":        COLORS["accent_blue"],
                        "marginBottom": "8px",
                    }
                ),

                # Live packet counter
                html.Div(
                    id="packet-counter",
                    style={
                        "textAlign":    "center",
                        "fontSize":     "18px",
                        "marginBottom": "25px",
                        "color":        COLORS["accent_cyan"],
                    }
                ),

                # Chart 1 — Live sensor values
                dcc.Graph(
                    id="live-values",
                    style={"height": "40vh", "marginBottom": "30px"}
                ),

                # Chart 2 — Running average
                dcc.Graph(
                    id="live-average",
                    style={"height": "40vh", "marginBottom": "40px"}
                ),

                # Telemetry section header
                html.H3(
                    "Pipeline Telemetry — Queue Health",
                    style={
                        "color":        COLORS["text_dim"],
                        "marginBottom": "20px",
                        "textAlign":    "center",
                    }
                ),

                # Telemetry bars (updated by callback)
                html.Div(id="telemetry-section"),

                # Auto-refresh interval (every 80ms)
                dcc.Interval(id="interval", interval=80, n_intervals=0),
            ]
        )

        # ── Callback ──────────────────────────────────────────
        @app.callback(
            [
                Output("live-values",       "figure"),
                Output("live-average",      "figure"),
                Output("telemetry-section", "children"),
                Output("packet-counter",    "children"),
            ],
            Input("interval", "n_intervals")
        )
        def update(_):
            # Drain up to 30 packets per tick
            for _ in range(30):
                try:
                    packet = self.processed_queue.get_nowait()
                except Exception:
                    break

                if packet is None:
                    self.stream_done = True
                    break

                x   = packet.get(self._values_chart["x_axis"])
                y   = packet.get(self._values_chart["y_axis"])
                avg = packet.get(self._avg_chart["y_axis"])

                if x is not None and y is not None:
                    self.x_data.append(x)
                    self.y_data.append(y)
                if x is not None and avg is not None:
                    self.avg_x.append(x)
                    self.avg_y.append(avg)

                self.packets_received += 1

            # Build Chart 1
            fig_values = go.Figure(
                go.Scatter(
                    x=list(self.x_data),
                    y=list(self.y_data),
                    mode="lines+markers",
                    name="Verified Value",
                    line=dict(color=COLORS["accent_blue"], width=2.5),
                    marker=dict(size=6, color=COLORS["accent_cyan"]),
                )
            )
            fig_values.update_layout(
                title=self._values_chart["title"],
                xaxis_title=self._values_chart["x_axis"],
                yaxis_title=self._values_chart["y_axis"],
                template="plotly_dark",
                plot_bgcolor=COLORS["panel"],
                paper_bgcolor=COLORS["background"],
                margin=dict(l=60, r=20, t=50, b=50),
            )

            # Build Chart 2
            fig_avg = go.Figure(
                go.Scatter(
                    x=list(self.avg_x),
                    y=list(self.avg_y),
                    mode="lines+markers",
                    name="Running Average",
                    line=dict(color=COLORS["accent_green"], width=3.0),
                    marker=dict(size=5, color=COLORS["accent_green"]),
                )
            )
            fig_avg.update_layout(
                title=self._avg_chart["title"],
                xaxis_title=self._avg_chart["x_axis"],
                yaxis_title=self._avg_chart["y_axis"],
                template="plotly_dark",
                plot_bgcolor=COLORS["panel"],
                paper_bgcolor=COLORS["background"],
                margin=dict(l=60, r=20, t=50, b=50),
            )

            # Counter
            counter = f"Packets Processed: {self.packets_received}"
            if self.stream_done:
                counter += "  |  Stream Complete"

            return fig_values, fig_avg, self._build_telemetry_html(), counter

        # Start Dash server (blocking call)
        try:
            app.run(debug=False, port=port, use_reloader=False)
        except Exception as e:
            print(f"[Output] ERROR: {e}")
            traceback.print_exc()

        print("[Output] Dashboard closed.")