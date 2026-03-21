"""
validate_config.py
------------------
Validates the configuration dictionary produced by read_config.py.

Usage:
    from config.validate_config import validate_config

    errors = validate_config(config_dict)
    if errors:
        # show errors via Streamlit and terminate
    else:
        # proceed with pipeline
"""

from typing import Any


# ---------------------------------------------------------------------------
# Supported chart type whitelist
#
# Naming convention: real_time_{chart_kind}_{data_series}
#   chart_kind  : line_graph | bar_chart | scatter
#   data_series : values (raw metric_value) | average (computed_metric)
#
# The user MUST write the exact string from this set in config.json.
# Any other string is rejected at validation time with a clear error
# listing all valid options — before the pipeline even starts.
#
# To add a new chart type in future:
#   1. Add the string here
#   2. Add a renderer entry in output_module.py
#   Nothing else needs to change anywhere.
# ---------------------------------------------------------------------------

SUPPORTED_CHART_TYPES = {
    "real_time_line_graph_values",
    "real_time_line_graph_average",
    "real_time_bar_chart_values",
    "real_time_bar_chart_average",
    "real_time_scatter_values",
    "real_time_scatter_average",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_top_level_keys(cfg: dict, errors: list[str]) -> None:
    """Check that all five mandatory top-level keys are present."""
    required_top_keys = [
        "dataset_path",
        "pipeline_dynamics",
        "schema_mapping",
        "processing",
        "visualizations",
    ]
    for key in required_top_keys:
        if key not in cfg:
            errors.append(f"[TOP-LEVEL] Missing required key: '{key}'")


def _check_dataset_path(cfg: dict, errors: list[str]) -> None:
    """dataset_path must be a non-empty string."""
    if "dataset_path" not in cfg:
        return  # already caught above
    val = cfg["dataset_path"]
    if not isinstance(val, str) or not val.strip():
        errors.append(
            "[dataset_path] Value must be a non-empty string. "
            f"Got: {repr(val)}"
        )


def _check_pipeline_dynamics(cfg: dict, errors: list[str]) -> None:
    """
    pipeline_dynamics must contain:
        input_delay_seconds   – float / int  (>= 0)
        core_parallelism      – int          (>= 1)
        stream_queue_max_size – int          (>= 1)
        telemetry_poll_interval – float / int (>= 0)
    """
    if "pipeline_dynamics" not in cfg:
        return  # already caught above

    pd_cfg = cfg["pipeline_dynamics"]

    if not isinstance(pd_cfg, dict):
        errors.append("[pipeline_dynamics] Must be a JSON object (dict).")
        return

    required = {
        "input_delay_seconds": (float, int),
        "core_parallelism": (int,),
        "stream_queue_max_size": (int,),
        "telemetry_poll_interval": (float, int),
    }

    for key, expected_types in required.items():
        if key not in pd_cfg:
            errors.append(f"[pipeline_dynamics] Missing required key: '{key}'")
            continue

        val = pd_cfg[key]
        # booleans are a subclass of int in Python – reject them explicitly
        if isinstance(val, bool) or not isinstance(val, expected_types):
            errors.append(
                f"[pipeline_dynamics] '{key}' must be of type "
                f"{' or '.join(t.__name__ for t in expected_types)}. "
                f"Got: {repr(val)}"
            )
            continue

        # range checks
        if key in ("core_parallelism", "stream_queue_max_size") and val < 1:
            errors.append(
                f"[pipeline_dynamics] '{key}' must be >= 1. Got: {val}"
            )
        elif key in ("input_delay_seconds", "telemetry_poll_interval") and val < 0:
            errors.append(
                f"[pipeline_dynamics] '{key}' must be >= 0. Got: {val}"
            )


def _check_schema_mapping(cfg: dict, errors: list[str]) -> None:
    """
    schema_mapping must contain:
        columns – non-empty list, each item having:
            source_name       – non-empty string
            internal_mapping  – non-empty string
            data_type         – one of 'string', 'integer', 'float'
    """
    if "schema_mapping" not in cfg:
        return

    sm_cfg = cfg["schema_mapping"]

    if not isinstance(sm_cfg, dict):
        errors.append("[schema_mapping] Must be a JSON object (dict).")
        return

    if "columns" not in sm_cfg:
        errors.append("[schema_mapping] Missing required key: 'columns'")
        return

    columns = sm_cfg["columns"]

    if not isinstance(columns, list) or len(columns) == 0:
        errors.append("[schema_mapping] 'columns' must be a non-empty list.")
        return

    allowed_data_types = {"string", "integer", "float"}
    required_col_keys = ["source_name", "internal_mapping", "data_type"]

    for idx, col in enumerate(columns):
        prefix = f"[schema_mapping.columns[{idx}]]"

        if not isinstance(col, dict):
            errors.append(f"{prefix} Each column entry must be a JSON object.")
            continue

        for key in required_col_keys:
            if key not in col:
                errors.append(f"{prefix} Missing required key: '{key}'")
                continue

            val = col[key]
            if not isinstance(val, str) or not val.strip():
                errors.append(
                    f"{prefix} '{key}' must be a non-empty string. Got: {repr(val)}"
                )
            elif key == "data_type" and val not in allowed_data_types:
                errors.append(
                    f"{prefix} 'data_type' must be one of "
                    f"{sorted(allowed_data_types)}. Got: {repr(val)}"
                )


def _check_processing(cfg: dict, errors: list[str]) -> None:
    """
    processing must contain:
        stateless_tasks:
            operation   – non-empty string
            algorithm   – non-empty string
            iterations  – int (>= 1)
            secret_key  – non-empty string
        stateful_tasks:
            operation                  – non-empty string
            running_average_window_size – int (>= 1)
    """
    if "processing" not in cfg:
        return

    proc = cfg["processing"]

    if not isinstance(proc, dict):
        errors.append("[processing] Must be a JSON object (dict).")
        return

    # --- stateless_tasks ---
    if "stateless_tasks" not in proc:
        errors.append("[processing] Missing required key: 'stateless_tasks'")
    else:
        st = proc["stateless_tasks"]
        if not isinstance(st, dict):
            errors.append("[processing.stateless_tasks] Must be a JSON object (dict).")
        else:
            _require_nonempty_str(st, "operation",  "[processing.stateless_tasks]", errors)
            _require_nonempty_str(st, "algorithm",  "[processing.stateless_tasks]", errors)
            _require_nonempty_str(st, "secret_key", "[processing.stateless_tasks]", errors)

            if "iterations" not in st:
                errors.append("[processing.stateless_tasks] Missing required key: 'iterations'")
            else:
                val = st["iterations"]
                if isinstance(val, bool) or not isinstance(val, int):
                    errors.append(
                        "[processing.stateless_tasks] 'iterations' must be an integer. "
                        f"Got: {repr(val)}"
                    )
                elif val < 1:
                    errors.append(
                        f"[processing.stateless_tasks] 'iterations' must be >= 1. Got: {val}"
                    )

    # --- stateful_tasks ---
    if "stateful_tasks" not in proc:
        errors.append("[processing] Missing required key: 'stateful_tasks'")
    else:
        sft = proc["stateful_tasks"]
        if not isinstance(sft, dict):
            errors.append("[processing.stateful_tasks] Must be a JSON object (dict).")
        else:
            _require_nonempty_str(sft, "operation", "[processing.stateful_tasks]", errors)

            if "running_average_window_size" not in sft:
                errors.append(
                    "[processing.stateful_tasks] Missing required key: "
                    "'running_average_window_size'"
                )
            else:
                val = sft["running_average_window_size"]
                if isinstance(val, bool) or not isinstance(val, int):
                    errors.append(
                        "[processing.stateful_tasks] 'running_average_window_size' "
                        f"must be an integer. Got: {repr(val)}"
                    )
                elif val < 1:
                    errors.append(
                        "[processing.stateful_tasks] 'running_average_window_size' "
                        f"must be >= 1. Got: {val}"
                    )


def _check_visualizations(cfg: dict, errors: list[str]) -> None:
    """
    visualizations must contain:
        telemetry:
            show_raw_stream          – bool
            show_intermediate_stream – bool
            show_processed_stream    – bool
        data_charts – non-empty list, each item having:
            type   – non-empty string
            title  – non-empty string
            x_axis – non-empty string
            y_axis – non-empty string
    """
    if "visualizations" not in cfg:
        return

    viz = cfg["visualizations"]

    if not isinstance(viz, dict):
        errors.append("[visualizations] Must be a JSON object (dict).")
        return

    # --- telemetry ---
    if "telemetry" not in viz:
        errors.append("[visualizations] Missing required key: 'telemetry'")
    else:
        tel = viz["telemetry"]
        if not isinstance(tel, dict):
            errors.append("[visualizations.telemetry] Must be a JSON object (dict).")
        else:
            for flag in ("show_raw_stream", "show_intermediate_stream", "show_processed_stream"):
                if flag not in tel:
                    errors.append(f"[visualizations.telemetry] Missing required key: '{flag}'")
                elif not isinstance(tel[flag], bool):
                    errors.append(
                        f"[visualizations.telemetry] '{flag}' must be a boolean "
                        f"(true/false). Got: {repr(tel[flag])}"
                    )

    # --- data_charts ---
    if "data_charts" not in viz:
        errors.append("[visualizations] Missing required key: 'data_charts'")
    else:
        charts = viz["data_charts"]
        if not isinstance(charts, list) or len(charts) == 0:
            errors.append("[visualizations] 'data_charts' must be a non-empty list.")
        else:
            required_chart_keys = ["type", "title", "x_axis", "y_axis"]
            for idx, chart in enumerate(charts):
                prefix = f"[visualizations.data_charts[{idx}]]"
                if not isinstance(chart, dict):
                    errors.append(f"{prefix} Each chart entry must be a JSON object.")
                    continue
                for key in required_chart_keys:
                    if key not in chart:
                        errors.append(f"{prefix} Missing required key: '{key}'")
                        continue
                    val = chart[key]
                    if not isinstance(val, str) or not val.strip():
                        errors.append(
                            f"{prefix} '{key}' must be a non-empty string. "
                            f"Got: {repr(val)}"
                        )
                    # Strict whitelist check — only for the 'type' field
                    elif key == "type" and val not in SUPPORTED_CHART_TYPES:
                        errors.append(
                            f"{prefix} 'type' must be one of: "
                            f"{sorted(SUPPORTED_CHART_TYPES)}. "
                            f"Got: {repr(val)}"
                        )


# ---------------------------------------------------------------------------
# Tiny utility
# ---------------------------------------------------------------------------

def _require_nonempty_str(
    obj: dict, key: str, prefix: str, errors: list[str]
) -> None:
    """Append an error if `key` is absent or not a non-empty string in `obj`."""
    if key not in obj:
        errors.append(f"{prefix} Missing required key: '{key}'")
    else:
        val = obj[key]
        if not isinstance(val, str) or not val.strip():
            errors.append(
                f"{prefix} '{key}' must be a non-empty string. Got: {repr(val)}"
            )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_config(cfg: Any) -> list[str]:
    """
    Validate the configuration dictionary loaded from config.json.

    Parameters
    ----------
    cfg : Any
        The object returned by read_config().  Expected to be a dict.

    Returns
    -------
    list[str]
        A (possibly empty) list of human-readable error messages.
        An empty list means the configuration is valid.
    """
    errors: list[str] = []

    if not isinstance(cfg, dict):
        errors.append(
            "[CONFIG] The configuration file must be a JSON object at the top level. "
            f"Got type: {type(cfg).__name__}"
        )
        return errors  # nothing more can be checked

    _check_top_level_keys(cfg, errors)
    _check_dataset_path(cfg, errors)
    _check_pipeline_dynamics(cfg, errors)
    _check_schema_mapping(cfg, errors)
    _check_processing(cfg, errors)
    _check_visualizations(cfg, errors)

    return errors