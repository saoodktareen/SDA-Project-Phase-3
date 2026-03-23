"""
validate_config.py
------------------
Validates the configuration dictionary produced by read_config.py.

Usage:
    from config.validate_config import validate_config

    errors = validate_config(config_dict)
    if errors:
        # show errors on dashboard and terminate
    else:
        # proceed with pipeline

All validation uses functional programming — map(), filter(), reduce(),
and list comprehensions — instead of imperative for loops.
"""

from typing import Any
from functools import reduce


# ---------------------------------------------------------------------------
# Supported chart type whitelist
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
# Utility — pure function variant (returns error string or None)
# ---------------------------------------------------------------------------

def _require_nonempty_str_result(obj: dict, key: str, prefix: str) -> str | None:
    """
    Pure function — returns an error string if key is absent or not a
    non-empty string. Returns None if valid.
    Used with filter(None, map(...)) to collect errors without a for loop.
    """
    if key not in obj:
        return f"{prefix} Missing required key: '{key}'"
    val = obj[key]
    if not isinstance(val, str) or not val.strip():
        return f"{prefix} '{key}' must be a non-empty string. Got: {repr(val)}"
    return None


def _require_nonempty_str(obj: dict, key: str, prefix: str, errors: list[str]) -> None:
    """Side-effect variant — appends to errors list. Delegates to pure variant."""
    result = _require_nonempty_str_result(obj, key, prefix)
    if result:
        errors.append(result)


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
    # filter() keeps only missing keys, map() formats them as error messages
    errors.extend(filter(None, map(
        lambda key: f"[TOP-LEVEL] Missing required key: '{key}'"
                    if key not in cfg else None,
        required_top_keys
    )))


def _check_dataset_path(cfg: dict, errors: list[str]) -> None:
    """dataset_path must be a non-empty string."""
    if "dataset_path" not in cfg:
        return
    val = cfg["dataset_path"]
    if not isinstance(val, str) or not val.strip():
        errors.append(
            "[dataset_path] Value must be a non-empty string. "
            f"Got: {repr(val)}"
        )


def _check_pipeline_dynamics(cfg: dict, errors: list[str]) -> None:
    """
    pipeline_dynamics must contain:
        input_delay_seconds     – float / int  (>= 0)
        core_parallelism        – int          (>= 1)
        stream_queue_max_size   – int          (>= 1)
    """
    if "pipeline_dynamics" not in cfg:
        return

    pd_cfg = cfg["pipeline_dynamics"]

    if not isinstance(pd_cfg, dict):
        errors.append("[pipeline_dynamics] Must be a JSON object (dict).")
        return

    required = {
        "input_delay_seconds":     (float, int),
        "core_parallelism":        (int,),
        "stream_queue_max_size":   (int,),
    }

    def _check_one_key(item: tuple) -> list[str]:
        """Pure function — validates one pipeline_dynamics key. Returns list of errors."""
        key, expected_types = item
        if key not in pd_cfg:
            return [f"[pipeline_dynamics] Missing required key: '{key}'"]
        val = pd_cfg[key]
        if isinstance(val, bool) or not isinstance(val, expected_types):
            type_names = " or ".join(t.__name__ for t in expected_types)
            return [f"[pipeline_dynamics] '{key}' must be of type {type_names}. Got: {repr(val)}"]
        if key in ("core_parallelism", "stream_queue_max_size") and val < 1:
            return [f"[pipeline_dynamics] '{key}' must be >= 1. Got: {val}"]
        if key in ("input_delay_seconds") and val < 0:
            return [f"[pipeline_dynamics] '{key}' must be >= 0. Got: {val}"]
        return []

    # map() validates each key, reduce() flattens all per-key error lists
    errors.extend(reduce(
        lambda acc, lst: acc + lst,
        map(_check_one_key, required.items()),
        []
    ))


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
    required_col_keys  = ["source_name", "internal_mapping", "data_type"]

    def _check_col_key(prefix: str, col: dict, key: str) -> list[str]:
        """Pure function — validates one key inside one column entry."""
        if key not in col:
            return [f"{prefix} Missing required key: '{key}'"]
        val = col[key]
        if not isinstance(val, str) or not val.strip():
            return [f"{prefix} '{key}' must be a non-empty string. Got: {repr(val)}"]
        if key == "data_type" and val not in allowed_data_types:
            return [f"{prefix} 'data_type' must be one of {sorted(allowed_data_types)}. Got: {repr(val)}"]
        return []

    def _check_one_column(item: tuple) -> list[str]:
        """Pure function — validates one column entry (idx, col dict)."""
        idx, col = item
        prefix = f"[schema_mapping.columns[{idx}]]"
        if not isinstance(col, dict):
            return [f"{prefix} Each column entry must be a JSON object."]
        # map() checks every required key, reduce() flattens results
        return reduce(
            lambda acc, lst: acc + lst,
            map(lambda key: _check_col_key(prefix, col, key), required_col_keys),
            []
        )

    # map() validates every column, reduce() flattens all error lists
    errors.extend(reduce(
        lambda acc, lst: acc + lst,
        map(_check_one_column, enumerate(columns)),
        []
    ))


def _check_processing(cfg: dict, errors: list[str]) -> None:
    """
    processing must contain:
        stateless_tasks: operation, algorithm, iterations, secret_key
        stateful_tasks:  operation, running_average_window_size
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
            # map() checks each string field, filter() removes None (valid) results
            errors.extend(filter(None, map(
                lambda key: _require_nonempty_str_result(st, key, "[processing.stateless_tasks]"),
                ["operation", "algorithm", "secret_key"]
            )))
            if "iterations" not in st:
                errors.append("[processing.stateless_tasks] Missing required key: 'iterations'")
            else:
                val = st["iterations"]
                if isinstance(val, bool) or not isinstance(val, int):
                    errors.append(f"[processing.stateless_tasks] 'iterations' must be an integer. Got: {repr(val)}")
                elif val < 1:
                    errors.append(f"[processing.stateless_tasks] 'iterations' must be >= 1. Got: {val}")

    # --- stateful_tasks ---
    if "stateful_tasks" not in proc:
        errors.append("[processing] Missing required key: 'stateful_tasks'")
    else:
        sft = proc["stateful_tasks"]
        if not isinstance(sft, dict):
            errors.append("[processing.stateful_tasks] Must be a JSON object (dict).")
        else:
            errors.extend(filter(None, map(
                lambda key: _require_nonempty_str_result(sft, key, "[processing.stateful_tasks]"),
                ["operation"]
            )))
            if "running_average_window_size" not in sft:
                errors.append("[processing.stateful_tasks] Missing required key: 'running_average_window_size'")
            else:
                val = sft["running_average_window_size"]
                if isinstance(val, bool) or not isinstance(val, int):
                    errors.append(f"[processing.stateful_tasks] 'running_average_window_size' must be an integer. Got: {repr(val)}")
                elif val < 1:
                    errors.append(f"[processing.stateful_tasks] 'running_average_window_size' must be >= 1. Got: {val}")


def _check_visualizations(cfg: dict, errors: list[str]) -> None:
    """
    visualizations must contain:
        telemetry: show_raw_stream, show_intermediate_stream, show_processed_stream
        data_charts: non-empty list of chart dicts with type, title, x_axis, y_axis
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
            telemetry_flags = (
                "show_raw_stream",
                "show_intermediate_stream",
                "show_processed_stream",
            )

            def _check_flag(flag: str) -> list[str]:
                """Pure function — validates one telemetry boolean flag."""
                if flag not in tel:
                    return [f"[visualizations.telemetry] Missing required key: '{flag}'"]
                if not isinstance(tel[flag], bool):
                    return [f"[visualizations.telemetry] '{flag}' must be a boolean (true/false). Got: {repr(tel[flag])}"]
                return []

            # map() checks every flag, reduce() flattens the results
            errors.extend(reduce(
                lambda acc, lst: acc + lst,
                map(_check_flag, telemetry_flags),
                []
            ))

    # --- data_charts ---
    if "data_charts" not in viz:
        errors.append("[visualizations] Missing required key: 'data_charts'")
    else:
        charts = viz["data_charts"]
        if not isinstance(charts, list) or len(charts) == 0:
            errors.append("[visualizations] 'data_charts' must be a non-empty list.")
        else:
            required_chart_keys = ["type", "title", "x_axis", "y_axis"]

            def _check_chart_key(prefix: str, chart: dict, key: str) -> list[str]:
                """Pure function — validates one key in one chart entry."""
                if key not in chart:
                    return [f"{prefix} Missing required key: '{key}'"]
                val = chart[key]
                if not isinstance(val, str) or not val.strip():
                    return [f"{prefix} '{key}' must be a non-empty string. Got: {repr(val)}"]
                if key == "type" and val not in SUPPORTED_CHART_TYPES:
                    return [f"{prefix} 'type' must be one of: {sorted(SUPPORTED_CHART_TYPES)}. Got: {repr(val)}"]
                return []

            def _check_one_chart(item: tuple) -> list[str]:
                """Pure function — validates one chart entry (idx, chart dict)."""
                idx, chart = item
                prefix = f"[visualizations.data_charts[{idx}]]"
                if not isinstance(chart, dict):
                    return [f"{prefix} Each chart entry must be a JSON object."]
                # map() checks every required key, reduce() flattens results
                return reduce(
                    lambda acc, lst: acc + lst,
                    map(lambda key: _check_chart_key(prefix, chart, key), required_chart_keys),
                    []
                )

            # map() validates every chart, reduce() flattens all error lists
            errors.extend(reduce(
                lambda acc, lst: acc + lst,
                map(_check_one_chart, enumerate(charts)),
                []
            ))


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
        return errors

    # map() applies every checker to (cfg, errors) — no for loop needed
    # list() forces evaluation since map() is lazy in Python 3
    list(map(
        lambda checker: checker(cfg, errors),
        [
            _check_top_level_keys,
            _check_dataset_path,
            _check_pipeline_dynamics,
            _check_schema_mapping,
            _check_processing,
            _check_visualizations,
        ]
    ))

    return errors