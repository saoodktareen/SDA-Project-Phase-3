"""
validate_config.py — Configuration Validator
=============================================
Validates the structure and completeness of config.json.

Checks that all required top-level keys are present, and that
every nested field within each section is also present and
non-empty. For sections that allow multiple entries (like
schema_mapping columns and data_charts), validates that at
least one entry exists.

Returns a list of error strings. Empty list means config is valid.
"""


# ─────────────────────────────────────────────────────────────
#  Required field definitions
# ─────────────────────────────────────────────────────────────

# Top-level keys that must exist in config.json
REQUIRED_TOP_LEVEL_KEYS = [
    "dataset_path",
    "pipeline_dynamics",
    "schema_mapping",
    "processing",
    "visualizations",
]

# Required fields inside pipeline_dynamics
REQUIRED_PIPELINE_DYNAMICS_KEYS = [
    "input_delay_seconds",
    "core_parallelism",
    "stream_queue_max_size",
    "telemetry_poll_interval",
]

# Required fields inside each column entry in schema_mapping.columns
REQUIRED_COLUMN_KEYS = [
    "source_name",
    "internal_mapping",
    "data_type",
]

# Valid data types for schema columns
VALID_DATA_TYPES = {"string", "integer", "float"}

# Required fields inside processing.stateless_tasks
REQUIRED_STATELESS_KEYS = [
    "operation",
    "algorithm",
    "iterations",
    "secret_key",
]

# Required fields inside processing.stateful_tasks
REQUIRED_STATEFUL_KEYS = [
    "operation",
    "running_average_window_size",
]

# Required fields inside visualizations.telemetry
REQUIRED_TELEMETRY_KEYS = [
    "show_raw_stream",
    "show_intermediate_stream",
    "show_processed_stream",
]

# Required fields inside each chart entry in visualizations.data_charts
REQUIRED_CHART_KEYS = [
    "type",
    "title",
    "x_axis",
    "y_axis",
]

# Valid chart types as defined in the project spec
VALID_CHART_TYPES = {
    "real_time_line_graph_values",
    "real_time_line_graph_average",
}


# ─────────────────────────────────────────────────────────────
#  Validation helpers
# ─────────────────────────────────────────────────────────────

def _check_keys(section: dict, required_keys: list, section_name: str) -> list:
    """
    Check that all required_keys exist in section and are not None or empty.
    Returns a list of error strings for any missing or empty fields.
    """
    errors = []
    for key in required_keys:
        if key not in section:
            errors.append(f"[{section_name}] Missing required field: '{key}'")
        elif section[key] is None:
            errors.append(f"[{section_name}] Field '{key}' cannot be null")
        elif isinstance(section[key], str) and not section[key].strip():
            errors.append(f"[{section_name}] Field '{key}' cannot be empty string")
    return errors


# ─────────────────────────────────────────────────────────────
#  Section validators
# ─────────────────────────────────────────────────────────────

def _validate_top_level(config: dict) -> list:
    """Check all top-level keys exist."""
    return _check_keys(config, REQUIRED_TOP_LEVEL_KEYS, "config")


def _validate_dataset_path(config: dict) -> list:
    """Check dataset_path is a non-empty string."""
    errors = []
    path = config.get("dataset_path", "")
    if not isinstance(path, str) or not path.strip():
        errors.append("[dataset_path] Must be a non-empty string path to the CSV file")
    return errors


def _validate_pipeline_dynamics(config: dict) -> list:
    """Check all pipeline_dynamics fields exist and have valid values."""
    errors = []
    pd = config.get("pipeline_dynamics", {})

    if not isinstance(pd, dict):
        return ["[pipeline_dynamics] Must be an object"]

    errors += _check_keys(pd, REQUIRED_PIPELINE_DYNAMICS_KEYS, "pipeline_dynamics")

    # Extra value checks
    if "input_delay_seconds" in pd and pd["input_delay_seconds"] is not None:
        if not isinstance(pd["input_delay_seconds"], (int, float)) or pd["input_delay_seconds"] < 0:
            errors.append("[pipeline_dynamics] 'input_delay_seconds' must be a non-negative number")

    if "core_parallelism" in pd and pd["core_parallelism"] is not None:
        if not isinstance(pd["core_parallelism"], int) or pd["core_parallelism"] < 1:
            errors.append("[pipeline_dynamics] 'core_parallelism' must be an integer >= 1")

    if "stream_queue_max_size" in pd and pd["stream_queue_max_size"] is not None:
        if not isinstance(pd["stream_queue_max_size"], int) or pd["stream_queue_max_size"] < 1:
            errors.append("[pipeline_dynamics] 'stream_queue_max_size' must be an integer >= 1")

    if "telemetry_poll_interval" in pd and pd["telemetry_poll_interval"] is not None:
        if not isinstance(pd["telemetry_poll_interval"], (int, float)) or pd["telemetry_poll_interval"] <= 0:
            errors.append("[pipeline_dynamics] 'telemetry_poll_interval' must be a positive number")

    return errors


def _validate_schema_mapping(config: dict) -> list:
    """Check schema_mapping has at least one column with all required fields."""
    errors = []
    sm = config.get("schema_mapping", {})

    if not isinstance(sm, dict):
        return ["[schema_mapping] Must be an object"]

    if "columns" not in sm:
        return ["[schema_mapping] Missing required field: 'columns'"]

    columns = sm["columns"]

    if not isinstance(columns, list):
        return ["[schema_mapping.columns] Must be a list"]

    if len(columns) == 0:
        return ["[schema_mapping.columns] Must contain at least one column entry"]

    # Validate each column entry
    for i, col in enumerate(columns):
        if not isinstance(col, dict):
            errors.append(f"[schema_mapping.columns[{i}]] Must be an object")
            continue
        errors += _check_keys(col, REQUIRED_COLUMN_KEYS, f"schema_mapping.columns[{i}]")

        # Check data_type is valid
        if "data_type" in col and col["data_type"] not in VALID_DATA_TYPES:
            errors.append(
                f"[schema_mapping.columns[{i}]] 'data_type' must be one of "
                f"{sorted(VALID_DATA_TYPES)}, got '{col['data_type']}'"
            )

    return errors


def _validate_processing(config: dict) -> list:
    """Check processing section has both stateless and stateful tasks."""
    errors = []
    proc = config.get("processing", {})

    if not isinstance(proc, dict):
        return ["[processing] Must be an object"]

    # stateless_tasks
    if "stateless_tasks" not in proc:
        errors.append("[processing] Missing required field: 'stateless_tasks'")
    else:
        st = proc["stateless_tasks"]
        if not isinstance(st, dict):
            errors.append("[processing.stateless_tasks] Must be an object")
        else:
            errors += _check_keys(st, REQUIRED_STATELESS_KEYS, "processing.stateless_tasks")

            if "iterations" in st and st["iterations"] is not None:
                if not isinstance(st["iterations"], int) or st["iterations"] < 1:
                    errors.append("[processing.stateless_tasks] 'iterations' must be a positive integer")

    # stateful_tasks
    if "stateful_tasks" not in proc:
        errors.append("[processing] Missing required field: 'stateful_tasks'")
    else:
        sf = proc["stateful_tasks"]
        if not isinstance(sf, dict):
            errors.append("[processing.stateful_tasks] Must be an object")
        else:
            errors += _check_keys(sf, REQUIRED_STATEFUL_KEYS, "processing.stateful_tasks")

            if "running_average_window_size" in sf and sf["running_average_window_size"] is not None:
                if not isinstance(sf["running_average_window_size"], int) or sf["running_average_window_size"] < 1:
                    errors.append(
                        "[processing.stateful_tasks] 'running_average_window_size' must be a positive integer"
                    )

    return errors


def _validate_visualizations(config: dict) -> list:
    """Check visualizations has telemetry flags and at least one data chart."""
    errors = []
    viz = config.get("visualizations", {})

    if not isinstance(viz, dict):
        return ["[visualizations] Must be an object"]

    # telemetry
    if "telemetry" not in viz:
        errors.append("[visualizations] Missing required field: 'telemetry'")
    else:
        tel = viz["telemetry"]
        if not isinstance(tel, dict):
            errors.append("[visualizations.telemetry] Must be an object")
        else:
            errors += _check_keys(tel, REQUIRED_TELEMETRY_KEYS, "visualizations.telemetry")

            # Each show_* flag must be a boolean
            for key in REQUIRED_TELEMETRY_KEYS:
                if key in tel and tel[key] is not None:
                    if not isinstance(tel[key], bool):
                        errors.append(f"[visualizations.telemetry] '{key}' must be true or false")

    # data_charts
    if "data_charts" not in viz:
        errors.append("[visualizations] Missing required field: 'data_charts'")
    else:
        charts = viz["data_charts"]
        if not isinstance(charts, list):
            errors.append("[visualizations.data_charts] Must be a list")
        elif len(charts) == 0:
            errors.append("[visualizations.data_charts] Must contain at least one chart entry")
        else:
            for i, chart in enumerate(charts):
                if not isinstance(chart, dict):
                    errors.append(f"[visualizations.data_charts[{i}]] Must be an object")
                    continue
                errors += _check_keys(chart, REQUIRED_CHART_KEYS, f"visualizations.data_charts[{i}]")

                # Check chart type is one of the valid spec-defined types
                if "type" in chart and chart["type"] not in VALID_CHART_TYPES:
                    errors.append(
                        f"[visualizations.data_charts[{i}]] 'type' must be one of "
                        f"{sorted(VALID_CHART_TYPES)}, got '{chart['type']}'"
                    )

    return errors


# ─────────────────────────────────────────────────────────────
#  Main entry point
# ─────────────────────────────────────────────────────────────

def validate_config(config: dict) -> list:
    """
    Validate the full config dictionary.

    Runs all section validators and collects every error found.
    Returns an empty list if the config is fully valid.

    Args:
        config : dict loaded from config.json

    Returns:
        List of error strings. Empty list means config is valid.
    """
    errors = []

    # Step 1 — top-level keys must exist before we can check anything else
    top_level_errors = _validate_top_level(config)
    if top_level_errors:
        return top_level_errors  # no point continuing if top level is broken

    # Step 2 — validate each section
    errors += _validate_dataset_path(config)
    errors += _validate_pipeline_dynamics(config)
    errors += _validate_schema_mapping(config)
    errors += _validate_processing(config)
    errors += _validate_visualizations(config)

    return errors