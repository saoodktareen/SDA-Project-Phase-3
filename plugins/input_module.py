"""
input_module.py — Input Module (Producer)
------------------------------------------
Responsibilities (per Phase 3 PDF spec):
  1. Read the CSV from dataset_path into a DataFrame
  2. Validate that every column named in config schema_mapping exists in the CSV
     — extra CSV columns are silently ignored
     — missing config columns are a FATAL error (shown on Streamlit, pipeline stops)
  3. Keep only the columns mentioned in config (drop the rest)
  4. Type-cast each cell to its declared data_type
     — if a cell cannot be cast (empty, wrong format) the ENTIRE ROW is dropped
     — dropped-row details are stored in self.skipped_rows for Streamlit to display
     — these row-level errors do NOT stop the pipeline
  5. Rename columns from source_name  →  internal_mapping  (generic packet keys)
  6. Push one data packet (dict) per row into the raw_stream Queue
     — respects input_delay_seconds between pushes to create controllable backpressure

DIP compliance:
  - InputModule depends only on the config dict (abstract data) and a
    multiprocessing.Queue (abstract channel). It knows nothing about
    Core, Output, or any other concrete module.
  - It is instantiated by main.py (the orchestrator) and run as a
    separate multiprocessing.Process.
"""

import time
import multiprocessing
import pandas as pd


# ── Type casting helpers ─────────────────────────────────────────────────────

# Map config data_type strings to Python callables
_CAST_MAP = {
    "string":  str,
    "integer": int,
    "float":   float,
}


def _try_cast(value, data_type: str):
    """
    Attempt to cast `value` to the type named by `data_type`.

    Returns (cast_value, True)  on success.
    Returns (None,       False) on any failure (empty, NaN, wrong format, etc.).
    """
    cast_fn = _CAST_MAP.get(data_type)
    if cast_fn is None:
        return None, False
    try:
        # Treat pandas NaN / None / blank strings as un-castable
        if pd.isna(value):
            return None, False
        casted = cast_fn(str(value).strip())
        return casted, True
    except (ValueError, TypeError):
        return None, False


# ── InputModule class ────────────────────────────────────────────────────────

class InputModule:
    """
    Producer process — reads a CSV, validates + maps schema, pushes packets.

    Parameters
    ----------
    config : dict
        The fully validated config dict from load_config().
    raw_stream : multiprocessing.Queue
        The bounded queue shared with the Core workers.
    """

    def __init__(self, config: dict, raw_stream: multiprocessing.Queue):
        self._dataset_path   = config["dataset_path"]
        self._schema_columns = config["schema_mapping"]["columns"]
        self._delay          = config["pipeline_dynamics"]["input_delay_seconds"]
        self._raw_stream     = raw_stream

        # These are populated during run() and available for Streamlit to read
        self.skipped_rows: list[dict] = []  # non-fatal row-level type errors
        self.fatal_errors: list[str]  = []  # missing columns — pipeline must stop

    # ── Public entry point (called by multiprocessing.Process) ──────────────

    def run(self) -> None:
        """
        Full pipeline:  load CSV -> validate columns -> cast -> rename -> push to queue.
        """
        # Step 1: load CSV into DataFrame (all values as raw strings first)
        df = self._load_csv()
        if df is None:
            return   # fatal error recorded; orchestrator will handle it

        # Step 2: check every config column exists in the CSV
        df = self._validate_and_select_columns(df)
        if df is None:
            return   # fatal error recorded

        # Step 3: cast each cell; drop any row that has an un-castable cell
        df = self._cast_and_clean(df)

        # Step 4: rename source_name -> internal_mapping
        df = self._apply_internal_mapping(df)

        # Step 5: push packets into the raw stream queue one by one
        self._stream_to_queue(df)

    # ── Private steps ────────────────────────────────────────────────────────

    def _load_csv(self):
        """
        Load the CSV file into a DataFrame.
        All values are read as strings so we can do our own casting in step 3.
        Returns None on failure (fatal error recorded in self.fatal_errors).
        """
        try:
            df = pd.read_csv(self._dataset_path, dtype=str)
            print(f"[Input]  Loaded '{self._dataset_path}' "
                  f"— {len(df)} rows, {len(df.columns)} columns")
            return df
        except FileNotFoundError:
            msg = f"[Input] FATAL — CSV file not found: '{self._dataset_path}'"
            self.fatal_errors.append(msg)
            print(msg)
            return None
        except Exception as e:
            msg = f"[Input] FATAL — Could not read CSV: {e}"
            self.fatal_errors.append(msg)
            print(msg)
            return None

    def _validate_and_select_columns(self, df):
        """
        Ensure every source_name declared in config exists as a CSV column.

        - Extra CSV columns are silently dropped (we only keep config columns).
        - Missing config columns are FATAL: recorded and None is returned so
          the orchestrator can display the error on Streamlit and stop.
        """
        csv_columns    = set(df.columns)
        config_columns = [col["source_name"] for col in self._schema_columns]

        # Check for missing columns — FATAL
        missing = [c for c in config_columns if c not in csv_columns]
        if missing:
            for col in missing:
                msg = (f"[Input] FATAL — Column '{col}' is declared in "
                       f"config schema_mapping but is not present in the CSV.")
                self.fatal_errors.append(msg)
                print(msg)
            return None

        # Report and discard any extra columns the CSV has but config does not need
        extra = csv_columns - set(config_columns)
        if extra:
            print(f"[Input]  Ignoring extra CSV columns (not in config): {sorted(extra)}")

        # Return a DataFrame with ONLY the config columns, in config order
        return df[config_columns].copy()

    def _cast_and_clean(self, df) -> pd.DataFrame:
        """
        Cast every cell to its declared data_type from config.

        Rules:
        - If ALL cells in a row cast successfully, the row is kept.
        - If ANY single cell in a row fails (empty, wrong type, etc.), the
          ENTIRE ROW is dropped and logged in self.skipped_rows.
        - self.skipped_rows is non-fatal — the pipeline keeps running.

        Returns a clean DataFrame with correct Python types.
        """
        keep_indices = []

        for row_idx, row in df.iterrows():
            row_ok   = True
            cast_row = {}

            for col_cfg in self._schema_columns:
                src_name  = col_cfg["source_name"]
                data_type = col_cfg["data_type"]
                raw_value = row[src_name]

                casted, ok = _try_cast(raw_value, data_type)

                if not ok:
                    # Record the failure and mark the whole row for dropping
                    self.skipped_rows.append({
                        "row_index": row_idx,
                        "column":    src_name,
                        "raw_value": raw_value,
                        "reason":    f"Cannot cast '{raw_value}' to {data_type}",
                    })
                    row_ok = False
                    break   # no need to check other columns in this row

                cast_row[src_name] = casted

            if row_ok:
                for k, v in cast_row.items():
                    df.at[row_idx, k] = v
                keep_indices.append(row_idx)

        if self.skipped_rows:
            print(f"[Input]  Skipped {len(self.skipped_rows)} row(s) due to "
                  f"type-cast failures.")

        clean_df = df.loc[keep_indices].reset_index(drop=True)
        print(f"[Input]  {len(clean_df)} rows passed type validation.")
        return clean_df

    def _apply_internal_mapping(self, df) -> pd.DataFrame:
        """
        Rename columns: source_name -> internal_mapping.

        After this step the DataFrame uses only generic internal names
        (e.g. entity_name, time_period, metric_value, security_hash).
        The Core module will only ever see these generic names — it has
        zero knowledge of the original CSV column names.
        """
        rename_map = {
            col["source_name"]: col["internal_mapping"]
            for col in self._schema_columns
        }
        df = df.rename(columns=rename_map)
        print(f"[Input]  Internal mapping applied: {rename_map}")
        return df

    def _stream_to_queue(self, df) -> None:
        """
        Push each row as a packet (plain dict) into the raw_stream Queue.

        Backpressure mechanism:
        - Queue.put() BLOCKS automatically when the queue is full.
        - time.sleep(input_delay_seconds) controls how fast packets are produced.
        - Together these two naturally throttle the Input module when the Core
          workers cannot keep up — no extra backpressure code needed.

        Sentinel value:
        - After all rows are pushed, a None sentinel is NOT pushed here.
        - main.py (the orchestrator) is responsible for pushing one None per
          Core worker after the Input process finishes, so each worker knows
          when to stop.
        """
        total = len(df)
        print(f"[Input]  Streaming {total} packets "
              f"(delay={self._delay}s each) ...")

        for _, row in df.iterrows():
            packet = row.to_dict()
            self._raw_stream.put(packet)  # blocks naturally if queue is full
            time.sleep(self._delay)

        print(f"[Input]  Done — all {total} packets pushed to raw stream.")