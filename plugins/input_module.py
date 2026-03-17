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
  6. Add a priority_index field (1-based) to each row so Core workers can
     re-sequence out-of-order results after parallel processing (Scatter-Gather)
  7. Convert each row into a packet (dict) and push into the raw_stream Queue
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
        The bounded queue created by main.py and shared with the Core workers.
    """

    def __init__(self, config: dict, raw_stream: multiprocessing.Queue):
        self._dataset_path   = config["dataset_path"]
        self._schema_columns = config["schema_mapping"]["columns"]
        self._delay          = config["pipeline_dynamics"]["input_delay_seconds"]
        self._raw_stream     = raw_stream

        # Populated during run() — available for Streamlit to read
        self.skipped_rows: list[dict] = []  # non-fatal row-level type errors
        self.fatal_errors: list[str]  = []  # missing columns — pipeline must stop

    # ── Public entry point ───────────────────────────────────────────────────

    def run(self) -> None:
        """
        Full pipeline:
          load CSV -> validate columns -> cast -> rename -> add priority_index
          -> build packet list -> push to queue
        """
        # Step 1: load CSV (all values as raw strings — we cast ourselves)
        df = self._load_csv()
        if df is None:
            return

        # Step 2: verify config columns exist, drop extras
        df = self._validate_and_select_columns(df)
        if df is None:
            return

        # Step 3: cast each cell to its declared type; drop un-castable rows
        # Returns a NEW DataFrame with correct Python types (not string dtype)
        df = self._cast_and_clean(df)

        # Step 4: rename source_name -> internal_mapping
        df = self._apply_internal_mapping(df)

        # Step 5: add priority_index (1-based) for Core re-sequencing
        df = self._add_priority_index(df)

        # Step 6: convert DataFrame rows to a list of dicts (packets)
        packets = self._build_packets(df)

        # Step 7: push packets into the queue one by one
        self._stream_to_queue(packets)

    # ── Private steps ────────────────────────────────────────────────────────

    def _load_csv(self):
        """Load CSV as all-string DataFrame. Returns None on failure."""
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
        Check every source_name in config exists in the CSV.
        Extra CSV columns are silently dropped.
        Missing config columns are FATAL — returns None.
        """
        csv_columns    = set(df.columns)
        config_columns = [col["source_name"] for col in self._schema_columns]

        missing = [c for c in config_columns if c not in csv_columns]
        if missing:
            for col in missing:
                msg = (f"[Input] FATAL — Column '{col}' declared in config "
                       f"schema_mapping but missing from CSV.")
                self.fatal_errors.append(msg)
                print(msg)
            return None

        extra = csv_columns - set(config_columns)
        if extra:
            print(f"[Input]  Ignoring extra CSV columns: {sorted(extra)}")

        return df[config_columns].copy()

    def _cast_and_clean(self, df) -> pd.DataFrame:
        """
        Cast every cell to its declared data_type.
        Any row with even one un-castable cell is dropped entirely and logged
        in self.skipped_rows (non-fatal — pipeline continues).

        Builds and returns a BRAND NEW DataFrame with native Python types
        (int, float, str) instead of writing back into the all-string source df.
        This avoids pandas dtype conflicts when storing int/float into str columns.
        """
        clean_rows = []   # list of fully-cast row dicts — becomes the new DataFrame

        col_names = [col["source_name"] for col in self._schema_columns]

        for row_idx, row in df.iterrows():
            row_ok   = True
            cast_row = {}

            for col_cfg in self._schema_columns:
                src_name  = col_cfg["source_name"]
                data_type = col_cfg["data_type"]
                raw_value = row[src_name]

                casted, ok = _try_cast(raw_value, data_type)

                if not ok:
                    self.skipped_rows.append({
                        "row_index": row_idx,
                        "column":    src_name,
                        "raw_value": raw_value,
                        "reason":    f"Cannot cast '{raw_value}' to {data_type}",
                    })
                    row_ok = False
                    break

                cast_row[src_name] = casted

            if row_ok:
                clean_rows.append(cast_row)

        if self.skipped_rows:
            print(f"[Input]  Skipped {len(self.skipped_rows)} row(s) — "
                  f"type-cast failures.")

        # Build a fresh DataFrame from clean rows — correct native types throughout
        clean_df = pd.DataFrame(clean_rows, columns=col_names)
        print(f"[Input]  {len(clean_df)} rows passed type validation.")
        return clean_df

    def _apply_internal_mapping(self, df) -> pd.DataFrame:
        """
        Rename columns: source_name -> internal_mapping.
        After this step the DataFrame has only generic internal names.
        Core has zero knowledge of original CSV column names.
        """
        rename_map = {
            col["source_name"]: col["internal_mapping"]
            for col in self._schema_columns
        }
        df = df.rename(columns=rename_map)
        print(f"[Input]  Internal mapping applied: {rename_map}")
        return df

    def _add_priority_index(self, df) -> pd.DataFrame:
        """
        Insert a 'priority_index' column as the FIRST column.
        Values are 1-based integers (1, 2, 3, ..., N).

        Purpose: after the Scatter-Gather pattern scatters packets across
        multiple Core workers in parallel, results arrive out of order.
        The Core Aggregator uses priority_index to re-sequence them back
        into the original order before computing the running average.
        """
        df.insert(0, "priority_index", range(1, len(df) + 1))
        print(f"[Input]  priority_index added (1 to {len(df)}).")
        return df

    def _build_packets(self, df) -> list[dict]:
        """
        Convert the validated, mapped, indexed DataFrame into a list of dicts.
        Each dict is one self-contained data packet that the Core workers
        will receive from the queue.

        Example packet (with our sensor dataset):
        {
            "priority_index": 1,
            "entity_name":    "Sensor_Alpha",
            "time_period":    1773037623,
            "metric_value":   24.99,
            "security_hash":  "18d9d277ba10acd3..."
        }
        """
        packets = df.to_dict(orient="records")
        print(f"[Input]  {len(packets)} packets built and ready to stream.")
        return packets

    def _stream_to_queue(self, packets: list[dict]) -> None:
        """
        Push each packet into the raw_stream Queue one by one.

        Backpressure:
        - Queue.put() BLOCKS automatically when the queue is full.
        - time.sleep(input_delay_seconds) controls production rate.
        - Together they throttle Input naturally — no extra code needed.

        Sentinels:
        - NOT pushed here. main.py pushes one None per Core worker after
          this process finishes, because only main.py knows core_parallelism.
        """
        total = len(packets)
        print(f"[Input]  Streaming {total} packets "
              f"(delay={self._delay}s each) ...")

        for packet in packets:
            self._raw_stream.put(packet)   # blocks if queue is full → backpressure
            time.sleep(self._delay)

        print(f"[Input]  Done — all {total} packets pushed to raw_stream.")