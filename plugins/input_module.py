"""
input_module.py — Input Module (Producer)
------------------------------------------
Responsibilities (per Phase 3 PDF spec):
  1. Read the CSV from dataset_path into a DataFrame
  2. Validate that every column named in config schema_mapping exists in the CSV
  3. Keep only the columns mentioned in config (drop the rest)
  4. Type-cast each cell to its declared data_type
  5. Rename columns from source_name  →  internal_mapping  (generic packet keys)
  6. Add a priority_index field (1-based) to each row so Core workers can
     re-sequence out-of-order results after parallel processing (Scatter-Gather)
  7. Convert each row into a packet (dict) and push into the raw_stream Queue
     — respects input_delay_seconds between pushes to create controllable backpressure

All iteration uses functional programming — map(), filter(), reduce(),
and list comprehensions — instead of imperative for loops.

DIP compliance:
  - InputModule depends only on the config dict (abstract data) and a
    multiprocessing.Queue (abstract channel). It knows nothing about
    Core, Output, or any other concrete module.
  - It is instantiated by main.py (the orchestrator) and run as a
    separate multiprocessing.Process.
"""

import time
import multiprocessing
from functools import reduce
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
    config      : dict                  — fully validated config from load_config()
    raw_stream  : multiprocessing.Queue — bounded queue shared with Core workers
    ready_event : multiprocessing.Event — fired after validation, before streaming
    start_event : multiprocessing.Event — waited on before streaming starts
    """

    def __init__(self, config: dict, raw_stream: multiprocessing.Queue,
                 ready_event=None, start_event=None):
        self._dataset_path   = config["dataset_path"]
        self._schema_columns = config["schema_mapping"]["columns"]
        self._delay          = config["pipeline_dynamics"]["input_delay_seconds"]
        self._raw_stream     = raw_stream
        self._ready_event    = ready_event   # fired after validation, before streaming
        self._start_event    = start_event   # waited on before streaming starts

        # Populated during run() — available for the dashboard to display
        self.skipped_rows: list[dict] = []  # non-fatal row-level type errors
        self.fatal_errors: list[str]  = []  # missing columns — pipeline must stop

    # ── Public entry point ───────────────────────────────────────────────────

    def run(self) -> None:
        """
        Full pipeline:
          load CSV -> validate columns -> cast -> rename -> add priority_index
          -> build packet list -> push to queue
        """
        df = self._load_csv()
        if df is None:
            if self._ready_event is not None:
                self._ready_event.set()
            return

        df = self._validate_and_select_columns(df)
        if df is None:
            if self._ready_event is not None:
                self._ready_event.set()
            return

        df = self._cast_and_clean(df)
        df = self._apply_internal_mapping(df)
        df = self._add_priority_index(df)
        packets = self._build_packets(df)

        # Signal Screen 1 to draw — validation done, skipped_rows populated.
        # Streaming has NOT started yet so Q1 will visibly fill after button click.
        if self._ready_event is not None:
            self._ready_event.set()

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

        Functional: filter() finds missing columns instead of a for loop.
        """
        csv_columns    = set(df.columns)
        # map() extracts source_name from every schema column — replaces list comprehension
        config_columns = list(map(lambda col: col["source_name"], self._schema_columns))

        # filter() replaces: for c in config_columns: if c not in csv_columns
        missing = list(filter(lambda c: c not in csv_columns, config_columns))

        if missing:
            # map() builds error messages for every missing column,
            # list() forces evaluation, extend() appends them all at once
            new_errors = list(map(
                lambda col: (
                    f"[Input] FATAL — Column '{col}' declared in config "
                    f"schema_mapping but missing from CSV."
                ),
                missing
            ))
            # map() also prints each error as a side effect
            list(map(print, new_errors))
            self.fatal_errors.extend(new_errors)
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

        Functional replacement for the nested for loops:
          - _cast_one_row() is a pure function applied via map() to every row
          - filter() separates clean rows from skipped rows
          - reduce() is not needed — map + filter is sufficient here
        """
        # map() extracts source_name from every schema column — replaces list comprehension
        col_names = list(map(lambda col: col["source_name"], self._schema_columns))

        def _cast_one_cell(row: pd.Series, col_cfg: dict) -> tuple:
            """
            Pure function — attempts to cast one cell.
            Returns (src_name, casted_value, True) on success,
                    (src_name, raw_value,    False) on failure.
            """
            src_name  = col_cfg["source_name"]
            data_type = col_cfg["data_type"]
            raw_value = row[src_name]
            casted, ok = _try_cast(raw_value, data_type)
            return (src_name, casted if ok else raw_value, ok)

        def _cast_one_row(indexed_row: tuple) -> dict | None:
            """
            Pure function — casts every cell in one row using map().

            Returns a clean dict  {col_name: cast_value, ...}  on full success.
            Returns None if any cell fails to cast (row is skipped).

            Replaces the inner for loop:
              for col_cfg in self._schema_columns: ...
            """
            row_idx, row = indexed_row

            # map() applies _cast_one_cell to every column config
            cast_results = list(map(
                lambda col_cfg: _cast_one_cell(row, col_cfg),
                self._schema_columns
            ))

            # Find the first failed cell (if any) using filter()
            failures = list(filter(lambda r: not r[2], cast_results))

            if failures:
                # Record skipped row for the dashboard using the first failure
                src_name, raw_value, _ = failures[0]
                # filter() finds the matching column, next() takes the first result
                data_type = next(iter(filter(
                    lambda c: c["source_name"] == src_name,
                    self._schema_columns
                )))["data_type"]
                self.skipped_rows.append({
                    "row_index": row_idx,
                    "column":    src_name,
                    "raw_value": raw_value,
                    "reason":    f"Cannot cast '{raw_value}' to {data_type}",
                })
                return None   # signals this row should be dropped

            # All cells cast successfully — build the clean row dict
            # dict() + map() builds the clean row dict — replaces dict comprehension
            return dict(map(lambda r: (r[0], r[1]), cast_results))

        # map() applies _cast_one_row to every (index, row) pair,
        # filter() removes None entries (the skipped rows)
        clean_rows = list(filter(
            lambda r: r is not None,
            map(_cast_one_row, df.iterrows())
        ))

        if self.skipped_rows:
            print(f"[Input]  Skipped {len(self.skipped_rows)} row(s) — "
                  f"type-cast failures.")

        clean_df = pd.DataFrame(clean_rows, columns=col_names)
        print(f"[Input]  {len(clean_df)} rows passed type validation.")
        return clean_df

    def _apply_internal_mapping(self, df) -> pd.DataFrame:
        """
        Rename columns: source_name -> internal_mapping.
        After this step the DataFrame has only generic internal names.
        Core has zero knowledge of original CSV column names.
        """
        # dict() + map() builds the rename mapping — replaces dict comprehension
        rename_map = dict(map(
            lambda col: (col["source_name"], col["internal_mapping"]),
            self._schema_columns
        ))
        df = df.rename(columns=rename_map)
        print(f"[Input]  Internal mapping applied: {rename_map}")
        return df

    def _add_priority_index(self, df) -> pd.DataFrame:
        """
        Insert a 'priority_index' column as the FIRST column (1-based).
        Used by the Aggregator to re-sequence out-of-order packets
        after the Scatter-Gather pattern.
        """
        df.insert(0, "priority_index", range(1, len(df) + 1))
        print(f"[Input]  priority_index added (1 to {len(df)}).")
        return df

    def _build_packets(self, df) -> list[dict]:
        """
        Convert the validated, mapped, indexed DataFrame into a list of dicts.
        Each dict is one self-contained data packet for the Core workers.

        Example packet:
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
          Queue.put() BLOCKS automatically when the queue is full.
          time.sleep(input_delay_seconds) controls production rate.
          Together they throttle Input naturally — no extra code needed.

        Functional: map() replaces the for loop over packets.
        The lambda pushes one packet then sleeps — applied to every packet.

        Sentinels:
          NOT pushed here. main.py pushes one None per Core worker after
          this process finishes, because only main.py knows core_parallelism.
        """
        if self._start_event is not None:
            print("[Input]  Waiting for Start Pipeline button...")
            self._start_event.wait()
            print("[Input]  Start signal received.")

        total = len(packets)
        print(f"[Input]  Streaming {total} packets "
              f"(delay={self._delay}s each) ...")

        def _push_one(packet: dict) -> None:
            """Pure action — push one packet then sleep."""
            self._raw_stream.put(packet)   # blocks if queue full → backpressure
            time.sleep(self._delay)

        # map() applies _push_one to every packet — replaces the for loop.
        # list() forces evaluation since map() is lazy in Python 3.
        list(map(_push_one, packets))

        print(f"[Input]  Done — all {total} packets pushed to raw_stream.")