import time
import multiprocessing
from functools import reduce
import pandas as pd

_CAST_MAP = {
    "string":  str,
    "integer": int,
    "float":   float,
}

def _try_cast(value, data_type: str):
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

class InputModule:

    def __init__(self, config: dict, raw_stream: multiprocessing.Queue,
                 ready_event=None, start_event=None):
        self._dataset_path   = config["dataset_path"]
        self._schema_columns = config["schema_mapping"]["columns"]
        self._delay          = config["pipeline_dynamics"]["input_delay_seconds"]
        self._raw_stream     = raw_stream
        self._ready_event    = ready_event
        self._start_event    = start_event

        self.skipped_rows: list[dict] = []
        self.fatal_errors: list[str]  = []

    def run(self) -> None:
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

        if self._ready_event is not None:
            self._ready_event.set()

        self._stream_to_queue(packets)

    def _load_csv(self):
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
        csv_columns    = set(df.columns)

        config_columns = list(map(lambda col: col["source_name"], self._schema_columns))

        missing = list(filter(lambda c: c not in csv_columns, config_columns))

        if missing:

            new_errors = list(map(
                lambda col: (
                    f"[Input] FATAL — Column '{col}' declared in config "
                    f"schema_mapping but missing from CSV."
                ),
                missing
            ))

            list(map(print, new_errors))
            self.fatal_errors.extend(new_errors)
            return None

        extra = csv_columns - set(config_columns)
        if extra:
            print(f"[Input]  Ignoring extra CSV columns: {sorted(extra)}")

        return df[config_columns].copy()

    def _cast_and_clean(self, df) -> pd.DataFrame:

        col_names = list(map(lambda col: col["source_name"], self._schema_columns))

        def _cast_one_cell(row: pd.Series, col_cfg: dict) -> tuple:
            src_name  = col_cfg["source_name"]
            data_type = col_cfg["data_type"]
            raw_value = row[src_name]
            casted, ok = _try_cast(raw_value, data_type)
            return (src_name, casted if ok else raw_value, ok)

        def _cast_one_row(indexed_row: tuple) -> dict | None:
            row_idx, row = indexed_row

            cast_results = list(map(
                lambda col_cfg: _cast_one_cell(row, col_cfg),
                self._schema_columns
            ))

            failures = list(filter(lambda r: not r[2], cast_results))

            if failures:

                src_name, raw_value, _ = failures[0]

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
                return None

            return dict(map(lambda r: (r[0], r[1]), cast_results))

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

        rename_map = dict(map(
            lambda col: (col["source_name"], col["internal_mapping"]),
            self._schema_columns
        ))
        df = df.rename(columns=rename_map)
        print(f"[Input]  Internal mapping applied: {rename_map}")
        return df

    def _add_priority_index(self, df) -> pd.DataFrame:
        df.insert(0, "priority_index", range(1, len(df) + 1))
        print(f"[Input]  priority_index added (1 to {len(df)}).")
        return df

    def _build_packets(self, df) -> list[dict]:
        packets = df.to_dict(orient="records")
        print(f"[Input]  {len(packets)} packets built and ready to stream.")
        return packets

    def _stream_to_queue(self, packets: list[dict]) -> None:
        if self._start_event is not None:
            print("[Input]  Waiting for Start Pipeline button...")
            self._start_event.wait()
            print("[Input]  Start signal received.")

        total = len(packets)
        print(f"[Input]  Streaming {total} packets "
              f"(delay={self._delay}s each) ...")

        def _push_one(packet: dict) -> None:
            self._raw_stream.put(packet)
            time.sleep(self._delay)

        list(map(_push_one, packets))

        print(f"[Input]  Done — all {total} packets pushed to raw_stream.")