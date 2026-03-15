"""
input_module.py — Generic CSV Producer
=======================================
Reads the dataset row by row, maps column names and casts types
strictly based on config.json schema_mapping.

This module is completely domain-agnostic — it has zero knowledge
of sensors, GDP, or any specific dataset. It only understands:
  - source_name   → which CSV column to read
  - internal_mapping → what to rename it to in the packet
  - data_type     → how to cast it (string / integer / float)

Producer in the Producer-Consumer architecture:
  Reads rows → builds generic packets → pushes into raw_queue (Queue 1)
"""

import csv
import time
import multiprocessing
from typing import Any


# ─────────────────────────────────────────────────────────────
#  Type casting helpers — driven entirely by config data_type
# ─────────────────────────────────────────────────────────────

TYPE_CASTERS = {
    "string":  str,
    "integer": int,
    "float":   float,
}

def cast_value(value: str, data_type: str) -> Any:
    """
    Cast a raw string value from the CSV to the configured type.
    Falls back to the raw string if casting fails.
    """
    caster = TYPE_CASTERS.get(data_type, str)
    try:
        return caster(value)
    except (ValueError, TypeError):
        return value


def map_row_to_packet(row: dict, schema_columns: list, packet_id: int) -> dict:
    """
    Pure function — converts a raw CSV row dict into a generic
    internal packet using the schema_mapping from config.

    Args:
        row            : raw dict from csv.DictReader {source_name: raw_string}
        schema_columns : list of column mapping dicts from config
        packet_id      : unique sequential ID assigned by the Input process

    Returns:
        A generic packet dict with internal_mapping keys, cast values,
        and a packet_id field for re-sequencing downstream.
    """
    packet = {"packet_id": packet_id}

    for col in schema_columns:
        source_name      = col["source_name"]
        internal_mapping = col["internal_mapping"]
        data_type        = col["data_type"]

        raw_value = row.get(source_name, "")
        packet[internal_mapping] = cast_value(raw_value, data_type)

    return packet


# ─────────────────────────────────────────────────────────────
#  InputModule — the Producer Process
# ─────────────────────────────────────────────────────────────

class InputModule:
    """
    Generic CSV Producer.

    Reads the dataset configured in config.json, converts each row
    into a domain-agnostic packet, and pushes it into raw_queue.

    The module is completely unaware of what the data represents.
    All behaviour is driven by the config passed at construction.

    Multiprocessing role: runs as a separate Process started by main.py
    """

    def __init__(self, config: dict, raw_queue: multiprocessing.Queue):
        """
        Args:
            config    : full config dict loaded from config.json
            raw_queue : Queue 1 — bounded multiprocessing.Queue shared
                        with Core worker processes
        """
        self.dataset_path   = config["dataset_path"]
        self.input_delay    = config["pipeline_dynamics"]["input_delay_seconds"]
        self.schema_columns = config["schema_mapping"]["columns"]
        self.raw_queue      = raw_queue

    def run(self) -> None:
        """
        Entry point — called inside a multiprocessing.Process.

        Reads every row from the CSV, maps it to a generic packet,
        and puts it on raw_queue. Sleeps input_delay_seconds between
        rows to simulate a real-time data stream.

        When all rows are consumed, pushes a sentinel value (None)
        for each Core worker so they know the stream has ended.
        The number of sentinels equals core_parallelism so every
        worker receives exactly one stop signal.
        """
        print(f"[Input] Starting — reading from '{self.dataset_path}'")
        packet_id = 0

        try:
            with open(self.dataset_path, newline="", encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file)

                for row in reader:
                    packet = map_row_to_packet(row, self.schema_columns, packet_id)

                    # put() blocks automatically if the queue is full (backpressure)
                    self.raw_queue.put(packet)

                    print(f"[Input] Sent packet #{packet_id:>4} | "
                          f"{list(packet.items())[1][0]}={list(packet.items())[1][1]}")

                    packet_id += 1
                    time.sleep(self.input_delay)

        except FileNotFoundError:
            print(f"[Input] ERROR — dataset not found: '{self.dataset_path}'")
            return

        print(f"[Input] Done — sent {packet_id} packets total.")