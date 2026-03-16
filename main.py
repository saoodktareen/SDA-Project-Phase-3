"""
main.py — Central Orchestrator
-------------------------------
As per the project spec, main.py is responsible for:
  1. Loading and validating the configuration (via the config layer)
  2. (Later) Creating the multiprocessing.Queue instances
  3. (Later) Instantiating Input, Core, Output module objects
  4. (Later) Wrapping those objects in multiprocessing.Process workers and starting them

For now (config layer testing phase), it simply loads and validates
config.json and prints a confirmation so you can verify everything works.

Run from the project root:
    python main.py
"""

import sys
import os

# Ensure project root is importable regardless of where you run from
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.read_config import load_config


def main() -> None:
    # ── Step 1: Load and validate configuration ────────────────────────────
    # load_config() internally calls validate_config().
    # If config.json is missing, invalid JSON, or fails any validation check,
    # it prints the exact errors to the terminal and exits — no pipeline starts.
    config = load_config("config.json")

    # ── Step 2: Confirm successful load ───────────────────────────────────
    print("[Main]   Pipeline configuration loaded successfully.")
    print(f"[Main]   Dataset        : {config['dataset_path']}")
    print(f"[Main]   Parallelism    : {config['pipeline_dynamics']['core_parallelism']} core workers")
    print(f"[Main]   Queue max size : {config['pipeline_dynamics']['stream_queue_max_size']}")

    # ── Step 3 onwards: Queues, Processes, etc. (to be implemented) ────────
    print("[Main]   Orchestrator ready — pipeline stages not yet implemented.")


if __name__ == "__main__":
    main()