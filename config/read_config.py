import sys
import json

def load_config(path: str = "config.json") -> dict:
    """Load and return the pipeline configuration from config.json."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[Main] ERROR — config file not found: '{path}'")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[Main] ERROR — invalid JSON in config: {e}")
        sys.exit(1)