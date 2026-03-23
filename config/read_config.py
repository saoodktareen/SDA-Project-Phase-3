import sys
import json
from config.validate_config import validate_config

def load_config(path: str = "config.json") -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"[Config] ERROR - config file not found: '{path}'")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[Config] ERROR - invalid JSON in config: {e}")
        sys.exit(1)

    errors = validate_config(config)
    if errors:
        print("[Config] ERROR - config.json failed validation:")

        list(map(lambda err: print(f"         {err}"), errors))
        sys.exit(1)

    print(f"[Config] Loaded and validated: '{path}'")
    return config