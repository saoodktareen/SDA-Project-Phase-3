import sys
import json
from config.validate_config import validate_config

def load_config(path: str = "config.json") -> dict:
    """
    Load, validate, and return the pipeline configuration from config.json.

    Exits with a clear error message if:
      - The file is not found
      - The file contains invalid JSON
      - Any required field is missing or invalid (caught by validate_config)

    Argument:
        path : path to the config JSON file (default: 'config.json')

    Returns:
        Validated config dict ready for use by the pipeline.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"[Config] ERROR - config file not found: '{path}'")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[Config] ERROR - invalid JSON in config: {e}")
        sys.exit(1)

    # Validate structure and completeness
    errors = validate_config(config)
    if errors:
        print("[Config] ERROR - config.json failed validation:")
        for err in errors:
            print(f"         {err}")
        sys.exit(1)

    print(f"[Config] Loaded and validated: '{path}'")
    return config