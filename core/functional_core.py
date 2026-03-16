import hashlib

# ─────────────────────────────────────────────────────────────
#  FUNCTIONAL CORE — Pure Functions (no side effects, no state)
# ─────────────────────────────────────────────────────────────

def verify_signature(packet: dict, secret_key: str, iterations: int) -> bool:
    """
    Pure function — verifies the cryptographic signature of a packet.

    Signature scheme (verified against sample_sensor_data.csv):
        password = secret_key (encoded as UTF-8 bytes)
        salt     = metric_value formatted to 2 decimal places (encoded as UTF-8 bytes)
        hash_fn  = sha256
        iters    = iterations (100,000)

    Args:
        packet      : generic data packet from InputModule
        secret_key  : secret key string from config
        iterations  : number of PBKDF2 iterations from config

    Returns:
        True if computed hash matches packet's security_hash, False otherwise.
    """
    key           = secret_key.encode("utf-8")
    raw_value_str = f"{packet['metric_value']:.2f}"
    salt          = raw_value_str.encode("utf-8")
    expected_hash = packet["security_hash"]

    computed_hash = hashlib.pbkdf2_hmac(
        "sha256",
        key,
        salt,
        iterations
    ).hex()

    return computed_hash == expected_hash


def compute_average(window: list) -> float:
    """
    Pure function — computes the arithmetic mean of a list of numbers.

    This is the Functional Core of the running average calculation.
    It has no side effects, no state, and is fully testable in isolation.

    Args:
        window : list of float values (the current sliding window)

    Returns:
        The arithmetic mean as a float. Returns 0.0 for empty window.
    """
    if not window:
        return 0.0
    return sum(window) / len(window)