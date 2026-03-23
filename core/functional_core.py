import hashlib

def verify_signature(packet: dict, secret_key: str, iterations: int) -> bool:
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
    if not window:
        return 0.0
    return sum(window) / len(window)