import os
from math import isfinite

QUEUE_MAX_SIZE = int(os.getenv("QUEUEGUARD_QUEUE_MAX_SIZE", "100"))
if QUEUE_MAX_SIZE < 1:
    raise ValueError("QUEUEGUARD_QUEUE_MAX_SIZE must be a positive integer")


DEFAULT_STATUS_RETENTION_SECONDS = 3600.0


def validate_status_retention_seconds(value: float | str) -> float:
    message = "QUEUEGUARD_STATUS_RETENTION_SECONDS must be positive and finite"
    try:
        if isinstance(value, bool):
            raise ValueError(message)
        seconds = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError(message) from None
    if not isfinite(seconds) or seconds <= 0:
        raise ValueError(message)
    return seconds


def get_status_retention_seconds() -> float:
    return validate_status_retention_seconds(
        os.getenv("QUEUEGUARD_STATUS_RETENTION_SECONDS", str(DEFAULT_STATUS_RETENTION_SECONDS))
    )
