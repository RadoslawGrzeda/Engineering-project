import os
import random
import time
from datetime import datetime, timedelta


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


ACTIVE_HOUR_START = _env_int("CRM_ACTIVE_HOUR_START", 6)
ACTIVE_HOUR_END = _env_int("CRM_ACTIVE_HOUR_END", 24)

MIN_SLEEP_SECONDS = _env_int("CRM_MIN_SLEEP_SECONDS", 5)
MAX_SLEEP_SECONDS = _env_int("CRM_MAX_SLEEP_SECONDS", 90)

MAX_BATCH_SIZE = _env_int("CRM_MAX_BATCH_SIZE", 10)

_MAX_WAIT_CHUNK_SECONDS = 300


def is_active(now: datetime | None = None) -> bool:

    hour = (now or datetime.now()).hour
    start, end = ACTIVE_HOUR_START, ACTIVE_HOUR_END
    if start < end:
        return start <= hour < end
    # Okno przechodzące przez północ, np. 22 -> 6.
    return hour >= start or hour < end


def _seconds_until_active(now: datetime) -> float:
    target = now.replace(
        hour=ACTIVE_HOUR_START % 24, minute=0, second=0, microsecond=0
    )
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def wait_until_active() -> None:
    while not is_active():
        remaining = _seconds_until_active(datetime.now())
        time.sleep(min(remaining, _MAX_WAIT_CHUNK_SECONDS))


def next_batch_size() -> int:
    if MAX_BATCH_SIZE <= 1:
        return 1
    size = int(random.triangular(1, MAX_BATCH_SIZE + 1, 1))
    return max(1, min(MAX_BATCH_SIZE, size))


def sleep_random() -> None:
    time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))
