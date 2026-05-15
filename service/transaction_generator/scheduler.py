import os
import random
import time
from datetime import datetime, timedelta


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_weekdays(name: str, default: str) -> set:
    # Lista po przecinku: 0=poniedziałek ... 6=niedziela. Pusta = brak wyłączeń.
    days = set()
    for part in str(os.getenv(name, default)).split(","):
        part = part.strip()
        if part.isdigit():
            days.add(int(part) % 7)
    return days


# Okno aktywności: aktywne, gdy START <= godzina < END (czas lokalny).
# Domyślnie 6 i 24 -> transakcje powstają tylko między 6:00 a 23:59.
ACTIVE_HOUR_START = _env_int("TXN_ACTIVE_HOUR_START", 6)
ACTIVE_HOUR_END = _env_int("TXN_ACTIVE_HOUR_END", 24)

# Dni tygodnia, w które generator w ogóle nie działa. Domyślnie niedziela (6).
SKIP_WEEKDAYS = _env_weekdays("TXN_SKIP_WEEKDAYS", "6")

# Maksymalna liczba transakcji w jednym cyklu — burst z tego samego sklepu
# (wiele kas naraz). Zwykle wyjdzie 1, czasem aż do MAX_BATCH_SIZE.
MAX_BATCH_SIZE = _env_int("TXN_MAX_BATCH_SIZE", 10)

# Granice odstępu między cyklami (sekundy). Mała dolna granica pozwala,
# by transakcje pojawiały się praktycznie "w tym samym momencie".
MIN_INTERVAL = _env_float("TXN_MIN_INTERVAL", 0.05)
MAX_INTERVAL = _env_float("TXN_MAX_INTERVAL", 20.0)

# Poza oknem aktywności śpimy w kawałkach, żeby kontener reagował
# na Ctrl+C / restart i żeby skorygować dryf zegara.
_MAX_WAIT_CHUNK_SECONDS = 300


def is_active(now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if now.weekday() in SKIP_WEEKDAYS:
        return False
    hour = now.hour
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
    # Przeskocz dni pominięte (np. niedziele), żeby nie budzić się co chwilę.
    while target.weekday() in SKIP_WEEKDAYS:
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


def next_interval(mean: float) -> float:
    """Wykładniczy odstęp między cyklami (proces Poissona).

    Krótkie odstępy są częste, długie rzadkie — to realistyczny model
    napływu transakcji. Suma wielu niezależnych wątków sklepów sprawia,
    że w jednym momencie potrafi pojawić się kilka transakcji z różnych
    sklepów; batch dokłada do tego transakcje z tego samego sklepu.
    """
    if mean <= 0:
        return MIN_INTERVAL
    delay = random.expovariate(1.0 / mean)
    return max(MIN_INTERVAL, min(MAX_INTERVAL, delay))
