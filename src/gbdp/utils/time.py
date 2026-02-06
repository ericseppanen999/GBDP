from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Iterable


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def daterange(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current = current + timedelta(days=1)


def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

