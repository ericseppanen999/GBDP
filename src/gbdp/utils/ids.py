from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import Optional

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _encode_crockford(value: int, length: int) -> str:
    chars = []
    for _ in range(length):
        value, rem = divmod(value, 32)
        chars.append(_CROCKFORD[rem])
    return "".join(reversed(chars)).rjust(length, "0")


def ulid_from_key(key: str, dt: Optional[str] = None) -> str:
    """
    Deterministic ULID-like ID using dt for time component and key hash for randomness.
    dt format: YYYY-MM-DD (optional).
    """
    if dt:
        try:
            ts = datetime.strptime(dt, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            ts = datetime.now(timezone.utc)
    else:
        ts = datetime.now(timezone.utc)
    millis = int(ts.timestamp() * 1000)
    time_part = _encode_crockford(millis, 10)

    seed = f"{dt or ''}|{key}|{os.getenv('GBDP_ID_SALT', 'gbdp')}"
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    # take 80 bits from hash for randomness (16 chars base32)
    rand_int = int.from_bytes(digest[:10], "big")
    rand_part = _encode_crockford(rand_int, 16)
    return time_part + rand_part

