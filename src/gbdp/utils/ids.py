from __future__ import annotations

import hashlib
import os

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _encode_crockford(value: int, length: int) -> str:
    chars = []
    for _ in range(length):
        value, rem = divmod(value, 32)
        chars.append(_CROCKFORD[rem])
    return "".join(reversed(chars)).rjust(length, "0")


def ulid_from_key(key: str) -> str:
    """
    Deterministic 26-char ULID-shaped ID derived from `key` alone.

    Must never take a date or any other per-run input: canonical IDs have to
    stay the same on every snapshot, or nothing joins across dates. Including
    dt here gave every player/team/game a new ID each day -- confirmed in
    production (0 of 2,369 entities kept their ID from 2026-09-11 to 09-12).
    This gives up ULID's time-ordered prefix, which can't be derived from a
    key without storing a first-seen date.
    """
    seed = f"{key}|{os.getenv('GBDP_ID_SALT', 'gbdp')}"
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    # 26 base32 chars hold 130 bits; take 136 and drop the low 6.
    return _encode_crockford(int.from_bytes(digest[:17], "big") >> 6, 26)
