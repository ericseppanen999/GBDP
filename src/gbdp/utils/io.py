from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict


def data_root() -> Path:
    return Path(os.getenv("GBDP_DATA_ROOT", "./data")).resolve()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def request_hash(url: str, params: Dict[str, Any] | None) -> str:
    payload = {"url": url, "params": params or {}}
    return sha256_bytes(stable_json_dumps(payload).encode("utf-8"))


def include_partition_cols_silver() -> bool:
    return os.getenv("GBDP_INCLUDE_PARTITION_COLS_SILVER", "false").lower() in (
        "1",
        "true",
        "yes",
    )


def storage_format() -> str:
    return os.getenv("GBDP_STORAGE_FORMAT", "parquet").lower()
