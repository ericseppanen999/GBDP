from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pyarrow as pa

from gbdp.utils.io import bronze_root, ensure_dir, sha256_bytes, stable_json_dumps, write_parquet_table


def write_request_log(record: Dict[str, Any], force: bool = False) -> Path:
    dt = record.get("dt")
    if not dt:
        dt = datetime.now(timezone.utc).date().isoformat()
        record["dt"] = dt
    out_dir = bronze_root() / "requests" / f"dt={dt}"
    ensure_dir(out_dir)
    key = sha256_bytes(stable_json_dumps(record).encode("utf-8"))[:16]
    out_path = out_dir / f"part-{key}.parquet"
    table = pa.Table.from_pylist([_normalize_record(record)])
    return write_parquet_table(table, out_path, force=force)


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(record)
    if "params" in out and isinstance(out["params"], (dict, list)):
        out["params"] = stable_json_dumps(out["params"])
    if "headers" in out and isinstance(out["headers"], (dict, list)):
        out["headers"] = stable_json_dumps(out["headers"])
    return out
