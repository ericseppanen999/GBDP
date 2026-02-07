from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from gbdp.silver.writer import write_parquet
from gbdp.utils.io import bronze_root, ensure_dir, include_partition_cols_silver, path_exists, read_parquet_rows, silver_root
from gbdp.utils.time import daterange, parse_date


def normalize_local_boxscore(
    source: str, entity: str, start: str, end: str, root: Path | None = None, force: bool = False
) -> List[Path]:
    root = root or silver_root()
    outputs: List[Path] = []
    for d in daterange(parse_date(start), parse_date(end)):
        outputs.append(_normalize_generic(root, source, entity, d, force))
    return outputs


def _normalize_generic(root: Path, source: str, entity: str, dt: date, force: bool) -> Path:
    path = bronze_root() / "parsed" / source / entity / f"dt={dt.isoformat()}"
    rows: List[Dict[str, Any]] = []
    if path_exists(path):
        rows = read_parquet_rows(path)
    if not include_partition_cols_silver():
        for r in rows:
            r.pop("dt", None)
            r.pop("source", None)
    out_dir = root / source / entity / f"dt={dt.isoformat()}"
    ensure_dir(out_dir)
    return write_parquet(rows, out_dir / "part-00001.parquet", force=force)
