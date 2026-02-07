from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pyarrow.dataset as ds

from gbdp.silver.writer import write_parquet
from gbdp.utils.io import bronze_root, silver_root, ensure_dir, include_partition_cols_silver
from gbdp.utils.time import daterange, parse_date


def normalize_indy(
    entity: str, start: str, end: str, root: Path | None = None, force: bool = False
) -> List[Path]:
    root = root or silver_root()
    outputs: List[Path] = []
    for d in daterange(parse_date(start), parse_date(end)):
        if entity in {"games", "rosters", "boxscore_batting", "boxscore_pitching"}:
            outputs.append(_normalize_generic(root, entity, d, force))
        else:
            raise ValueError(f"Unsupported indy silver entity: {entity}")
    return outputs


def _normalize_generic(root: Path, entity: str, dt: date, force: bool) -> Path:
    path = bronze_root() / "parsed" / "indy_local" / entity / f"dt={dt.isoformat()}"
    rows: List[Dict[str, Any]] = []
    if path.exists():
        rows = ds.dataset(path, format="parquet").to_table().to_pylist()
    if not include_partition_cols_silver():
        for r in rows:
            r.pop("dt", None)
            r.pop("source", None)
    out_dir = root / "indy_local" / entity / f"dt={dt.isoformat()}"
    ensure_dir(out_dir)
    return write_parquet(rows, out_dir / "part-00001.parquet", force=force)
