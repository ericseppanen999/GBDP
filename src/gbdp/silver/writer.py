from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq

from gbdp.utils.io import ensure_dir


def write_parquet(rows: Iterable[Dict[str, Any]], path: Path) -> Path:
    ensure_dir(path.parent)
    data: List[Dict[str, Any]] = list(rows)
    if not data:
        data = [{"empty": True}]
    table = pa.Table.from_pylist(data)
    pq.write_table(table, path, use_dictionary=False)
    return path

