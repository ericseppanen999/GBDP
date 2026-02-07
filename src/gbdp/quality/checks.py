from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict, List

import duckdb
import yaml

from gbdp.utils.io import data_root, ensure_dir
from gbdp.utils.time import daterange, parse_date


def run_quality_checks(
    start: str, end: str, root: Path | None = None, force: bool = False
) -> List[Path]:
    root = root or data_root()
    outputs: List[Path] = []
    cfg = _load_quality_config()
    for d in daterange(parse_date(start), parse_date(end)):
        outputs.append(_run_for_date(root, d, cfg, force))
    return outputs


def _run_for_date(root: Path, dt: date, cfg: Dict, force: bool) -> Path:
    con = duckdb.connect()
    results: List[Dict[str, object]] = []

    # Uniqueness checks
    for rule in cfg.get("uniqueness", []):
        table = rule["table"].split(".")[-1]
        cols = rule["columns"]
        path = root / "gold" / table / f"dt={dt.isoformat()}"
        if not path.exists():
            continue
        con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{path}/*.parquet')")
        cols_sql = ", ".join(cols)
        dupes = con.execute(
            f"SELECT COUNT(*) FROM (SELECT {cols_sql}, COUNT(*) c FROM t GROUP BY {cols_sql} HAVING c>1)"
        ).fetchone()[0]
        results.append(
            {"check": "uniqueness", "table": table, "columns": cols_sql, "dupes": dupes, "dt": dt.isoformat()}
        )

    # Referential integrity
    for rule in cfg.get("referential_integrity", []):
        child = rule["child"].split(".")[-1]
        parent = rule["parent"].split(".")[-1]
        key = rule["key"]
        child_path = root / "gold" / child / f"dt={dt.isoformat()}"
        parent_path = root / "gold" / parent / f"dt={dt.isoformat()}"
        if not child_path.exists() or not parent_path.exists():
            continue
        con.execute(f"CREATE OR REPLACE VIEW c AS SELECT * FROM read_parquet('{child_path}/*.parquet')")
        con.execute(f"CREATE OR REPLACE VIEW p AS SELECT * FROM read_parquet('{parent_path}/*.parquet')")
        missing = con.execute(
            f"SELECT COUNT(*) FROM c LEFT JOIN p ON c.{key}=p.{key} WHERE c.{key} IS NOT NULL AND p.{key} IS NULL"
        ).fetchone()[0]
        results.append(
            {
                "check": "referential_integrity",
                "child": child,
                "parent": parent,
                "key": key,
                "missing": missing,
                "dt": dt.isoformat(),
            }
        )

    out_dir = root / "gold" / "audit_quality" / f"dt={dt.isoformat()}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if out_path.exists() and not force:
        return out_path
    _write_parquet(results, out_path)
    return out_path


def _load_quality_config() -> Dict:
    path = Path("configs/quality.yaml")
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f).get("quality", {})


def _write_parquet(rows: List[Dict[str, object]], path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not rows:
        rows = [{"empty": True}]
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, use_dictionary=False)
