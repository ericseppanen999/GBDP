from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict, List

import yaml

from gbdp.utils.io import data_root, ensure_dir, storage_format
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
    fmt = storage_format()
    con = None
    if fmt == "parquet":
        try:
            import duckdb
        except Exception as exc:
            raise RuntimeError("duckdb is required for parquet quality checks") from exc
        con = duckdb.connect()
    results: List[Dict[str, object]] = []

    # Uniqueness checks
    for rule in cfg.get("uniqueness", []):
        table = rule["table"].split(".")[-1]
        cols = rule["columns"]
        path = root / "gold" / table / f"dt={dt.isoformat()}"
        if not path.exists():
            continue
        cols_sql = ", ".join(cols)
        if fmt == "parquet":
            con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{path}/*.parquet')")
            dupes = con.execute(
                f"SELECT COUNT(*) FROM (SELECT {cols_sql}, COUNT(*) c FROM t GROUP BY {cols_sql} HAVING c>1)"
            ).fetchone()[0]
        else:
            dupes = _spark_uniqueness(path, cols)
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
        if fmt == "parquet":
            con.execute(f"CREATE OR REPLACE VIEW c AS SELECT * FROM read_parquet('{child_path}/*.parquet')")
            con.execute(f"CREATE OR REPLACE VIEW p AS SELECT * FROM read_parquet('{parent_path}/*.parquet')")
            missing = con.execute(
                f"SELECT COUNT(*) FROM c LEFT JOIN p ON c.{key}=p.{key} WHERE c.{key} IS NOT NULL AND p.{key} IS NULL"
            ).fetchone()[0]
        else:
            missing = _spark_ref_integrity(child_path, parent_path, key)
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

    # Row count sanity
    for rule in cfg.get("row_count_min", []):
        table = rule["table"].split(".")[-1]
        min_count = int(rule.get("min_count", 0))
        path = root / "gold" / table / f"dt={dt.isoformat()}"
        if not path.exists():
            continue
        if fmt == "parquet":
            con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{path}/*.parquet')")
            count = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        else:
            count = _spark_count(path)
        results.append(
            {
                "check": "row_count_min",
                "table": table,
                "count": count,
                "min_count": min_count,
                "ok": count >= min_count,
                "dt": dt.isoformat(),
            }
        )

    # Completeness thresholds (coverage of games)
    thresholds = cfg.get("thresholds", {})
    if thresholds:
        game_path = root / "gold" / "fact_game" / f"dt={dt.isoformat()}"
        if game_path.exists():
            if fmt == "parquet":
                con.execute(
                    f"CREATE OR REPLACE VIEW games AS SELECT DISTINCT game_id FROM read_parquet('{game_path}/*.parquet')"
                )
                total_games = con.execute("SELECT COUNT(*) FROM games").fetchone()[0]
            else:
                total_games = _spark_distinct_count(game_path, "game_id")
            pitch_path = root / "gold" / "fact_pitch" / f"dt={dt.isoformat()}"
            if pitch_path.exists() and total_games > 0:
                if fmt == "parquet":
                    con.execute(
                        f"CREATE OR REPLACE VIEW pitches AS SELECT DISTINCT game_id FROM read_parquet('{pitch_path}/*.parquet')"
                    )
                    with_pitch = con.execute("SELECT COUNT(*) FROM pitches").fetchone()[0]
                else:
                    with_pitch = _spark_distinct_count(pitch_path, "game_id")
                pct = with_pitch / total_games
                results.append(
                    {
                        "check": "pitch_coverage",
                        "pct": pct,
                        "threshold": float(thresholds.get("mlb_pitch_coverage_pct", 0)),
                        "ok": pct >= float(thresholds.get("mlb_pitch_coverage_pct", 0)),
                        "dt": dt.isoformat(),
                    }
                )

    # Schema drift detection for bronze parsed
    results.extend(_schema_drift_checks(root, dt))

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


def _schema_drift_checks(root: Path, dt: date) -> List[Dict[str, object]]:
    import json
    import pyarrow.parquet as pq

    results: List[Dict[str, object]] = []
    bronze_root = root / "bronze" / "parsed"
    if not bronze_root.exists():
        return results
    for source_dir in bronze_root.iterdir():
        if not source_dir.is_dir():
            continue
        for entity_dir in source_dir.iterdir():
            if not entity_dir.is_dir():
                continue
            part_dir = entity_dir / f"dt={dt.isoformat()}"
            if not part_dir.exists():
                continue
            files = list(part_dir.glob("*.parquet"))
            if not files:
                continue
            schema = pq.read_schema(files[0])
            fields = {name: str(schema.field(name).type) for name in schema.names}
            audit_dir = root / "gold" / "audit_schema" / source_dir.name / entity_dir.name
            ensure_dir(audit_dir)
            current_path = audit_dir / f"dt={dt.isoformat()}.json"
            prev = None
            prev_files = sorted(audit_dir.glob("dt=*.json"))
            if prev_files:
                prev_path = prev_files[-1]
                try:
                    prev = json.loads(prev_path.read_text(encoding="utf-8"))
                except Exception:
                    prev = None
            current_path.write_text(json.dumps(fields, ensure_ascii=True, sort_keys=True), encoding="utf-8")
            if prev and prev != fields:
                results.append(
                    {
                        "check": "schema_drift",
                        "source": source_dir.name,
                        "entity": entity_dir.name,
                        "dt": dt.isoformat(),
                        "changed": True,
                    }
                )
    return results


def _spark_count(path: Path) -> int:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    return spark.read.format("delta").load(str(path)).count()


def _spark_distinct_count(path: Path, col: str) -> int:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    return spark.read.format("delta").load(str(path)).select(col).distinct().count()


def _spark_uniqueness(path: Path, cols: List[str]) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("delta").load(str(path))
    dupes = df.groupBy(cols).count().where(F.col("count") > 1).count()
    return dupes


def _spark_ref_integrity(child: Path, parent: Path, key: str) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    c = spark.read.format("delta").load(str(child))
    p = spark.read.format("delta").load(str(parent))
    missing = c.join(p, c[key] == p[key], "left").where(c[key].isNotNull() & p[key].isNull()).count()
    return missing
