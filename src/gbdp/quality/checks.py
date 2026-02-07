from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict, List

import yaml

from gbdp.utils.io import (
    data_root,
    ensure_dir,
    list_dir,
    path_exists,
    read_text,
    spark_path,
    storage_format,
    write_parquet_table,
    write_text,
)
from gbdp.utils.time import daterange, parse_date


def run_quality_checks(
    start: str, end: str, root: Path | None = None, force: bool = False
) -> List[Path]:
    from gbdp.utils.io import gold_root
    root = root or gold_root()
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
        path = root / table / f"dt={dt.isoformat()}"
        if not path_exists(path):
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
        child_path = root / child / f"dt={dt.isoformat()}"
        parent_path = root / parent / f"dt={dt.isoformat()}"
        if not path_exists(child_path) or not path_exists(parent_path):
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
        path = root / table / f"dt={dt.isoformat()}"
        if not path_exists(path):
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
        game_path = root / "fact_game" / f"dt={dt.isoformat()}"
        if path_exists(game_path):
            if fmt == "parquet":
                con.execute(
                    f"CREATE OR REPLACE VIEW games AS SELECT DISTINCT game_id FROM read_parquet('{game_path}/*.parquet')"
                )
                total_games = con.execute("SELECT COUNT(*) FROM games").fetchone()[0]
            else:
                total_games = _spark_distinct_count(game_path, "game_id")
            pitch_path = root / "fact_pitch" / f"dt={dt.isoformat()}"
            if path_exists(pitch_path) and total_games > 0:
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
            # NPB PBP coverage
            pa_path = root / "gold" / "fact_plate_appearance" / f"dt={dt.isoformat()}"
            if path_exists(pa_path) and thresholds.get("npb_pbp_coverage_pct") is not None:
                if fmt == "parquet":
                    con.execute(
                        f"CREATE OR REPLACE VIEW games_npb AS SELECT DISTINCT game_id FROM read_parquet('{game_path}/*.parquet') WHERE source='npb_spaia'"
                    )
                    total_npb = con.execute("SELECT COUNT(*) FROM games_npb").fetchone()[0]
                    con.execute(
                        f"CREATE OR REPLACE VIEW pa_npb AS SELECT DISTINCT game_id FROM read_parquet('{pa_path}/*.parquet') WHERE source='npb_spaia'"
                    )
                    with_pbp = con.execute("SELECT COUNT(*) FROM pa_npb").fetchone()[0]
                else:
                    from pyspark.sql import SparkSession
                    from pyspark.sql import functions as F

                    spark = SparkSession.builder.getOrCreate()
                    games_df = spark.read.format("delta").load(spark_path(game_path)).where("source = 'npb_spaia'")
                    total_npb = games_df.select("game_id").distinct().count()
                    pa_df = spark.read.format("delta").load(spark_path(pa_path)).where("source = 'npb_spaia'")
                    with_pbp = pa_df.select("game_id").distinct().count()
                pct_npb = (with_pbp / total_npb) if total_npb else 0
                results.append(
                    {
                        "check": "npb_pbp_coverage",
                        "pct": pct_npb,
                        "threshold": float(thresholds.get("npb_pbp_coverage_pct", 0)),
                        "ok": pct_npb >= float(thresholds.get("npb_pbp_coverage_pct", 0)),
                        "dt": dt.isoformat(),
                    }
                )

    # Base/out validity checks
    pa_path = root / "fact_plate_appearance" / f"dt={dt.isoformat()}"
    if path_exists(pa_path):
        if fmt == "parquet":
            con.execute(f"CREATE OR REPLACE VIEW pa AS SELECT * FROM read_parquet('{pa_path}/*.parquet')")
            invalid_base = con.execute(
                "SELECT COUNT(*) FROM pa WHERE (base_state_before IS NOT NULL AND (base_state_before < 0 OR base_state_before > 7)) "
                "OR (base_state_after IS NOT NULL AND (base_state_after < 0 OR base_state_after > 7))"
            ).fetchone()[0]
            invalid_outs = con.execute(
                "SELECT COUNT(*) FROM pa WHERE (outs_before IS NOT NULL AND (outs_before < 0 OR outs_before > 3)) "
                "OR (outs_after IS NOT NULL AND (outs_after < 0 OR outs_after > 3))"
            ).fetchone()[0]
        else:
            invalid_base = _spark_invalid_range(pa_path, "base_state_before", 0, 7) + _spark_invalid_range(
                pa_path, "base_state_after", 0, 7
            )
            invalid_outs = _spark_invalid_range(pa_path, "outs_before", 0, 3) + _spark_invalid_range(
                pa_path, "outs_after", 0, 3
            )
        results.append(
            {"check": "base_state_range", "invalid": invalid_base, "dt": dt.isoformat(), "ok": invalid_base == 0}
        )
        results.append(
            {"check": "outs_range", "invalid": invalid_outs, "dt": dt.isoformat(), "ok": invalid_outs == 0}
        )

    # Schema drift detection for bronze parsed
    results.extend(_schema_drift_checks(root, dt))

    out_dir = root / "audit_quality" / f"dt={dt.isoformat()}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if path_exists(out_path) and not force:
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
    if not rows:
        rows = [{"empty": True}]
    import pyarrow as pa
    table = pa.Table.from_pylist(rows)
    write_parquet_table(table, path, force=True)


def _schema_drift_checks(root: Path, dt: date) -> List[Dict[str, object]]:
    import json
    import pyarrow.parquet as pq

    results: List[Dict[str, object]] = []
    from gbdp.utils.io import bronze_root as _bronze_root
    bronze_root = _bronze_root() / "parsed"
    if not path_exists(bronze_root):
        return results
    for source_dir in list_dir(bronze_root, dirs_only=True):
        for entity_dir in list_dir(source_dir, dirs_only=True):
            part_dir = entity_dir / f"dt={dt.isoformat()}"
            if not path_exists(part_dir):
                continue
            try:
                files = list(part_dir.glob("*.parquet"))
            except Exception:
                continue
            if not files:
                continue
            schema = pq.read_schema(files[0])
            fields = {name: str(schema.field(name).type) for name in schema.names}
            audit_dir = root / "audit_schema" / source_dir.name / entity_dir.name
            ensure_dir(audit_dir)
            current_path = audit_dir / f"dt={dt.isoformat()}.json"
            prev = None
            try:
                prev_files = sorted(audit_dir.glob("dt=*.json"))
            except Exception:
                prev_files = []
            if prev_files:
                prev_path = prev_files[-1]
                try:
                    prev = json.loads(read_text(prev_path, encoding="utf-8"))
                except Exception:
                    prev = None
            write_text(
                current_path,
                json.dumps(fields, ensure_ascii=True, sort_keys=True),
                encoding="utf-8",
                force=True,
            )
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
    return spark.read.format("delta").load(spark_path(path)).count()


def _spark_distinct_count(path: Path, col: str) -> int:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    return spark.read.format("delta").load(spark_path(path)).select(col).distinct().count()


def _spark_uniqueness(path: Path, cols: List[str]) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("delta").load(spark_path(path))
    dupes = df.groupBy(cols).count().where(F.col("count") > 1).count()
    return dupes


def _spark_ref_integrity(child: Path, parent: Path, key: str) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    c = spark.read.format("delta").load(spark_path(child))
    p = spark.read.format("delta").load(spark_path(parent))
    missing = c.join(p, c[key] == p[key], "left").where(c[key].isNotNull() & p[key].isNull()).count()
    return missing


def _spark_invalid_range(path: Path, col: str, min_val: int, max_val: int) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("delta").load(spark_path(path))
    return (
        df.where(F.col(col).isNotNull() & ((F.col(col) < min_val) | (F.col(col) > max_val)))
        .count()
    )
