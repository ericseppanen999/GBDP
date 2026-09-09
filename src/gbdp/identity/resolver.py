from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List


from gbdp.identity.manual_overrides import load_manual_overrides
from gbdp.identity.rules import player_match_key, team_match_key
from gbdp.utils.ids import ulid_from_key
from gbdp.utils.io import (
    ensure_dir,
    gold_root,
    path_exists,
    read_parquet_rows,
    silver_root,
    spark_path,
    storage_format,
    write_parquet_table,
)
from gbdp.utils.time import daterange, parse_date
from gbdp.utils.logging import get_logger

logger = get_logger("gbdp.identity")


def resolve_identity(start: str, end: str, root: Path | None = None, force: bool = False) -> List[Path]:
    root = root or gold_root()
    outputs: List[Path] = []
    overrides = load_manual_overrides(Path("manual_entity_links.csv"))
    override_map = {
        (o["entity_type"], o["source"], o["source_id"]): o["canonical_id"] for o in overrides
    }
    days = list(daterange(parse_date(start), parse_date(end)))
    for i, d in enumerate(days, 1):
        logger.info("identity_resolve dt=%s (%d/%d)", d.isoformat(), i, len(days))
        outputs.append(_resolve_for_date(root, d, override_map, force))
        _write_merge_events(root, d, overrides, force)
    return outputs


def _resolve_for_date(root: Path, dt: date, overrides: Dict[tuple, str], force: bool) -> Path:
    records: List[Dict[str, Any]] = []
    records.extend(_collect_player_sources(root, dt))
    records.extend(_collect_team_sources(root, dt))

    bridge_rows: List[Dict[str, Any]] = []
    for r in records:
        entity_type = r["entity_type"]
        source = r["source"]
        source_id = r["source_id"]
        override = overrides.get((entity_type, source, source_id))
        if entity_type == "player":
            match = player_match_key(
                {
                    "source": source,
                    "source_player_id": source_id,
                    "name": r.get("name"),
                    "dob": r.get("dob"),
                    "team_id": r.get("team_id"),
                }
            )
        else:
            match = team_match_key(
                {
                    "source": source,
                    "source_team_id": source_id,
                    "name": r.get("name"),
                    "league_code": r.get("league_code"),
                }
            )
        canonical_key = match.canonical_key if match else f"{entity_type}:{source}:{source_id}"
        canonical_id = override or ulid_from_key(canonical_key, dt.isoformat())
        bridge_rows.append(
            {
                "entity_type": entity_type,
                "source": source,
                "source_id": source_id,
                "canonical_id": canonical_id,
                "first_seen_dt": dt.isoformat(),
                "last_seen_dt": dt.isoformat(),
                "confidence": match.confidence if match else 0.5,
                "match_method": match.method if match else "unknown",
                "manual_override": bool(override),
                "notes": None,
                "dt": dt.isoformat(),
            }
        )

    out_dir = root / "bridge_source_ids" / f"dt={dt.isoformat()}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if path_exists(out_path) and not force:
        return out_path
    _write_output(bridge_rows, out_dir, out_path)
    return out_path


def _collect_player_sources(root: Path, dt: date) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    # NPB rosters silver
    npb_rosters = _read_silver(silver_root(), "npb_spaia", "rosters", dt)
    for r in npb_rosters:
        source_id = r.get("player_id")
        if not source_id:
            continue
        rows.append(
            {
                "entity_type": "player",
                "source": "npb_spaia",
                "source_id": str(source_id),
                "name": r.get("player_name"),
                "dob": r.get("dob"),
                "team_id": r.get("team_id"),
                "league_code": "NPB",
            }
        )
    # MLB rosters silver
    mlb_rosters = _read_silver(silver_root(), "mlb_statsapi", "rosters", dt)
    for r in mlb_rosters:
        source_id = r.get("player_id")
        if not source_id:
            continue
        rows.append(
            {
                "entity_type": "player",
                "source": "mlb_statsapi",
                "source_id": str(source_id),
                "name": r.get("player_name"),
                "dob": r.get("dob"),
                "team_id": r.get("team_id"),
                "league_code": "MLB",
            }
        )
    # Indy rosters silver
    indy_rosters = _read_silver(silver_root(), "indy_local", "rosters", dt)
    for r in indy_rosters:
        source_id = r.get("player_id")
        if not source_id:
            continue
        rows.append(
            {
                "entity_type": "player",
                "source": "indy_local",
                "source_id": str(source_id),
                "name": r.get("player_name") or r.get("name"),
                "dob": r.get("dob"),
                "team_id": r.get("team_id"),
                "league_code": "INDY",
            }
        )
    # Retrosheet rosters silver
    retro_rosters = _read_silver(silver_root(), "retrosheet_local", "rosters", dt)
    for r in retro_rosters:
        source_id = r.get("player_id")
        if not source_id:
            continue
        rows.append(
            {
                "entity_type": "player",
                "source": "retrosheet_local",
                "source_id": str(source_id),
                "name": r.get("player_name"),
                "dob": None,
                "team_id": r.get("team_id"),
                "league_code": "MLB",
            }
        )
    # KBO rosters silver
    kbo_rosters = _read_silver(silver_root(), "kbo_local", "rosters", dt)
    for r in kbo_rosters:
        source_id = r.get("player_id")
        if not source_id:
            continue
        rows.append(
            {
                "entity_type": "player",
                "source": "kbo_local",
                "source_id": str(source_id),
                "name": r.get("player_name") or r.get("name"),
                "dob": r.get("dob"),
                "team_id": r.get("team_id"),
                "league_code": "KBO",
            }
        )
    # LMB rosters silver
    lmb_rosters = _read_silver(silver_root(), "lmb_local", "rosters", dt)
    for r in lmb_rosters:
        source_id = r.get("player_id")
        if not source_id:
            continue
        rows.append(
            {
                "entity_type": "player",
                "source": "lmb_local",
                "source_id": str(source_id),
                "name": r.get("player_name") or r.get("name"),
                "dob": r.get("dob"),
                "team_id": r.get("team_id"),
                "league_code": "LMB",
            }
        )
    return rows


def _collect_team_sources(root: Path, dt: date) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    npb_games = _read_silver(silver_root(), "npb_spaia", "games", dt)
    for r in npb_games:
        for team_id, team_name in [
            (r.get("home_team_id"), r.get("home_team_name")),
            (r.get("away_team_id"), r.get("away_team_name")),
        ]:
            if team_id:
                rows.append(
                    {
                        "entity_type": "team",
                        "source": "npb_spaia",
                        "source_id": str(team_id),
                        "name": team_name,
                        "league_code": "NPB",
                    }
                )
    mlb_games = _read_silver(silver_root(), "mlb_statsapi", "games", dt)
    for r in mlb_games:
        for team_id, team_name in [
            (r.get("home_team_id"), r.get("home_team_name")),
            (r.get("away_team_id"), r.get("away_team_name")),
        ]:
            if team_id:
                rows.append(
                    {
                        "entity_type": "team",
                        "source": "mlb_statsapi",
                        "source_id": str(team_id),
                        "name": team_name,
                        "league_code": "MLB",
                    }
                )
    indy_games = _read_silver(silver_root(), "indy_local", "games", dt)
    for r in indy_games:
        for team_id, team_name in [
            (r.get("home_team_id"), r.get("home_team_name")),
            (r.get("away_team_id"), r.get("away_team_name")),
        ]:
            if team_id:
                rows.append(
                    {
                        "entity_type": "team",
                        "source": "indy_local",
                        "source_id": str(team_id),
                        "name": team_name,
                        "league_code": "INDY",
                    }
                )
    retro_games = _read_silver(silver_root(), "retrosheet_local", "games", dt)
    for r in retro_games:
        for team_id, team_name in [
            (r.get("home_team_id"), r.get("home_team_id")),
            (r.get("away_team_id"), r.get("away_team_id")),
        ]:
            if team_id:
                rows.append(
                    {
                        "entity_type": "team",
                        "source": "retrosheet_local",
                        "source_id": str(team_id),
                        "name": team_name,
                        "league_code": "MLB",
                    }
                )
    kbo_games = _read_silver(silver_root(), "kbo_local", "games", dt)
    for r in kbo_games:
        for team_id, team_name in [
            (r.get("home_team_id"), r.get("home_team_name")),
            (r.get("away_team_id"), r.get("away_team_name")),
        ]:
            if team_id:
                rows.append(
                    {
                        "entity_type": "team",
                        "source": "kbo_local",
                        "source_id": str(team_id),
                        "name": team_name,
                        "league_code": "KBO",
                    }
                )
    lmb_games = _read_silver(silver_root(), "lmb_local", "games", dt)
    for r in lmb_games:
        for team_id, team_name in [
            (r.get("home_team_id"), r.get("home_team_name")),
            (r.get("away_team_id"), r.get("away_team_name")),
        ]:
            if team_id:
                rows.append(
                    {
                        "entity_type": "team",
                        "source": "lmb_local",
                        "source_id": str(team_id),
                        "name": team_name,
                        "league_code": "LMB",
                    }
                )
    return rows


def _read_silver(root: Path, source: str, entity: str, dt: date) -> List[Dict[str, Any]]:
    path = root / source / entity / f"dt={dt.isoformat()}"
    if not path_exists(path):
        return []
    # Delta if _delta_log present, otherwise parquet (read_parquet_rows handles DBFS safely)
    if path_exists(path / "_delta_log"):
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            rows = spark.read.format("delta").load(spark_path(path)).toPandas().to_dict(orient="records")
        except Exception:
            return []
    else:
        try:
            rows = read_parquet_rows(path)
        except Exception:
            return []
    # Writers emit a single {"empty": True} sentinel row instead of an empty
    # file; callers here expect real records, so drop the sentinel.
    return [r for r in rows if not r.get("empty")]


def _write_output(rows: List[Dict[str, Any]], out_dir: Path, out_path: Path) -> None:
    fmt = storage_format()
    if fmt == "parquet":
        if not rows:
            rows = [{"empty": True}]
        import pyarrow as pa
        table = pa.Table.from_pylist(rows)
        write_parquet_table(table, out_path, force=True)
        return
    if not rows:
        return
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError("pyspark is required for delta writes") from exc
    spark = SparkSession.builder.getOrCreate()
    # Replace all-None columns with "" before createDataFrame — Spark can't infer NullType
    null_cols = {k for k in rows[0] if all(r.get(k) is None for r in rows)}
    if null_cols:
        rows = [{k: ("" if k in null_cols else v) for k, v in r.items()} for r in rows]
    df = spark.createDataFrame(rows)
    # Cast any remaining NullType columns to string
    from pyspark.sql import functions as F
    from pyspark.sql.types import NullType
    for field in df.schema.fields:
        if isinstance(field.dataType, NullType):
            df = df.withColumn(field.name, F.lit(None).cast("string"))

    # out_dir is a dt= partition folder; write to the table root with partitionBy("dt")
    if out_dir.name.startswith("dt="):
        table_root = spark_path(out_dir.parent)
        dt_value = out_dir.name.split("=", 1)[1]
        try:
            spark.read.format("delta").load(table_root).limit(1).collect()
            table_exists = True
        except Exception:
            table_exists = False

        def _fresh_write():
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("dt")
                .save(table_root)
            )

        try:
            if table_exists:
                (
                    df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .option("replaceWhere", f"dt = '{dt_value}'")
                    .save(table_root)
                )
            else:
                _fresh_write()
        except Exception as exc:
            msg = str(exc)
            if "DELTA_MISSING_TRANSACTION_LOG" in msg or "Incompatible format detected" in msg:
                # Stale per-partition delta logs are blocking the root-level write — clear and retry
                try:
                    from pyspark.dbutils import DBUtils
                    DBUtils(spark).fs.rm(table_root, True)
                except Exception:
                    pass
                _fresh_write()
            else:
                raise
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(spark_path(out_dir))


def _write_merge_events(root: Path, dt: date, overrides: List[Dict[str, str]], force: bool) -> None:
    out_dir = root / "merge_events" / f"dt={dt.isoformat()}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if path_exists(out_path) and not force:
        return
    rows: List[Dict[str, Any]] = []
    for o in overrides:
        rows.append(
            {
                "entity_type": o.get("entity_type"),
                "source": o.get("source"),
                "source_id": o.get("source_id"),
                "canonical_id": o.get("canonical_id"),
                "match_method": "manual_override",
                "notes": o.get("notes"),
                "dt": dt.isoformat(),
            }
        )
    _write_output(rows, out_dir, out_path)
