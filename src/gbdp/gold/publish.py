from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from gbdp.utils.ids import ulid_from_key
from gbdp.utils.io import (
    ensure_dir,
    gold_root,
    list_dir,
    path_exists,
    read_parquet_rows,
    silver_root,
    spark_path,
    stable_json_dumps,
    storage_format,
    write_parquet_table,
    has_files_with_suffix,
)
from gbdp.utils.time import utc_now
from gbdp.utils.time import daterange, parse_date
from gbdp.utils.logging import get_logger
import time

logger = get_logger("gbdp.gold")


# -----------------------------
# Delta helpers (fixes NullType + dt=... delta-log issues)
# -----------------------------

# Gold tables are written incrementally, one dt at a time, into ONE Delta
# table via replaceWhere + mergeSchema. If a field's TYPE is inferred purely
# from that single write's batch of rows, a date where every source happens
# to leave the field null (e.g. kbo_api boxscore rows never populate hr/bb/
# so; a date with no MLB boxscore data yet) infers as STRING (the all-null
# fallback), while a date with real data infers as LONG/DOUBLE -- and Delta
# legitimately refuses to merge those ("Failed to merge fields 'x' and 'x'"
# -- confirmed live on fact_boxscore_batting.2b during a real backfill: kbo
# rows are always-null for it, and register_uc failed exactly this way).
# Pin these fields to one type on every write, whatever a given batch holds, so
# an all-null or oddly-typed batch can't drift and poison schema merge for every
# later write. ip/pct/gb are text ("6.1", ".500", "-"), not numbers.
_KNOWN_FIELD_TYPES: Dict[str, str] = {
    # fact_boxscore_batting / fact_boxscore_pitching
    "ab": "long", "h": "long", "2b": "long", "3b": "long", "hr": "long",
    "bb": "long", "so": "long", "rbi": "long", "r": "long", "er": "long",
    "ip": "string",
    # fact_pitch
    "pitch_number_in_pa": "long", "balls_before": "long", "strikes_before": "long",
    "release_speed": "double", "plate_x": "double", "plate_z": "double",
    # fact_plate_appearance
    "inning": "long", "outs_on_play": "long", "base_state_before": "long", "outs_before": "long",
    "base_state_after": "long", "outs_after": "long", "runs_scored_on_play": "long",
    # fact_game
    "home_score": "long", "away_score": "long",
    # dim_player
    "height_cm": "double", "weight_kg": "double",
    # fact_standings
    "w": "long", "l": "long", "t": "long", "pct": "string", "gb": "string",
}


def _coerce_known_type(value: Any, type_name: str) -> Any:
    if value is None:
        return None
    if type_name == "string":
        return str(value)
    try:
        number = float(value)
        return int(number) if type_name == "long" else number
    except (TypeError, ValueError, OverflowError):
        return None


def _infer_type(values: List[Any], field_name: str | None = None) -> str:
    """
    Conservative type inference to avoid Spark conversion errors:
    - known field_name -> its pinned type, whatever the values are
    - if any string-like appears -> string
    - else if any float -> double
    - else if any int -> long
    - else if any bool -> boolean
    - else if any bytes -> binary
    - else -> string (all null)
    """
    if field_name in _KNOWN_FIELD_TYPES:
        return _KNOWN_FIELD_TYPES[field_name]

    has_str = False
    has_float = False
    has_int = False
    has_bool = False
    has_bin = False

    for v in values:
        if v is None:
            continue
        if isinstance(v, (dict, list)):
            has_str = True
            continue
        if isinstance(v, str):
            has_str = True
            continue
        if isinstance(v, bool):
            has_bool = True
            continue
        if isinstance(v, int) and not isinstance(v, bool):
            has_int = True
            continue
        if isinstance(v, float):
            has_float = True
            continue
        if isinstance(v, (bytes, bytearray)):
            has_bin = True
            continue
        # unknown types -> stringify
        has_str = True

    if has_str:
        return "string"
    if has_float:
        return "double"
    if has_int:
        return "long"
    if has_bool:
        return "boolean"
    if has_bin:
        return "binary"
    return "string"


def _rows_to_df(spark, rows: List[Dict[str, Any]]):
    """
    Deterministic DataFrame creation:
    - dict/list -> JSON string
    - all-null cols -> STRING (prevents CANNOT_DETERMINE_TYPE)
    - mixed types -> STRING (prevents cast failures)
    """
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        LongType,
        DoubleType,
        BooleanType,
        BinaryType,
    )

    def dtype(name: str):
        return {
            "string": StringType(),
            "long": LongType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "binary": BinaryType(),
        }.get(name, StringType())

    if not rows:
        schema = StructType([StructField("empty", BooleanType(), True)])
        return spark.createDataFrame([], schema=schema)

    norm: List[Dict[str, Any]] = []
    for r in rows:
        nr: Dict[str, Any] = {}
        for k, v in r.items():
            if isinstance(v, (dict, list)):
                nr[k] = json.dumps(v, separators=(",", ":"), ensure_ascii=False)
            elif k in _KNOWN_FIELD_TYPES:
                nr[k] = _coerce_known_type(v, _KNOWN_FIELD_TYPES[k])
            else:
                nr[k] = v
        norm.append(nr)

    keys = sorted({k for r in norm for k in r.keys()})
    fields = []
    for k in keys:
        vals = [r.get(k) for r in norm]
        fields.append(StructField(k, dtype(_infer_type(vals, field_name=k)), True))

    schema = StructType(fields)
    return spark.createDataFrame(norm, schema=schema)


def _delta_exists(spark, table_root: str) -> bool:
    try:
        spark.read.format("delta").load(table_root).limit(1).collect()
        return True
    except Exception:
        return False


def _write_delta(rows: List[Dict[str, Any]], table_root_dir: Path, dt_value: Optional[str] = None) -> None:
    """
    Write ONE delta table at table_root_dir (UC-friendly), partitioned by dt.
    Overwrite only dt partition when dt_value is provided (replaceWhere).
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    if not rows:
        return

    if dt_value is not None:
        for r in rows:
            r["dt"] = dt_value

    df = _rows_to_df(spark, rows)
    table_root = spark_path(table_root_dir)

    if not _delta_exists(spark, table_root):
        try:
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("dt")
                .save(table_root)
            )
            return
        except Exception as exc:
            msg = str(exc)
            if "DELTA_MISSING_TRANSACTION_LOG" in msg or "Incompatible format detected" in msg:
                df.write.mode("overwrite").parquet(spark_path(table_root_dir))
                return
            raise

    w = (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
    )
    if dt_value is not None:
        w = w.option("replaceWhere", f"dt = '{dt_value}'")
    try:
        w.save(table_root)
    except Exception as exc:
        msg = str(exc)
        if "DELTA_MISSING_TRANSACTION_LOG" in msg or "Incompatible format detected" in msg:
            df.write.mode("overwrite").parquet(spark_path(table_root_dir))
            return
        # Never retry a merge failure as an unscoped overwrite: that replaces the
        # ENTIRE table with this one date. It erased all MLB rows from
        # fact_plate_appearance in production (Delta versions 136, 142-144).
        raise


# -----------------------------
# Public API
# -----------------------------

def publish_gold(start: str, end: str, root: Path | None = None, force: bool = False) -> List[Path]:
    root = root or gold_root()
    outputs: List[Path] = []
    days = list(daterange(parse_date(start), parse_date(end)))
    for i, d in enumerate(days, 1):
        t0 = time.monotonic()
        logger.info("gold_publish dt=%s (%d/%d)", d.isoformat(), i, len(days))
        outputs.extend(_publish_for_date(root, d, force))
        logger.info("gold_publish dt=%s done (%.1fs)", d.isoformat(), time.monotonic() - t0)
    return outputs


def _publish_for_date(root: Path, dt: date, force: bool) -> List[Path]:
    bridge = _read_gold_bridge(root, dt)

    steps: List[tuple] = [
        ("dim_league", lambda: _write_dim_league(root, dt, force)),
        ("dim_team", lambda: _write_dim_team(root, dt, bridge, force)),
        ("dim_player", lambda: _write_dim_player(root, dt, bridge, force)),
        ("dim_season", lambda: _write_dim_season(root, dt, force)),
        ("fact_game", lambda: _write_fact_game(root, dt, bridge, force)),
        ("fact_roster", lambda: _write_fact_roster(root, dt, bridge, force)),
        ("fact_transaction", lambda: _write_fact_transaction(root, dt, bridge, force)),
        ("fact_pitch", lambda: _write_fact_pitch(root, dt, bridge, force)),
        ("fact_plate_appearance", lambda: _write_fact_plate_appearance(root, dt, bridge, force)),
        ("fact_standings", lambda: _write_fact_standings(root, dt, bridge, force)),
        ("fact_boxscore_batting", lambda: _write_fact_boxscore_batting(root, dt, bridge, force)),
        ("fact_boxscore_pitching", lambda: _write_fact_boxscore_pitching(root, dt, bridge, force)),
        ("run_expectancy", lambda: _write_run_expectancy(root, dt, force)),
        ("breakout_candidates", lambda: _write_breakout_candidates(root, dt, force)),
        ("feature_player_rolling_30d", lambda: _write_feature_player_rolling_30d(root, dt, force)),
        ("fact_contract", lambda: _write_fact_contract(root, dt, force)),
    ]
    outputs: List[Path] = []
    for name, fn in steps:
        t0 = time.monotonic()
        outputs.append(fn())
        logger.info("  gold table=%s dt=%s (%.1fs)", name, dt.isoformat(), time.monotonic() - t0)
    return outputs


# -----------------------------
# Reads
# -----------------------------

def _read_gold_bridge(root: Path, dt: date) -> Dict[tuple, str]:
    """
    Parquet mode: reads root/bridge_source_ids/dt=YYYY-MM-DD
    Delta mode: reads root/bridge_source_ids (delta root) and filters dt
    """
    dt_str = dt.isoformat()
    fmt = storage_format()

    if fmt == "parquet":
        path = root / "bridge_source_ids" / f"dt={dt_str}"
        if not path_exists(path):
            return {}
        data = read_parquet_rows(path)

    elif fmt == "delta":
        base = root / "bridge_source_ids"
        if not path_exists(base):
            return {}
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        dt_path = base / f"dt={dt_str}"
        dt_delta_log = dt_path / "_delta_log"
        try:
            # Always prefer root-level partitioned table first
            df = spark.read.format("delta").load(spark_path(base)).where(f"dt = '{dt_str}'")
            data = [row.asDict() for row in df.collect()]
        except Exception:
            try:
                # Legacy: partition folder was itself a delta table
                if path_exists(dt_delta_log):
                    df = spark.read.format("delta").load(spark_path(dt_path))
                    data = [row.asDict() for row in df.collect()]
                else:
                    raise
            except Exception:
                # Last resort: legacy parquet partition folder
                legacy = root / "bridge_source_ids" / f"dt={dt_str}"
                if not path_exists(legacy) or not has_files_with_suffix(legacy, ".parquet"):
                    return {}
                data = read_parquet_rows(legacy)

    else:
        return {}

    out: Dict[tuple, str] = {}
    for r in data:
        entity_type = r.get("entity_type")
        source = r.get("source")
        source_id = r.get("source_id")
        canonical_id = r.get("canonical_id")
        if not (entity_type and source and source_id and canonical_id):
            continue
        out[(entity_type, source, source_id)] = canonical_id
    return out


def _read_silver(root: Path, source: str, entity: str, dt: date) -> List[Dict[str, Any]]:
    # _write_parquet-style writers emit a single {"empty": True} sentinel row
    # instead of an empty file. Callers iterate this list expecting real
    # records, so filter the sentinel out here rather than at every call site.
    return [r for r in _read_silver_raw(root, source, entity, dt) if not r.get("empty")]


def _read_silver_raw(root: Path, source: str, entity: str, dt: date) -> List[Dict[str, Any]]:
    """
    Parquet mode: reads .../dt=YYYY-MM-DD folder.
    Delta mode: reads ONE delta table at .../<source>/<entity> and filters dt.
    Falls back to legacy parquet dt-folder if delta log missing.
    """
    dt_str = dt.isoformat()
    fmt = storage_format()

    if fmt == "parquet":
        path = silver_root() / source / entity / f"dt={dt_str}"
        if not path_exists(path):
            return []
        return read_parquet_rows(path)

    if fmt == "delta":
        base = silver_root() / source / entity
        if not path_exists(base):
            return []
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        try:
            df = spark.read.format("delta").load(spark_path(base)).where(f"dt = '{dt_str}'")
            return [row.asDict() for row in df.collect()]
        except Exception:
            legacy = silver_root() / source / entity / f"dt={dt_str}"
            if not path_exists(legacy) or not has_files_with_suffix(legacy, ".parquet"):
                return []
            df = spark.read.format("parquet").load(spark_path(legacy))
            return [row.asDict() for row in df.collect()]

    raise ValueError(f"Unsupported storage format: {fmt}")


def _read_gold_snapshot(root: Path, table: str, dt: date) -> List[Dict[str, Any]]:
    """
    Parquet mode: reads root/<table>/dt=YYYY-MM-DD folder.
    Delta mode: reads ONE delta table at root/<table> and filters dt.
    """
    dt_str = dt.isoformat()
    fmt = storage_format()

    if fmt == "parquet":
        path = root / table / f"dt={dt_str}"
        if not path_exists(path):
            return []
        return read_parquet_rows(path)

    if fmt == "delta":
        base = root / table
        if not path_exists(base):
            return []
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.format("delta").load(spark_path(base)).where(f"dt = '{dt_str}'")
        return [row.asDict() for row in df.collect()]

    raise ValueError(f"Unsupported storage format: {fmt}")


# -----------------------------
# Writes
# -----------------------------

def _write_gold_table(root: Path, table: str, dt: date, rows: List[Dict[str, Any]], force: bool) -> Path:
    """
    Parquet mode: per-day folder with parquet files (unchanged).
    Delta mode: ONE delta table at root/<table>, partitioned by dt (fixes UC + delta log errors).
    """
    fmt = storage_format()
    dt_str = dt.isoformat()
    now = utc_now().isoformat()

    for r in rows:
        r["dt"] = dt_str
        r.setdefault("ingested_at_utc", now)

    if fmt == "parquet":
        out_dir = root / table / f"dt={dt_str}"
        out_path = out_dir / "part-00001.parquet"
        if not rows:
            # A prior (buggy) run may have written real-looking rows to this
            # partition; a correct recompute now yielding zero rows should be
            # able to clear that out rather than leaving stale data forever.
            if force and path_exists(out_path):
                try:
                    out_path.unlink()
                except OSError as exc:
                    logger.warning("failed to clear empty gold partition %s: %s", out_path, exc)
            return out_path
        ensure_dir(out_dir)
        if path_exists(out_path) and not force:
            return out_path
        import pyarrow as pa
        table_data = pa.Table.from_pylist(rows)
        write_parquet_table(table_data, out_path, force=True)
        return out_path

    if fmt == "delta":
        table_root_dir = root / table
        if not rows:
            if force:
                _delete_delta_partition(table_root_dir, dt_str)
            return table_root_dir
        ensure_dir(table_root_dir)
        _write_delta(rows, table_root_dir, dt_value=dt_str)
        return table_root_dir

    raise ValueError(f"Unsupported storage format: {fmt}")


def _delete_delta_partition(table_root_dir: Path, dt_str: str) -> None:
    try:
        from pyspark.sql import SparkSession
    except Exception:
        return
    spark = SparkSession.builder.getOrCreate()
    table_root = spark_path(table_root_dir)
    if not _delta_exists(spark, table_root):
        return
    try:
        spark.sql(f"DELETE FROM delta.`{table_root}` WHERE dt = '{dt_str}'")
    except Exception as exc:
        logger.warning("failed to clear empty gold partition %s dt=%s: %s", table_root, dt_str, exc)


# -----------------------------
# Dimension tables
# -----------------------------

def _write_dim_league(root: Path, dt: date, force: bool) -> Path:
    leagues_path = Path("configs/leagues.yaml")
    if path_exists(leagues_path):
        import yaml

        with leagues_path.open("r", encoding="utf-8") as f:
            leagues = yaml.safe_load(f)["leagues"]
    else:
        leagues = {}
    rows = []
    for code, meta in leagues.items():
        rows.append(
            {
                "league_id": meta.get("league_id"),
                "league_code": code,
                "country": meta.get("country"),
                "level": meta.get("level"),
                "season_start_month": meta.get("season_start_month"),
                "season_end_month": meta.get("season_end_month"),
                "dt": dt.isoformat(),
            }
        )
    return _write_gold_table(root, "dim_league", dt, rows, force)


def _write_dim_team(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "mlb_statsapi", "games", dt):
        rows.extend(_team_rows_from_game(r, "MLB", bridge, "mlb_statsapi"))
    for r in _read_silver(root, "npb_spaia", "games", dt):
        rows.extend(_team_rows_from_game(r, "NPB", bridge, "npb_spaia"))
    for r in _read_silver(root, "indy_local", "games", dt):
        rows.extend(_team_rows_from_game(r, "INDY", bridge, "indy_local"))
    for r in _read_silver(root, "kbo_local", "games", dt):
        rows.extend(_team_rows_from_game(r, "KBO", bridge, "kbo_local"))
    for r in _read_silver(root, "lmb_local", "games", dt):
        rows.extend(_team_rows_from_game(r, "LMB", bridge, "lmb_local"))
    for r in _read_silver(root, "retrosheet_local", "games", dt):
        rows.extend(_team_rows_from_game(r, "MLB", bridge, "retrosheet_local"))
    dedup = {(r["team_id"], r["team_name"]): r for r in rows}
    scd_rows = _apply_scd2(
        root, "dim_team", dt, list(dedup.values()), key_field="team_id", attr_fields=["team_name", "team_abbrev", "home_city", "league_id"]
    )
    return _write_gold_table(root, "dim_team", dt, scd_rows, force)


def _team_rows_from_game(row: Dict[str, Any], league_code: str, bridge: Dict[tuple, str], source: str):
    out = []
    for team_id, team_name, abbrev in [
        (row.get("home_team_id"), row.get("home_team_name"), None),
        (row.get("away_team_id"), row.get("away_team_name"), None),
    ]:
        if not team_id:
            continue
        canonical = bridge.get(("team", source, str(team_id))) or ulid_from_key(f"team:source:{source}:{team_id}")
        out.append(
            {
                "team_id": canonical,
                "league_id": _league_id_for_code(league_code),
                "team_name": team_name,
                "team_abbrev": abbrev,
                "home_city": None,
                "valid_from_dt": row.get("dt") or row.get("game_date") or "",
                "valid_to_dt": None,
                "is_current": True,
                "dt": row.get("dt") or row.get("game_date") or "",
                "source": source,
                "source_team_id": str(team_id),
            }
        )
    return out


def _write_dim_player(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    bios = {str(b.get("player_id")): b for b in _read_silver(root, "mlb_statsapi", "player_bios", dt)}
    for r in _read_silver(root, "mlb_statsapi", "rosters", dt):
        rows.append(_player_row(r, "MLB", bridge, "mlb_statsapi", bios.get(str(r.get("player_id")))))
    for r in _read_silver(root, "npb_spaia", "rosters", dt):
        rows.append(_player_row(r, "NPB", bridge, "npb_spaia"))
    for r in _read_silver(root, "indy_local", "rosters", dt):
        rows.append(_player_row(r, "INDY", bridge, "indy_local"))
    for r in _read_silver(root, "kbo_local", "rosters", dt):
        rows.append(_player_row(r, "KBO", bridge, "kbo_local"))
    for r in _read_silver(root, "lmb_local", "rosters", dt):
        rows.append(_player_row(r, "LMB", bridge, "lmb_local"))
    for r in _read_silver(root, "retrosheet_local", "rosters", dt):
        rows.append(_player_row(r, "MLB", bridge, "retrosheet_local"))
    dedup = {r["player_id"]: r for r in rows if r.get("player_id")}
    scd_rows = _apply_scd2(
        root, "dim_player", dt, list(dedup.values()), key_field="player_id", attr_fields=["primary_name", "bats", "throws", "primary_position", "league_id"]
    )
    return _write_gold_table(root, "dim_player", dt, scd_rows, force)


def _alternate_names(r: Dict[str, Any]) -> List[str] | None:
    # NPB rosters carry a kana reading alongside the kanji name.
    try:
        kana = json.loads(r.get("raw_json") or "{}").get("PlayerNameK")
    except (TypeError, ValueError, AttributeError):
        return None
    return [kana] if kana else None


def _player_row(
    r: Dict[str, Any], league_code: str, bridge: Dict[tuple, str], source: str, bio: Dict[str, Any] | None = None
) -> Dict[str, Any]:
    bio = bio or {}
    source_id = r.get("player_id")
    canonical = bridge.get(("player", source, str(source_id))) or ulid_from_key(f"player:source:{source}:{source_id}")
    return {
        "player_id": canonical,
        "primary_name": r.get("player_name"),
        "alternate_names": _alternate_names(r),
        "dob": r.get("dob") or bio.get("dob"),
        "bats": r.get("bats") or bio.get("bats") or "UNK",
        "throws": r.get("throws") or bio.get("throws") or "UNK",
        "height_cm": bio.get("height_cm"),
        "weight_kg": bio.get("weight_kg"),
        "nationality": bio.get("birth_country"),
        "primary_position": r.get("position"),
        "valid_from_dt": r.get("dt"),
        "valid_to_dt": None,
        "is_current": True,
        "dt": r.get("dt"),
        "source": source,
        "source_player_id": str(source_id) if source_id is not None else None,
        "league_id": _league_id_for_code(league_code),
    }


def _write_dim_season(root: Path, dt: date, force: bool) -> Path:
    year = dt.year
    rows = [
        {
            "season_id": ulid_from_key(f"season:MLB:{year}"),
            "league_id": _league_id_for_code("MLB"),
            "season_year": year,
            "season_type": "regular",
            "season_start_dt": f"{year}-03-01",
            "season_end_dt": f"{year}-11-30",
            "dt": dt.isoformat(),
        },
        {
            "season_id": ulid_from_key(f"season:NPB:{year}"),
            "league_id": _league_id_for_code("NPB"),
            "season_year": year,
            "season_type": "regular",
            "season_start_dt": f"{year}-03-01",
            "season_end_dt": f"{year}-11-30",
            "dt": dt.isoformat(),
        },
        {
            "season_id": ulid_from_key(f"season:INDY:{year}"),
            "league_id": _league_id_for_code("INDY"),
            "season_year": year,
            "season_type": "regular",
            "season_start_dt": f"{year}-04-01",
            "season_end_dt": f"{year}-10-31",
            "dt": dt.isoformat(),
        },
        {
            "season_id": ulid_from_key(f"season:KBO:{year}"),
            "league_id": _league_id_for_code("KBO"),
            "season_year": year,
            "season_type": "regular",
            "season_start_dt": f"{year}-03-01",
            "season_end_dt": f"{year}-10-31",
            "dt": dt.isoformat(),
        },
        {
            "season_id": ulid_from_key(f"season:LMB:{year}"),
            "league_id": _league_id_for_code("LMB"),
            "season_year": year,
            "season_type": "regular",
            "season_start_dt": f"{year}-03-01",
            "season_end_dt": f"{year}-09-30",
            "dt": dt.isoformat(),
        },
    ]
    return _write_gold_table(root, "dim_season", dt, rows, force)


# -----------------------------
# Fact tables
# -----------------------------

def _write_fact_game(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    rows.extend(_fact_game_from_silver(root, dt, "mlb_statsapi", "MLB", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "npb_spaia", "NPB", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "indy_local", "INDY", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "kbo_local", "KBO", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "kbo_api", "KBO", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "lmb_local", "LMB", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "retrosheet_local", "MLB", bridge))
    return _write_gold_table(root, "fact_game", dt, rows, force)


def _fact_game_from_silver(root: Path, dt: date, source: str, league_code: str, bridge: Dict[tuple, str]):
    rows = []
    for r in _read_silver(root, source, "games", dt):
        game_id = ulid_from_key(f"game:{source}:{r.get('game_id')}")
        home_src = str(r.get("home_team_id")) if r.get("home_team_id") is not None else None
        away_src = str(r.get("away_team_id")) if r.get("away_team_id") is not None else None
        home_team = (bridge.get(("team", source, home_src)) if home_src else None) or (ulid_from_key(f"team:source:{source}:{home_src}") if home_src else None)
        away_team = (bridge.get(("team", source, away_src)) if away_src else None) or (ulid_from_key(f"team:source:{source}:{away_src}") if away_src else None)
        rows.append(
            {
                "game_id": game_id,
                "league_id": _league_id_for_code(league_code),
                "season_id": ulid_from_key(f"season:{league_code}:{dt.year}"),
                "game_date": r.get("game_date") or dt.isoformat(),
                "game_start_ts_utc": r.get("game_date"),
                "home_team_id": home_team,
                "away_team_id": away_team,
                "venue": r.get("venue"),
                "status": r.get("status") or r.get("game_state_name") or "unknown",
                "home_score": r.get("home_score"),
                "away_score": r.get("away_score"),
                "dt": dt.isoformat(),
                "source": source,
                "source_game_id": r.get("game_id"),
            }
        )
    return rows


def _write_fact_roster(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "mlb_statsapi", "rosters", dt):
        rows.append(_fact_roster_row(r, "mlb_statsapi", "MLB", bridge, dt))
    for r in _read_silver(root, "npb_spaia", "rosters", dt):
        rows.append(_fact_roster_row(r, "npb_spaia", "NPB", bridge, dt))
    for r in _read_silver(root, "indy_local", "rosters", dt):
        rows.append(_fact_roster_row(r, "indy_local", "INDY", bridge, dt))
    for r in _read_silver(root, "kbo_local", "rosters", dt):
        rows.append(_fact_roster_row(r, "kbo_local", "KBO", bridge, dt))
    for r in _read_silver(root, "lmb_local", "rosters", dt):
        rows.append(_fact_roster_row(r, "lmb_local", "LMB", bridge, dt))
    for r in _read_silver(root, "retrosheet_local", "rosters", dt):
        rows.append(_fact_roster_row(r, "retrosheet_local", "MLB", bridge, dt))
    return _write_gold_table(root, "fact_roster", dt, rows, force)


def _fact_roster_row(r: Dict[str, Any], source: str, league_code: str, bridge: Dict[tuple, str], dt: date):
    team_src = str(r.get("team_id")) if r.get("team_id") is not None else None
    player_src = str(r.get("player_id")) if r.get("player_id") is not None else None
    team = (bridge.get(("team", source, team_src)) if team_src else None) or (ulid_from_key(f"team:source:{source}:{team_src}") if team_src else None)
    player = (bridge.get(("player", source, player_src)) if player_src else None) or (ulid_from_key(f"player:source:{source}:{player_src}") if player_src else None)
    return {
        "team_id": team,
        "player_id": player,
        "season_id": ulid_from_key(f"season:{league_code}:{dt.year}"),
        "role": "player",
        "start_dt": dt.isoformat(),
        "end_dt": None,
        "dt": dt.isoformat(),
        "source": source,
    }


def _write_fact_transaction(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "mlb_statsapi", "transactions", dt):
        rows.append(
            {
                "transaction_id": ulid_from_key(f"txn:{r.get('transaction_id')}"),
                "league_id": _league_id_for_code("MLB"),
                "team_id": bridge.get(("team", "mlb_statsapi", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "mlb_statsapi", str(r.get("player_id")))),
                "transaction_type": r.get("type_desc"),
                "effective_dt": r.get("date"),
                "details": r.get("description"),
                "dt": dt.isoformat(),
                "source": "mlb_statsapi",
            }
        )
    return _write_gold_table(root, "fact_transaction", dt, rows, force)


def _write_fact_pitch(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "mlb_statcast", "pitches", dt):
        game_pk = r.get("game_pk")
        pa_id = _statcast_pa_id(game_pk, r.get("at_bat_number"))
        rows.append(
            {
                "pitch_id": ulid_from_key(f"pitch:mlb_statcast:{game_pk}:{r.get('at_bat_number')}:{r.get('pitch_number')}"),
                "game_id": ulid_from_key(f"game:mlb_statsapi:{game_pk}"),
                "pa_id": pa_id,
                "pitch_number_in_pa": r.get("pitch_number"),
                "pitcher_id": bridge.get(("player", "mlb_statsapi", str(r.get("pitcher_id")))),
                "batter_id": bridge.get(("player", "mlb_statsapi", str(r.get("batter_id")))),
                "balls_before": r.get("balls"),
                "strikes_before": r.get("strikes"),
                "pitch_type": r.get("pitch_type"),
                "release_speed": r.get("release_speed"),
                "plate_x": r.get("plate_x"),
                "plate_z": r.get("plate_z"),
                "result": r.get("description"),
                "dt": dt.isoformat(),
                "source": "mlb_statcast",
            }
        )
    return _write_gold_table(root, "fact_pitch", dt, rows, force)


def _statcast_pa_id(game_pk: Any, at_bat_number: Any) -> str:
    # at_bat_number is unique within a game, and fact_pitch must derive the same
    # ID. The batter stays out of the key: a pinch hitter can take over mid-PA.
    return ulid_from_key(f"pa:mlb_statcast:{game_pk}:{at_bat_number}")


def _statcast_runs_on_play(pitch: Dict[str, Any]) -> int | None:
    # The score fields live only in raw_json. Summing this per game reproduced
    # the real final score in 13 of 13 games on 2025-04-01.
    try:
        raw = json.loads(pitch.get("raw_json") or "{}")
    except (TypeError, ValueError):
        return None
    before = _coerce_known_type(raw.get("bat_score"), "long")
    after = _coerce_known_type(raw.get("post_bat_score"), "long")
    if before is None or after is None:
        return None
    return after - before


# Only used for a game's final PA, which has no next PA to read the outs from.
_EVENT_OUTS = {"OUT": 1, "K": 1, "SAC": 1, "DP": 2, "TP": 3}


def _statcast_plate_appearances(pitches: List[Dict[str, Any]], dt: date, bridge: Dict[tuple, str]) -> List[Dict[str, Any]]:
    """
    One row per plate appearance. Statcast gives only the state before each
    pitch, so the state after a PA comes from the first pitch of the next one.
    """
    by_game: Dict[Any, Dict[int, List[Dict[str, Any]]]] = {}
    for r in pitches:
        if r.get("at_bat_number") is None:
            continue
        by_game.setdefault(r.get("game_pk"), {}).setdefault(r["at_bat_number"], []).append(r)

    out: List[Dict[str, Any]] = []
    for game_pk, plate_appearances in by_game.items():
        ordered = [
            sorted(plate_appearances[n], key=lambda x: x.get("pitch_number") or 0)
            for n in sorted(plate_appearances)
        ]
        for i, pa_pitches in enumerate(ordered):
            first, last = pa_pitches[0], pa_pitches[-1]
            event_type = _map_event_type(last.get("events"))
            outs_before = first.get("outs_when_up")
            next_first = ordered[i + 1][0] if i + 1 < len(ordered) else None
            if next_first is None:
                # Last PA of the game. Usually the third out, but a walk-off isn't.
                outs_after = min(3, (outs_before or 0) + _EVENT_OUTS.get(event_type, 0))
                base_after = 0 if outs_after == 3 else None
            elif (next_first.get("inning"), next_first.get("inning_topbot")) == (first.get("inning"), first.get("inning_topbot")):
                outs_after = next_first.get("outs_when_up")
                base_after = _base_state_from_statcast(next_first)
            else:
                outs_after = 3
                base_after = 0
            out.append(
                {
                    "pa_id": _statcast_pa_id(game_pk, first.get("at_bat_number")),
                    "game_id": ulid_from_key(f"game:mlb_statsapi:{game_pk}"),
                    "inning": first.get("inning"),
                    "is_top_inning": first.get("inning_topbot") == "Top",
                    "batting_team_id": None,
                    "fielding_team_id": None,
                    "batter_id": bridge.get(("player", "mlb_statsapi", str(last.get("batter_id")))),
                    "pitcher_id": bridge.get(("player", "mlb_statsapi", str(last.get("pitcher_id")))),
                    "event_type": event_type,
                    "rbi": None,
                    "runs_scored_on_play": _statcast_runs_on_play(last),
                    "outs_on_play": None if outs_before is None or outs_after is None else outs_after - outs_before,
                    "base_state_before": _base_state_from_statcast(first),
                    "outs_before": outs_before,
                    "base_state_after": base_after,
                    "outs_after": outs_after,
                    "dt": dt.isoformat(),
                    "source": "mlb_statcast",
                }
            )
    return out


def _write_fact_plate_appearance(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    rows.extend(_statcast_plate_appearances(_read_silver(root, "mlb_statcast", "pitches", dt), dt, bridge))
    # NPB from PBP (minimal)
    npb_rows = _read_silver(root, "npb_spaia", "game_pbp", dt)
    rows.extend(_reconstruct_npb_pa(npb_rows, dt, bridge))
    # Retrosheet plays
    for r in _read_silver(root, "retrosheet_local", "game_pbp", dt):
        event_type = _map_retrosheet_event(r)
        base_before = _base_state_from_br(r.get("br1_pre"), r.get("br2_pre"), r.get("br3_pre"))
        base_after = _base_state_from_br(r.get("br1_post"), r.get("br2_post"), r.get("br3_post"))
        outs_before = r.get("outs_pre")
        outs_after = r.get("outs_post")
        outs_on_play = None
        if outs_before is not None and outs_after is not None:
            outs_on_play = outs_after - outs_before
        rows.append(
            {
                "pa_id": ulid_from_key(f"pa:retrosheet:{r.get('game_id')}:{r.get('event_id') or r.get('pn') or ''}"),
                "game_id": ulid_from_key(f"game:retrosheet_local:{r.get('game_id')}"),
                "inning": r.get("inning"),
                "is_top_inning": str(r.get("top_bottom")) in {"0", "Top", "top"},
                "batting_team_id": bridge.get(("team", "retrosheet_local", str(r.get("batting_team_id")))),
                "fielding_team_id": bridge.get(("team", "retrosheet_local", str(r.get("pitching_team_id")))),
                "batter_id": bridge.get(("player", "retrosheet_local", str(r.get("batter_id")))),
                "pitcher_id": bridge.get(("player", "retrosheet_local", str(r.get("pitcher_id")))),
                "event_type": event_type,
                "rbi": r.get("rbi"),
                "runs_scored_on_play": r.get("runs"),
                "outs_on_play": outs_on_play,
                "base_state_before": base_before,
                "outs_before": outs_before,
                "base_state_after": base_after,
                "outs_after": outs_after,
                "dt": dt.isoformat(),
                "source": "retrosheet_local",
            }
        )
    return _write_gold_table(root, "fact_plate_appearance", dt, rows, force)


def _write_fact_standings(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "npb_spaia", "standings", dt):
        rows.append(
            {
                "league_id": _league_id_for_code("NPB"),
                "season_id": ulid_from_key(f"season:NPB:{dt.year}"),
                "team_id": bridge.get(("team", "npb_spaia", str(r.get("team_id")))),
                "w": r.get("w"),
                "l": r.get("l"),
                "t": r.get("t"),
                "pct": r.get("pct"),
                "gb": r.get("gb"),
                "dt": dt.isoformat(),
                "source": "npb_spaia",
            }
        )
    return _write_gold_table(root, "fact_standings", dt, rows, force)


def _write_fact_boxscore_batting(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "mlb_statsapi", "boxscore_batting", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:mlb_statsapi:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "mlb_statsapi", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "mlb_statsapi", str(r.get("player_id")))),
                "ab": r.get("ab"),
                "h": r.get("h"),
                "2b": r.get("2b"),
                "3b": r.get("3b"),
                "hr": r.get("hr"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "rbi": r.get("rbi"),
                "r": r.get("r"),
                "dt": dt.isoformat(),
                "source": "mlb_statsapi",
            }
        )
    for r in _read_silver(root, "kbo_api", "boxscore_batting", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:kbo_api:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "kbo_api", str(r.get("team_id")))),
                # kbo_api exposes no numeric player ID anywhere -- player_name
                # is the only available bridge key for this source.
                "player_id": bridge.get(("player", "kbo_api", str(r.get("player_name")))),
                "ab": r.get("ab"),
                "h": r.get("h"),
                "2b": None,
                "3b": None,
                "hr": None,
                "bb": None,
                "so": None,
                "rbi": r.get("rbi"),
                "r": r.get("r"),
                "dt": dt.isoformat(),
                "source": "kbo_api",
            }
        )
    for r in _read_silver(root, "indy_local", "boxscore_batting", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:indy_local:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "indy_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "indy_local", str(r.get("player_id")))),
                "ab": r.get("ab"),
                "h": r.get("h"),
                "2b": r.get("2b"),
                "3b": r.get("3b"),
                "hr": r.get("hr"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "rbi": r.get("rbi"),
                "r": r.get("r"),
                "dt": dt.isoformat(),
                "source": "indy_local",
            }
        )
    for r in _read_silver(root, "kbo_local", "boxscore_batting", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:kbo_local:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "kbo_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "kbo_local", str(r.get("player_id")))),
                "ab": r.get("ab"),
                "h": r.get("h"),
                "2b": r.get("2b"),
                "3b": r.get("3b"),
                "hr": r.get("hr"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "rbi": r.get("rbi"),
                "r": r.get("r"),
                "dt": dt.isoformat(),
                "source": "kbo_local",
            }
        )
    for r in _read_silver(root, "lmb_local", "boxscore_batting", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:lmb_local:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "lmb_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "lmb_local", str(r.get("player_id")))),
                "ab": r.get("ab"),
                "h": r.get("h"),
                "2b": r.get("2b"),
                "3b": r.get("3b"),
                "hr": r.get("hr"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "rbi": r.get("rbi"),
                "r": r.get("r"),
                "dt": dt.isoformat(),
                "source": "lmb_local",
            }
        )
    for r in _read_silver(root, "retrosheet_local", "boxscore_batting", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:retrosheet_local:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "retrosheet_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "retrosheet_local", str(r.get("player_id")))),
                "ab": r.get("ab"),
                "h": r.get("h"),
                "2b": r.get("2b"),
                "3b": r.get("3b"),
                "hr": r.get("hr"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "rbi": r.get("rbi"),
                "r": r.get("r"),
                "dt": dt.isoformat(),
                "source": "retrosheet_local",
            }
        )
    return _write_gold_table(root, "fact_boxscore_batting", dt, rows, force)


def _write_fact_boxscore_pitching(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "mlb_statsapi", "boxscore_pitching", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:mlb_statsapi:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "mlb_statsapi", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "mlb_statsapi", str(r.get("player_id")))),
                "ip": r.get("ip"),
                "h": r.get("h"),
                "r": r.get("r"),
                "er": r.get("er"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "hr": r.get("hr"),
                "dt": dt.isoformat(),
                "source": "mlb_statsapi",
            }
        )
    for r in _read_silver(root, "kbo_api", "boxscore_pitching", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:kbo_api:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "kbo_api", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "kbo_api", str(r.get("player_name")))),
                "ip": r.get("ip"),
                "h": r.get("h"),
                "r": r.get("r"),
                "er": r.get("er"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "hr": r.get("hr"),
                "dt": dt.isoformat(),
                "source": "kbo_api",
            }
        )
    for r in _read_silver(root, "indy_local", "boxscore_pitching", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:indy_local:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "indy_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "indy_local", str(r.get("player_id")))),
                "ip": r.get("ip"),
                "h": r.get("h"),
                "r": r.get("r"),
                "er": r.get("er"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "hr": r.get("hr"),
                "dt": dt.isoformat(),
                "source": "indy_local",
            }
        )
    for r in _read_silver(root, "kbo_local", "boxscore_pitching", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:kbo_local:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "kbo_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "kbo_local", str(r.get("player_id")))),
                "ip": r.get("ip"),
                "h": r.get("h"),
                "r": r.get("r"),
                "er": r.get("er"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "hr": r.get("hr"),
                "dt": dt.isoformat(),
                "source": "kbo_local",
            }
        )
    for r in _read_silver(root, "lmb_local", "boxscore_pitching", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:lmb_local:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "lmb_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "lmb_local", str(r.get("player_id")))),
                "ip": r.get("ip"),
                "h": r.get("h"),
                "r": r.get("r"),
                "er": r.get("er"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "hr": r.get("hr"),
                "dt": dt.isoformat(),
                "source": "lmb_local",
            }
        )
    for r in _read_silver(root, "retrosheet_local", "boxscore_pitching", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:retrosheet_local:{r.get('game_id')}"),
                "team_id": bridge.get(("team", "retrosheet_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "retrosheet_local", str(r.get("player_id")))),
                "ip": r.get("ip"),
                "h": r.get("h"),
                "r": r.get("r"),
                "er": r.get("er"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "hr": r.get("hr"),
                "dt": dt.isoformat(),
                "source": "retrosheet_local",
            }
        )
    return _write_gold_table(root, "fact_boxscore_pitching", dt, rows, force)


# -----------------------------
# Derived tables (FIXED delta reads)
# -----------------------------

def _write_run_expectancy(root: Path, dt: date, force: bool) -> Path:
    # Compute RE by base_state_before + outs_before using available runs_scored_on_play
    fmt = storage_format()
    dt_str = dt.isoformat()

    if fmt == "parquet":
        path = root / "fact_plate_appearance" / f"dt={dt_str}"
        if not path_exists(path):
            return _write_gold_table(root, "run_expectancy", dt, [], force)
        import duckdb
        con = duckdb.connect()
        con.execute(f"CREATE OR REPLACE VIEW pa AS SELECT * FROM read_parquet('{path}/*.parquet')")
        rows = con.execute(
            """
            SELECT
              base_state_before,
              outs_before,
              AVG(COALESCE(runs_scored_on_play, 0)) AS exp_runs
            FROM pa
            WHERE base_state_before IS NOT NULL AND outs_before IS NOT NULL
            GROUP BY 1,2
            """
        ).fetchall()

    else:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = SparkSession.builder.getOrCreate()
        base = root / "fact_plate_appearance"
        if not path_exists(base):
            return _write_gold_table(root, "run_expectancy", dt, [], force)

        df = spark.read.format("delta").load(spark_path(base)).where(f"dt = '{dt_str}'")
        df = df.where("base_state_before IS NOT NULL AND outs_before IS NOT NULL")
        rows = [
            (r["base_state_before"], r["outs_before"], r["exp_runs"])
            for r in df.groupBy("base_state_before", "outs_before")
            .agg(F.avg(F.coalesce("runs_scored_on_play", F.lit(0))).alias("exp_runs"))
            .collect()
        ]

    out_rows = [{"base_state": r[0], "outs": r[1], "exp_runs": float(r[2]), "dt": dt_str} for r in rows]
    return _write_gold_table(root, "run_expectancy", dt, out_rows, force)


def _write_breakout_candidates(root: Path, dt: date, force: bool) -> Path:
    # Simple heuristic: top 20 HR on dt from boxscore_batting
    fmt = storage_format()
    dt_str = dt.isoformat()

    if fmt == "parquet":
        path = root / "fact_boxscore_batting" / f"dt={dt_str}"
        if not path_exists(path):
            return _write_gold_table(root, "breakout_candidates", dt, [], force)
        import duckdb
        con = duckdb.connect()
        con.execute(f"CREATE OR REPLACE VIEW b AS SELECT * FROM read_parquet('{path}/*.parquet')")
        rows = con.execute(
            """
            SELECT player_id, SUM(hr) as hr, SUM(h) as h, SUM(ab) as ab
            FROM b
            WHERE player_id IS NOT NULL
            GROUP BY 1
            ORDER BY hr DESC, h DESC
            LIMIT 20
            """
        ).fetchall()

    else:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = SparkSession.builder.getOrCreate()
        base = root / "fact_boxscore_batting"
        if not path_exists(base):
            return _write_gold_table(root, "breakout_candidates", dt, [], force)

        df = spark.read.format("delta").load(spark_path(base)).where(f"dt = '{dt_str}'")
        rows = [
            (r["player_id"], r["hr"], r["h"], r["ab"])
            for r in df.where("player_id IS NOT NULL")
            .groupBy("player_id")
            .agg(F.sum("hr").alias("hr"), F.sum("h").alias("h"), F.sum("ab").alias("ab"))
            .orderBy(F.col("hr").desc(), F.col("h").desc())
            .limit(20)
            .collect()
        ]

    out_rows = [
        {"player_id": r[0], "hr": int(r[1] or 0), "h": int(r[2] or 0), "ab": int(r[3] or 0), "dt": dt_str}
        for r in rows
    ]
    return _write_gold_table(root, "breakout_candidates", dt, out_rows, force)


def _write_feature_player_rolling_30d(root: Path, dt: date, force: bool) -> Path:
    # Rolling 30d from fact_boxscore_batting across leagues
    fmt = storage_format()
    dt_str = dt.isoformat()

    if fmt == "parquet":
        import duckdb
        con = duckdb.connect()
        paths = []
        for offset in range(0, 30):
            d = (dt - timedelta(days=offset)).isoformat()
            path = root / "fact_boxscore_batting" / f"dt={d}"
            if path_exists(path):
                paths.append(f"{path}/*.parquet")
        if not paths:
            return _write_gold_table(root, "feature_player_rolling_30d", dt, [], force)
        paths_sql = ", ".join(f"'{p}'" for p in paths)
        con.execute(f"CREATE OR REPLACE VIEW b AS SELECT * FROM read_parquet([{paths_sql}])")
        rows = con.execute(
            """
            SELECT
              player_id,
              SUM(ab) AS ab,
              SUM(h) AS h,
              SUM(hr) AS hr,
              SUM(rbi) AS rbi,
              SUM(bb) AS bb,
              SUM(so) AS so
            FROM b
            WHERE player_id IS NOT NULL
            GROUP BY 1
            """
        ).fetchall()
        out_rows = [
            {"player_id": r[0], "ab": int(r[1] or 0), "h": int(r[2] or 0), "hr": int(r[3] or 0),
             "rbi": int(r[4] or 0), "bb": int(r[5] or 0), "so": int(r[6] or 0), "window_days": 30, "dt": dt_str}
            for r in rows
        ]
        return _write_gold_table(root, "feature_player_rolling_30d", dt, out_rows, force)

    # delta: read ONE table and filter dt range
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    base = root / "fact_boxscore_batting"
    if not path_exists(base):
        return _write_gold_table(root, "feature_player_rolling_30d", dt, [], force)

    start_dt = (dt - timedelta(days=29)).isoformat()
    df = spark.read.format("delta").load(spark_path(base))
    df = df.where((F.col("dt") >= start_dt) & (F.col("dt") <= dt_str))

    agg = (
        df.where("player_id IS NOT NULL")
        .groupBy("player_id")
        .agg(
            F.sum("ab").alias("ab"),
            F.sum("h").alias("h"),
            F.sum("hr").alias("hr"),
            F.sum("rbi").alias("rbi"),
            F.sum("bb").alias("bb"),
            F.sum("so").alias("so"),
        )
    )

    out_rows = [
        {
            "player_id": r["player_id"],
            "ab": int(r["ab"] or 0),
            "h": int(r["h"] or 0),
            "hr": int(r["hr"] or 0),
            "rbi": int(r["rbi"] or 0),
            "bb": int(r["bb"] or 0),
            "so": int(r["so"] or 0),
            "window_days": 30,
            "dt": dt_str,
        }
        for r in agg.collect()
    ]
    return _write_gold_table(root, "feature_player_rolling_30d", dt, out_rows, force)


def _write_fact_contract(root: Path, dt: date, force: bool) -> Path:
    # Placeholder until contract sources are integrated
    return _write_gold_table(root, "fact_contract", dt, [], force)


# -----------------------------
# Event mapping helpers
# -----------------------------

def _map_event_type(value: Any) -> str:
    if not value:
        return "UNKNOWN"
    v = str(value).upper()
    if "STRIKEOUT" in v or v == "K":
        return "K"
    if "WALK" in v or v == "BB":
        return "BB"
    if "HBP" in v or "HIT_BY_PITCH" in v:
        return "HBP"
    if "HOME_RUN" in v or v == "HR":
        return "HR"
    if v in {"SINGLE", "1B"}:
        return "1B"
    if v in {"DOUBLE", "2B"}:
        return "2B"
    if v in {"TRIPLE", "3B"}:
        return "3B"
    if "TRIPLE_PLAY" in v:
        return "TP"
    if "DOUBLE_PLAY" in v:
        return "DP"
    if v.startswith("SAC"):
        return "SAC"
    if "FIELDERS_CHOICE" in v:
        return "FC"
    if "ERROR" in v:
        return "ERROR"
    if "OUT" in v:
        return "OUT"
    return "UNKNOWN"


def _map_retrosheet_event(r: Dict[str, Any]) -> str:
    def _is_one(key: str) -> bool:
        v = r.get(key)
        return str(v) == "1"

    if _is_one("hr"):
        return "HR"
    if _is_one("triple"):
        return "3B"
    if _is_one("double"):
        return "2B"
    if _is_one("single"):
        return "1B"
    if _is_one("walk"):
        return "BB"
    if _is_one("hbp"):
        return "HBP"
    if _is_one("k"):
        return "K"
    if _is_one("roe"):
        return "ERROR"
    if _is_one("fc"):
        return "FC"
    if _is_one("sf"):
        return "SAC"
    if _is_one("sh"):
        return "SAC"
    if _is_one("gdp"):
        return "DP"
    if _is_one("tp"):
        return "TP"
    if _is_one("othout") or _is_one("noout"):
        return "OUT"
    return "UNKNOWN"


# -----------------------------
# NPB reconstruction helpers
# -----------------------------

def _reconstruct_npb_pa(rows: List[Dict[str, Any]], dt: date, bridge: Dict[tuple, str]) -> List[Dict[str, Any]]:
    by_game: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        gid = str(r.get("game_id") or "")
        by_game.setdefault(gid, []).append(r)
    out: List[Dict[str, Any]] = []
    for gid, items in by_game.items():
        items.sort(key=lambda x: (x.get("inning") or 0, _npb_event_order(x)))
        bases = [0, 0, 0]
        outs = 0
        last_inning = None
        last_top = None
        for r in items:
            inning = r.get("inning")
            top = r.get("top_bottom")
            if (inning, top) != (last_inning, last_top):
                bases = [0, 0, 0]
                outs = 0
                last_inning, last_top = inning, top
            text = (r.get("text_info_text") or r.get("text_info_name") or "").lower()
            event_type, outs_on_play, bases_after = _npb_infer_event(text, bases)
            base_before = _bases_to_state(bases)
            outs_before = outs
            outs = min(3, outs + outs_on_play)
            base_after = _bases_to_state(bases_after)
            outs_after = outs
            bases = bases_after
            out.append(
                {
                    "pa_id": ulid_from_key(f"pa:npb:{gid}:{r.get('event_id') or r.get('play_seq_no')}"),
                    "game_id": ulid_from_key(f"game:npb_spaia:{gid}"),
                    "inning": inning,
                    "is_top_inning": str(top) == "1",
                    "batting_team_id": None,
                    "fielding_team_id": None,
                    "batter_id": bridge.get(("player", "npb_spaia", str(r.get("play_player_id")))),
                    "pitcher_id": None,
                    "event_type": event_type,
                    "rbi": None,
                    "runs_scored_on_play": None,
                    "outs_on_play": outs_on_play,
                    "base_state_before": base_before,
                    "outs_before": outs_before,
                    "base_state_after": base_after,
                    "outs_after": outs_after,
                    "dt": dt.isoformat(),
                    "source": "npb_spaia",
                }
            )
    return out


def _npb_event_order(row: Dict[str, Any]) -> int:
    # play_seq_no is an int but the event_id fallback is a string; comparing the
    # two crashed gold_publish on real data.
    value = _coerce_known_type(row.get("play_seq_no") or row.get("event_id"), "long")
    return value or 0


def _npb_infer_event(text: str, bases: List[int]) -> tuple[str, int, List[int]]:
    t = text or ""
    if "三重殺" in t or "triple play" in t:
        return "TP", 3, [0, 0, 0]
    if "併殺" in t or "double play" in t:
        return "DP", 2, [0, 0, 0]
    if "本塁打" in t or "ホームラン" in t or "home run" in t:
        return "HR", 0, [0, 0, 0]
    if "三塁打" in t or "triple" in t:
        return "3B", 0, [0, 0, 1]
    if "二塁打" in t or "double" in t:
        return "2B", 0, _advance_bases(bases, 2)
    if "安打" in t or "ヒット" in t or "single" in t:
        return "1B", 0, _advance_bases(bases, 1)
    if "四球" in t or "死球" in t or "walk" in t or "hbp" in t:
        return "BB", 0, _advance_bases(bases, 1)
    if "犠打" in t or "犠飛" in t or "sacrifice" in t:
        return "SAC", 1, bases
    if "アウト" in t or "out" in t or "フライ" in t or "ゴロ" in t:
        return "OUT", 1, bases
    return "UNKNOWN", 0, bases


def _advance_bases(bases: List[int], bases_taken: int) -> List[int]:
    b1, b2, b3 = bases
    if bases_taken == 1:
        if b1:
            if b2:
                b3 = 1
            b2 = 1
        b1 = 1
    elif bases_taken == 2:
        if b2:
            b3 = 1
        if b1:
            b3 = 1
        b2 = 1
        b1 = 0
    return [b1, b2, b3]


def _bases_to_state(bases: List[int]) -> int:
    return (1 if bases[0] else 0) + (2 if bases[1] else 0) + (4 if bases[2] else 0)


def _base_state_from_statcast(r: Dict[str, Any]) -> int | None:
    on_1b = r.get("on_1b")
    on_2b = r.get("on_2b")
    on_3b = r.get("on_3b")
    try:
        b1 = 1 if on_1b not in (None, "") else 0
        b2 = 1 if on_2b not in (None, "") else 0
        b3 = 1 if on_3b not in (None, "") else 0
        return b1 + (b2 * 2) + (b3 * 4)
    except Exception:
        return None


def _base_state_from_br(br1: Any, br2: Any, br3: Any) -> int | None:
    def _present(v: Any) -> int:
        if v in (None, "", "0", "NA"):
            return 0
        return 1
    return _present(br1) + (_present(br2) * 2) + (_present(br3) * 4)


# -----------------------------
# SCD2 helpers
# -----------------------------

def _apply_scd2(
    root: Path,
    table: str,
    dt: date,
    new_rows: List[Dict[str, Any]],
    key_field: str,
    attr_fields: List[str],
) -> List[Dict[str, Any]]:
    prev_dt = _latest_dt_before(root, table, dt)
    if not prev_dt:
        for r in new_rows:
            r["valid_from_dt"] = dt.isoformat()
            r["valid_to_dt"] = None
            r["is_current"] = True
        return new_rows
    prev_rows = _read_gold_snapshot(root, table, prev_dt)
    return _scd2_merge(prev_rows, new_rows, key_field, attr_fields, dt, prev_dt)


def _attrs_equal(a: Dict[str, Any], b: Dict[str, Any], fields: List[str]) -> bool:
    for f in fields:
        if a.get(f) != b.get(f):
            return False
    return True


def _scd2_merge(
    prev_rows: List[Dict[str, Any]],
    new_rows: List[Dict[str, Any]],
    key_field: str,
    attr_fields: List[str],
    dt: date,
    prev_dt: date,
) -> List[Dict[str, Any]]:
    # Each dt partition is a full snapshot, so prev_rows holds closed history as
    # well as the open version of each key. History is immutable: carry it over
    # untouched and only compare new rows against the open version.
    prev_by_key: Dict[str, Dict[str, Any]] = {}
    out_rows: List[Dict[str, Any]] = []
    for r in prev_rows:
        if not r.get(key_field):
            continue
        if r.get("is_current") is False:
            out_rows.append(dict(r))
        else:
            prev_by_key[str(r.get(key_field))] = r
    seen_keys = set()
    prev_dt_str = prev_dt.isoformat()
    for r in new_rows:
        key = str(r.get(key_field))
        seen_keys.add(key)
        prev = prev_by_key.get(key)
        if not prev:
            r["valid_from_dt"] = dt.isoformat()
            r["valid_to_dt"] = None
            r["is_current"] = True
            out_rows.append(r)
            continue
        if _attrs_equal(prev, r, attr_fields):
            r["valid_from_dt"] = prev.get("valid_from_dt") or prev_dt_str
            r["valid_to_dt"] = None
            r["is_current"] = True
            out_rows.append(r)
        else:
            prev_closed = dict(prev)
            prev_closed["valid_to_dt"] = (dt - timedelta(days=1)).isoformat()
            prev_closed["is_current"] = False
            out_rows.append(prev_closed)
            r["valid_from_dt"] = dt.isoformat()
            r["valid_to_dt"] = None
            r["is_current"] = True
            out_rows.append(r)
    # Dims are built from one day's games/rosters, so a key missing today just
    # didn't appear (a team with an off day), not an entity that ended.
    for key, prev in prev_by_key.items():
        if key not in seen_keys:
            out_rows.append(dict(prev))
    return out_rows


def _latest_dt_before(root: Path, table: str, dt: date) -> date | None:
    base = root / table
    if not path_exists(base):
        return None
    candidates: List[date] = []
    for part in list_dir(base, dirs_only=True):
        name = part.name
        if not name.startswith("dt="):
            continue
        try:
            d = date.fromisoformat(name.split("=", 1)[1])
        except Exception:
            continue
        if d < dt:
            candidates.append(d)
    if not candidates:
        return None
    return sorted(candidates)[-1]


# -----------------------------
# League config
# -----------------------------

def _league_id_for_code(code: str) -> str | None:
    leagues_path = Path("configs/leagues.yaml")
    if not path_exists(leagues_path):
        return None
    import yaml

    with leagues_path.open("r", encoding="utf-8") as f:
        leagues = yaml.safe_load(f)["leagues"]
    meta = leagues.get(code, {})
    return meta.get("league_id")
