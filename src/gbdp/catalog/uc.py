from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

from gbdp.utils.io import (
    bronze_root,
    dbfs_uri,
    gold_root,
    list_dir,
    path_exists,
    silver_root,
    storage_format,
    uc_location,
)


def register_uc_tables_from_env() -> List[Tuple[str, str, str]]:
    catalog = os.getenv("GBDP_UC_CATALOG")
    bronze_schema = os.getenv("GBDP_UC_BRONZE_SCHEMA", "bronze_gbdp")
    silver_schema = os.getenv("GBDP_UC_SILVER_SCHEMA", "silver_gbdp")
    gold_schema = os.getenv("GBDP_UC_GOLD_SCHEMA", "gold_gbdp")
    if not catalog:
        return []
    return register_uc_tables(
        catalog=catalog,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
        bronze_path=bronze_root(),
        silver_path=silver_root(),
        gold_path=gold_root(),
        fmt=storage_format(),
    )


def register_uc_tables(
    catalog: str,
    bronze_schema: str,
    silver_schema: str,
    gold_schema: str,
    bronze_path: Path,
    silver_path: Path,
    gold_path: Path,
    fmt: str,
) -> List[Tuple[str, str, str]]:
    spark = _spark_session()
    if spark is None:
        return []

    _ensure_schema(spark, catalog, bronze_schema)
    _ensure_schema(spark, catalog, silver_schema)
    _ensure_schema(spark, catalog, gold_schema)

    created: List[Tuple[str, str, str]] = []
    created += _register_bronze_parsed(spark, catalog, bronze_schema, bronze_path)
    created += _register_silver(spark, catalog, silver_schema, silver_path, fmt)
    created += _register_gold(spark, catalog, gold_schema, gold_path, fmt)
    created += _register_expected_tables(
        spark,
        catalog,
        bronze_schema,
        silver_schema,
        gold_schema,
        bronze_path,
        silver_path,
        gold_path,
        fmt,
    )
    return created


def _spark_session():
    try:
        from pyspark.sql import SparkSession
    except Exception:
        return None
    return SparkSession.builder.getOrCreate()


def _ensure_schema(spark, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def _register_bronze_parsed(
    spark,
    catalog: str,
    schema: str,
    bronze_path: Path,
) -> List[Tuple[str, str, str]]:
    created: List[Tuple[str, str, str]] = []
    base = bronze_path / "parsed"
    if path_exists(base):
        for source_dir in list_dir(base, dirs_only=True):
            source = source_dir.name
            for entity_dir in list_dir(source_dir, dirs_only=True):
                entity = entity_dir.name
                table = f"{source}_{entity}"
            location = uc_location(entity_dir)
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} "
                f"USING PARQUET LOCATION '{location}'"
            )
                created.append((schema, table, location))
    requests_dir = bronze_path / "requests"
    if path_exists(requests_dir):
        location = uc_location(requests_dir)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.requests "
            f"USING PARQUET LOCATION '{location}'"
        )
        created.append((schema, "requests", location))
    return created


def _register_silver(
    spark,
    catalog: str,
    schema: str,
    silver_path: Path,
    fmt: str,
) -> List[Tuple[str, str, str]]:
    if not path_exists(silver_path):
        return []
    created: List[Tuple[str, str, str]] = []
    for source_dir in list_dir(silver_path, dirs_only=True):
        source = source_dir.name
        for entity_dir in list_dir(source_dir, dirs_only=True):
            entity = entity_dir.name
            table = f"{source}_{entity}"
            location = uc_location(entity_dir)
            _create_table(spark, catalog, schema, table, location, fmt)
            created.append((schema, table, location))
    return created


def _register_gold(
    spark,
    catalog: str,
    schema: str,
    gold_path: Path,
    fmt: str,
) -> List[Tuple[str, str, str]]:
    if not path_exists(gold_path):
        return []
    created: List[Tuple[str, str, str]] = []
    for table_dir in list_dir(gold_path, dirs_only=True):
        table = table_dir.name
        location = uc_location(table_dir)
        _create_table(spark, catalog, schema, table, location, fmt)
        created.append((schema, table, location))
    return created


def _create_table(spark, catalog: str, schema: str, table: str, location: str, fmt: str) -> None:
    fmt_upper = "DELTA" if fmt == "delta" else "PARQUET"
    # Ensure UC-compatible location (no dbfs: scheme)
    loc = location
    if isinstance(location, str):
        if location.startswith("dbfs:/"):
            loc = "/" + location[len("dbfs:/") :]
        if location.startswith("/dbfs/"):
            loc = "/" + location[len("/dbfs/") :]
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} "
        f"USING {fmt_upper} LOCATION '{loc}'"
    )


def _register_expected_tables(
    spark,
    catalog: str,
    bronze_schema: str,
    silver_schema: str,
    gold_schema: str,
    bronze_path: Path,
    silver_path: Path,
    gold_path: Path,
    fmt: str,
) -> List[Tuple[str, str, str]]:
    created: List[Tuple[str, str, str]] = []
    sources = _load_sources(Path("configs/sources.yaml"))
    for source, cfg in sources.items():
        endpoints = cfg.get("endpoints", {})
        for entity in endpoints.keys():
            table = f"{source}_{entity}"
            # bronze parsed
            bronze_loc = uc_location(bronze_path / "parsed" / source / entity)
            _ensure_dbfs_dir(bronze_loc)
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.{table} "
                f"USING PARQUET LOCATION '{bronze_loc}'"
            )
            created.append((bronze_schema, table, bronze_loc))
            # silver
            silver_loc = uc_location(silver_path / source / entity)
            _ensure_dbfs_dir(silver_loc)
            _create_table(spark, catalog, silver_schema, table, silver_loc, fmt)
            created.append((silver_schema, table, silver_loc))
    # gold fixed list
    for table in _expected_gold_tables():
        gold_loc = uc_location(gold_path / table)
        _ensure_dbfs_dir(gold_loc)
        _create_table(spark, catalog, gold_schema, table, gold_loc, fmt)
        created.append((gold_schema, table, gold_loc))
    return created


def _load_sources(path: Path) -> Dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _expected_gold_tables() -> List[str]:
    return [
        "dim_league",
        "dim_team",
        "dim_player",
        "dim_season",
        "bridge_source_ids",
        "merge_events",
        "fact_game",
        "fact_roster",
        "fact_transaction",
        "fact_contract",
        "fact_plate_appearance",
        "fact_pitch",
        "fact_boxscore_batting",
        "fact_boxscore_pitching",
        "fact_standings",
        "run_expectancy",
        "breakout_candidates",
        "feature_player_rolling_30d",
        "audit_pipeline_runs",
        "audit_stage_runs",
        "audit_quality",
        "audit_schema",
    ]


def _ensure_dbfs_dir(location: str) -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils
    except Exception:
        return
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    try:
        # dbutils expects dbfs:/ scheme
        if location.startswith("/Volumes/"):
            dbutils.fs.mkdirs(f"dbfs:{location}")
        elif location.startswith("dbfs:/"):
            dbutils.fs.mkdirs(location)
        else:
            return
    except Exception:
        pass
