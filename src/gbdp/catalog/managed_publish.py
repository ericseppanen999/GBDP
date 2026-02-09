from __future__ import annotations

from datetime import date
from typing import List

import os

from gbdp.utils.io import (
    bronze_root,
    gold_root,
    list_dir,
    path_exists,
    silver_root,
    spark_path,
    storage_format,
    has_files_with_suffix,
)
from gbdp.utils.time import daterange, parse_date


def publish_uc_managed(start: str, end: str, catalog: str, bronze_schema: str, silver_schema: str, gold_schema: str) -> None:
    spark = _spark_session()
    if spark is None:
        return
    _ensure_schema(spark, catalog, bronze_schema)
    _ensure_schema(spark, catalog, silver_schema)
    _ensure_schema(spark, catalog, gold_schema)
    for d in daterange(parse_date(start), parse_date(end)):
        _publish_gold(spark, d, catalog, gold_schema)
        _publish_silver(spark, d, catalog, silver_schema)
        _publish_bronze_parsed(spark, d, catalog, bronze_schema)
        _publish_bronze_requests(spark, d, catalog, bronze_schema)


def publish_uc_managed_from_env(start: str, end: str) -> None:
    catalog = os.getenv("GBDP_UC_CATALOG")
    if not catalog:
        return
    bronze_schema = os.getenv("GBDP_UC_BRONZE_SCHEMA", "bronze_gbdp")
    silver_schema = os.getenv("GBDP_UC_SILVER_SCHEMA", "silver_gbdp")
    gold_schema = os.getenv("GBDP_UC_GOLD_SCHEMA", "gold_gbdp")
    publish_uc_managed(start, end, catalog, bronze_schema, silver_schema, gold_schema)


def _spark_session():
    try:
        from pyspark.sql import SparkSession
    except Exception:
        return None
    return SparkSession.builder.getOrCreate()


def _table_exists(spark, full_name: str) -> bool:
    try:
        return spark.catalog.tableExists(full_name)
    except Exception:
        return False


def _ensure_schema(spark, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def _is_delta_table_dir(path) -> bool:
    # Delta table is only "real" if _delta_log exists at table root
    try:
        return path_exists(path / "_delta_log")
    except Exception:
        return False


def _publish_gold(spark, dt: date, catalog: str, schema: str) -> None:
    base = gold_root()
    if not path_exists(base):
        return

    dt_str = dt.isoformat()

    for table_dir in list_dir(base, dirs_only=True):
        table = table_dir.name
        part = table_dir / f"dt={dt_str}"

        # Keep the cheap existence check so we don't scan the whole table for dates with no data
        if not path_exists(part):
            continue

        df = None
        try:
            # IMPORTANT: for delta, read the TABLE ROOT (table_dir), not the partition dir (part)
            if _is_delta_table_dir(table_dir):
                candidate = spark.read.format("delta").load(spark_path(table_dir)).where(f"dt = '{dt_str}'")
            else:
                candidate = spark.read.format("delta").load(spark_path(part))

            # force evaluation to catch missing _delta_log errors
            candidate.limit(1).collect()
            df = candidate

        except Exception:
            # fallback parquet for legacy dt-folders / non-delta datasets
            if not has_files_with_suffix(part, ".parquet"):
                continue
            df = spark.read.format("parquet").load(spark_path(part))

        from pyspark.sql import functions as F
        if "dt" not in df.columns:
            df = df.withColumn("dt", F.lit(dt_str))

        full_name = f"{catalog}.{schema}.{table}"
        _write_managed(spark, df, full_name, dt)


def _publish_silver(spark, dt: date, catalog: str, schema: str) -> None:
    base = silver_root()
    if not path_exists(base):
        return

    fmt = storage_format()
    dt_str = dt.isoformat()

    for source_dir in list_dir(base, dirs_only=True):
        source = source_dir.name
        for entity_dir in list_dir(source_dir, dirs_only=True):
            entity = entity_dir.name
            part = entity_dir / f"dt={dt_str}"

            # Same optimization: if no dt partition folder, skip
            if not path_exists(part):
                continue

            df = None

            if fmt == "delta":
                try:
                    # IMPORTANT: for delta, read the ENTITY ROOT (entity_dir), not the partition dir (part)
                    if _is_delta_table_dir(entity_dir):
                        candidate = spark.read.format("delta").load(spark_path(entity_dir)).where(f"dt = '{dt_str}'")
                    else:
                        candidate = spark.read.format("delta").load(spark_path(part))

                    candidate.limit(1).collect()
                    df = candidate

                except Exception:
                    # legacy parquet dt-folders
                    if has_files_with_suffix(part, ".parquet"):
                        df = spark.read.format("parquet").load(spark_path(part))

            if df is None:
                # default parquet
                df = spark.read.format("parquet").load(spark_path(part))

            from pyspark.sql import functions as F
            if "dt" not in df.columns:
                df = df.withColumn("dt", F.lit(dt_str))
            if "source" not in df.columns:
                df = df.withColumn("source", F.lit(source))

            full_name = f"{catalog}.{schema}.{source}_{entity}"
            _write_managed(spark, df, full_name, dt)


def _publish_bronze_parsed(spark, dt: date, catalog: str, schema: str) -> None:
    base = bronze_root() / "parsed"
    if not path_exists(base):
        return
    for source_dir in list_dir(base, dirs_only=True):
        source = source_dir.name
        for entity_dir in list_dir(source_dir, dirs_only=True):
            entity = entity_dir.name
            part = entity_dir / f"dt={dt.isoformat()}"
            if not path_exists(part):
                continue
            df = spark.read.format("parquet").load(spark_path(part))
            from pyspark.sql import functions as F
            if "dt" not in df.columns:
                df = df.withColumn("dt", F.lit(dt.isoformat()))
            if "source" not in df.columns:
                df = df.withColumn("source", F.lit(source))
            if "entity" not in df.columns:
                df = df.withColumn("entity", F.lit(entity))
            full_name = f"{catalog}.{schema}.{source}_{entity}"
            _write_managed(spark, df, full_name, dt)


def _write_managed(spark, df, full_name: str, dt: date) -> None:
    if not _table_exists(spark, full_name):
        if "dt" in df.columns:
            df.write.format("delta").mode("overwrite").partitionBy("dt").saveAsTable(full_name)
        else:
            df.write.format("delta").mode("overwrite").saveAsTable(full_name)
        return

    # Overwrite only the dt partition
    if "dt" in df.columns:
        df.write.format("delta").mode("overwrite").option("replaceWhere", f"dt = '{dt.isoformat()}'").option(
            "overwriteSchema", "true"
        ).saveAsTable(full_name)
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)


def _publish_bronze_requests(spark, dt: date, catalog: str, schema: str) -> None:
    base = bronze_root() / "requests"
    if not path_exists(base):
        return
    part = base / f"dt={dt.isoformat()}"
    if not path_exists(part):
        return
    df = spark.read.format("parquet").load(spark_path(part))
    from pyspark.sql import functions as F
    if "dt" not in df.columns:
        df = df.withColumn("dt", F.lit(dt.isoformat()))
    full_name = f"{catalog}.{schema}.requests"
    _write_managed(spark, df, full_name, dt)