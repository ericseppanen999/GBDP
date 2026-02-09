from __future__ import annotations

from datetime import date
from typing import List
import os
import re

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


def _sanitize_for_uc(df):
    """
    Make DF safe for UC-managed Delta tables:
    - Convert complex types (struct/map/array) to JSON strings (avoids nested-field name + schema evolution pain)
    - Rename columns to remove characters Delta rejects unless column mapping is enabled. :contentReference[oaicite:1]{index=1}
    - Ensure unique column names after renaming
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, MapType, ArrayType

    # 1) stringify complex types
    for field in df.schema.fields:
        if isinstance(field.dataType, (StructType, MapType, ArrayType)):
            df = df.withColumn(field.name, F.to_json(F.col(field.name)))

    # 2) sanitize top-level column names
    invalid_re = re.compile(r"[ ,;{}()\n\t=]")
    seen = set()
    renames = {}

    for i, c in enumerate(df.columns):
        name = "" if c is None else str(c)

        # Replace known-invalid chars + also anything non [A-Za-z0-9_]
        new = invalid_re.sub("_", name).strip()
        new = re.sub(r"[^0-9A-Za-z_]", "_", new)
        new = re.sub(r"_+", "_", new).strip("_")

        if new == "" or new == ".":
            new = f"col_{i}"
        if new[0].isdigit():
            new = f"c_{new}"

        base = new
        j = 1
        while new in seen:
            new = f"{base}_{j}"
            j += 1
        seen.add(new)
        renames[c] = new

    for old, new in renames.items():
        if old != new:
            df = df.withColumnRenamed(old, new)

    return df


def _publish_gold(spark, dt: date, catalog: str, schema: str) -> None:
    base = gold_root()
    if not path_exists(base):
        return

    dt_str = dt.isoformat()
    fmt = storage_format()

    for table_dir in list_dir(base, dirs_only=True):
        table = table_dir.name
        full_name = f"{catalog}.{schema}.{table}"
        df = None

        if fmt == "delta":
            # Read from DELTA TABLE ROOT, not dt= partition folder
            try:
                candidate = spark.read.format("delta").load(spark_path(table_dir)).where(f"dt = '{dt_str}'")
                candidate.limit(1).collect()
                df = candidate
            except Exception:
                df = None

        if df is None:
            # Parquet fallback: read partition folder
            part = table_dir / f"dt={dt_str}"
            if not path_exists(part) or not has_files_with_suffix(part, ".parquet"):
                continue
            df = spark.read.format("parquet").load(spark_path(part))

        from pyspark.sql import functions as F
        if "dt" not in df.columns:
            df = df.withColumn("dt", F.lit(dt_str))

        _write_managed(spark, df, full_name, dt)


def _publish_silver(spark, dt: date, catalog: str, schema: str) -> None:
    base = silver_root()
    if not path_exists(base):
        return

    dt_str = dt.isoformat()
    fmt = storage_format()

    for source_dir in list_dir(base, dirs_only=True):
        source = source_dir.name
        for entity_dir in list_dir(source_dir, dirs_only=True):
            entity = entity_dir.name
            full_name = f"{catalog}.{schema}.{source}_{entity}"
            df = None

            if fmt == "delta":
                # Read from DELTA TABLE ROOT, not dt= partition folder
                try:
                    candidate = spark.read.format("delta").load(spark_path(entity_dir)).where(f"dt = '{dt_str}'")
                    candidate.limit(1).collect()
                    df = candidate
                except Exception:
                    df = None

            if df is None:
                # Parquet fallback: read partition folder
                part = entity_dir / f"dt={dt_str}"
                if not path_exists(part) or not has_files_with_suffix(part, ".parquet"):
                    continue
                df = spark.read.format("parquet").load(spark_path(part))

            from pyspark.sql import functions as F
            if "dt" not in df.columns:
                df = df.withColumn("dt", F.lit(dt_str))
            if "source" not in df.columns:
                df = df.withColumn("source", F.lit(source))

            _write_managed(spark, df, full_name, dt)


def _publish_bronze_parsed(spark, dt: date, catalog: str, schema: str) -> None:
    base = bronze_root() / "parsed"
    if not path_exists(base):
        return

    dt_str = dt.isoformat()

    for source_dir in list_dir(base, dirs_only=True):
        source = source_dir.name
        for entity_dir in list_dir(source_dir, dirs_only=True):
            entity = entity_dir.name
            part = entity_dir / f"dt={dt_str}"
            if not path_exists(part):
                continue
            df = spark.read.format("parquet").load(spark_path(part))
            from pyspark.sql import functions as F
            if "dt" not in df.columns:
                df = df.withColumn("dt", F.lit(dt_str))
            if "source" not in df.columns:
                df = df.withColumn("source", F.lit(source))
            if "entity" not in df.columns:
                df = df.withColumn("entity", F.lit(entity))
            full_name = f"{catalog}.{schema}.{source}_{entity}"
            _write_managed(spark, df, full_name, dt)


def _publish_bronze_requests(spark, dt: date, catalog: str, schema: str) -> None:
    base = bronze_root() / "requests"
    if not path_exists(base):
        return

    dt_str = dt.isoformat()
    part = base / f"dt={dt_str}"
    if not path_exists(part):
        return

    df = spark.read.format("parquet").load(spark_path(part))
    from pyspark.sql import functions as F
    if "dt" not in df.columns:
        df = df.withColumn("dt", F.lit(dt_str))
    full_name = f"{catalog}.{schema}.requests"
    _write_managed(spark, df, full_name, dt)


def _write_managed(spark, df, full_name: str, dt: date) -> None:
    from pyspark.sql import functions as F

    dt_str = dt.isoformat()
    if "dt" not in df.columns:
        df = df.withColumn("dt", F.lit(dt_str))

    # Make UC-safe (fix invalid columns + stabilize complex types)
    df = _sanitize_for_uc(df)

    # Create table if missing
    if not _table_exists(spark, full_name):
        df.write.format("delta").mode("overwrite").partitionBy("dt").saveAsTable(full_name)
        return

    # Try overwrite just this dt partition (NO overwriteSchema here; not allowed with replaceWhere)
    try:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"dt = '{dt_str}'")
            .saveAsTable(full_name)
        )
        return
    except Exception as e:
        msg = str(e).lower()

        # UC schema mismatch / ACL blocks automerge -> one-time full overwrite with overwriteSchema
        if "schema mismatch" in msg or "schema migration is not allowed" in msg or "_legacy_error_temp_delta_0007" in msg:
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("dt")
                .saveAsTable(full_name)
            )
            return

        raise