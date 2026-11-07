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
    days = list(daterange(parse_date(start), parse_date(end)))
    errors: List[str] = []
    # All gold first: an error in a lower layer must not hold back later gold dates.
    for d in days:
        _publish_gold(spark, d, catalog, gold_schema, errors)
    for d in days:
        _publish_silver(spark, d, catalog, silver_schema, errors)
        _publish_bronze_parsed(spark, d, catalog, bronze_schema, errors)
        _publish_bronze_requests(spark, d, catalog, bronze_schema, errors)
    if errors:
        # Schema clashes come out the same on every attempt; a retry would only
        # re-publish the whole window again (~45 min each, confirmed live).
        raise PermanentStageError(f"{len(errors)} UC table write(s) failed: " + " | ".join(errors[:10]))


# Deterministic: the same partition hits the same clash on every attempt.
_SCHEMA_CLASH_MARKERS = (
    "delta_failed_to_merge_fields",
    "schema mismatch",
    "schema migration is not allowed",
    "_legacy_error_temp_delta_0007",
    "cannot_up_cast",
)


def _try_write(spark, df, full_name: str, dt: date, errors: List[str]) -> None:
    """Write one table; a schema clash is recorded so the other tables still publish."""
    try:
        _write_managed(spark, df, full_name, dt)
    except Exception as e:
        msg = str(e)
        if not any(m in msg.lower() for m in _SCHEMA_CLASH_MARKERS):
            raise
        errors.append(f"{full_name} dt={dt.isoformat()}: {msg.splitlines()[0][:200]}")


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


def _publish_gold(spark, dt: date, catalog: str, schema: str, errors: List[str]) -> None:
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
            part = table_dir / f"dt={dt_str}"
            if not path_exists(part):
                continue
            # Some partition folders are themselves delta tables (nested delta log).
            if path_exists(part / "_delta_log"):
                try:
                    df = spark.read.format("delta").load(spark_path(part))
                except Exception:
                    continue
            elif has_files_with_suffix(part, ".parquet"):
                try:
                    df = spark.read.format("parquet").load(spark_path(part))
                except Exception as e:
                    if "DELTA_INVALID_FORMAT" in str(e) or "delta_invalid_format" in str(e).lower():
                        try:
                            df = spark.read.format("delta").load(spark_path(part))
                        except Exception:
                            continue
                    else:
                        continue
            else:
                continue

        from pyspark.sql import functions as F
        if "dt" not in df.columns:
            df = df.withColumn("dt", F.lit(dt_str))

        _try_write(spark, df, full_name, dt, errors)


def _publish_silver(spark, dt: date, catalog: str, schema: str, errors: List[str]) -> None:
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

            _try_write(spark, df, full_name, dt, errors)


def _publish_bronze_parsed(spark, dt: date, catalog: str, schema: str, errors: List[str]) -> None:
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
            _try_write(spark, df, full_name, dt, errors)


def _publish_bronze_requests(spark, dt: date, catalog: str, schema: str, errors: List[str]) -> None:
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
    _try_write(spark, df, full_name, dt, errors)


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

    # Try overwrite just this dt partition (NO overwriteSchema here; not allowed with replaceWhere).
    # mergeSchema IS allowed with replaceWhere and is what actually lets new columns show up
    # going forward instead of being silently dropped to match whatever schema the table
    # happened to get on its first (possibly empty/narrow) write.
    def _write_partition(df):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"dt = '{dt_str}'")
            .option("mergeSchema", "true")
            .saveAsTable(full_name)
        )

    try:
        _write_partition(df)
        return
    except Exception as e:
        msg = str(e).lower()

        # UC schema mismatch / ACL blocks automerge -> widen the schema additively
        # (ALTER TABLE ADD COLUMNS never touches existing rows or other partitions)
        # and retry the same partition-scoped write. Do NOT fall back to a bare
        # mode("overwrite") here without replaceWhere -- that replaces the ENTIRE
        # table with just this partition's rows, silently destroying every other
        # date ever written to it.
        #
        # unresolved_column / cannot be resolved: confirmed live -- a target
        # table that has NEVER had a real write (e.g. breakout_candidates on a
        # day it happened to have zero rows, before any real schema was ever
        # established) can end up registered in UC with a genuinely empty
        # schema. replaceWhere's "dt = ..." predicate then fails to resolve
        # `dt` at all (a more fundamental case of the same "table doesn't
        # have the columns this write needs yet" problem), not just a field-
        # level mismatch. ADD COLUMNS handles going from zero columns to N
        # the same way it handles adding one missing column.
        if (
            "schema mismatch" in msg
            or "schema migration is not allowed" in msg
            or "_legacy_error_temp_delta_0007" in msg
            or "delta_failed_to_merge_fields" in msg
            or "unresolved_column" in msg
            or "cannot be resolved" in msg
        ):
            # delta_failed_to_merge_fields is a TYPE clash, which ADD COLUMNS
            # can't fix: confirmed live on silver npb_spaia_games, created from
            # an all-STRING placeholder write and then fed real bigint scores.
            df = _align_to_table_types(spark, full_name, df)
            _add_missing_columns(spark, full_name, df)
            _write_partition(df)
            return

        raise


def _lossless_cast(target, incoming) -> bool:
    """True when every value of type `incoming` survives a cast to `target`."""
    from pyspark.sql.types import (
        AtomicType, ByteType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType,
    )

    if isinstance(incoming, NullType):
        return True
    if isinstance(target, StringType):
        return isinstance(incoming, AtomicType)
    if isinstance(target, LongType):
        return isinstance(incoming, (IntegerType, ShortType, ByteType))
    if isinstance(target, DoubleType):
        # LongType is exact up to 2^53, far past any ID or count here. Confirmed
        # live: kbo_api_games was born DOUBLE from a sparse fetch, later bigint.
        return isinstance(incoming, (FloatType, IntegerType, LongType, ShortType, ByteType))
    return False


def _align_to_table_types(spark, full_name: str, df):
    """Cast incoming columns to the table's existing type wherever no value is lost.

    Never rewrites the table, so other partitions are untouched; a clash that
    can't be cast losslessly is left alone and the retry fails loudly.
    """
    from pyspark.sql import functions as F

    target = {f.name: f.dataType for f in spark.table(full_name).schema.fields}
    cols = []
    for f in df.schema.fields:
        t = target.get(f.name)
        if t is not None and t != f.dataType and _lossless_cast(t, f.dataType):
            cols.append(F.col(f"`{f.name}`").cast(t).alias(f.name))
        else:
            cols.append(F.col(f"`{f.name}`"))
    return df.select(*cols)


def _add_missing_columns(spark, full_name: str, df) -> None:
    existing = {f.name for f in spark.table(full_name).schema.fields}
    new_fields = [f for f in df.schema.fields if f.name not in existing]
    if not new_fields:
        return
    ddl = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in new_fields)
    spark.sql(f"ALTER TABLE {full_name} ADD COLUMNS ({ddl})")