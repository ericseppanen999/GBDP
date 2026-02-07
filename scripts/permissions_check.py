def run_all() -> None:
    import os
    import sys
    import platform
    from pathlib import Path

    # Ensure repo src/ is on path when run from Databricks notebook
    try:
        import gbdp  # noqa: F401
    except Exception:
        repo_root = Path("/Workspace/Repos/eric.t.seppanen@gmail.com/GBDP")
        src_path = repo_root / "src"
        if src_path.exists():
            sys.path.insert(0, str(src_path))

    print("python", sys.version)
    print("platform", platform.platform())
    print("GBDP_UC_CATALOG", os.getenv("GBDP_UC_CATALOG"))
    print("DBFS FUSE exists?", os.path.exists("/dbfs"))

    print("\n[1] /dbfs list")
    try:
        print(os.listdir("/dbfs")[:5])
    except Exception as exc:
        print("ERROR /dbfs list:", repr(exc))

    print("\n[2] dbutils")
    try:
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        print("dbutils ok")
    except Exception as exc:
        print("ERROR dbutils:", repr(exc))
        return

    print("\n[3] dbutils ls + mkdir")
    try:
        dbutils.fs.ls("dbfs:/Volumes/gbdp/bronze_gbdp/bronze_vol/")
        dbutils.fs.mkdirs("dbfs:/Volumes/gbdp/bronze_gbdp/bronze_vol/gbdp/test_dir")
        print("mkdir ok")
    except Exception as exc:
        print("ERROR dbutils ls/mkdir:", repr(exc))

    print("\n[4] spark write/read parquet")
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame([{"a": 1, "dt": "2004-05-18"}])
        out = "dbfs:/Volumes/gbdp/bronze_gbdp/bronze_vol/gbdp/test_parquet/dt=2004-05-18"
        df.write.mode("overwrite").parquet(out)
        print("read back", spark.read.parquet(out).count())
    except Exception as exc:
        print("ERROR spark write/read:", repr(exc))

    print("\n[5] UC managed table write")
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS gbdp.bronze_gbdp")
        df = spark.createDataFrame([{"a": 1, "dt": "2004-05-18"}])
        df.write.format("delta").mode("overwrite").saveAsTable("gbdp.bronze_gbdp._perm_test")
        print("UC write ok", spark.table("gbdp.bronze_gbdp._perm_test").count())
    except Exception as exc:
        print("ERROR UC write:", repr(exc))

    print("\n[6] dbutils cp local file -> dbfs (expected fail on serverless)")
    try:
        import tempfile

        p = tempfile.NamedTemporaryFile(delete=False)
        p.write(b"hi")
        p.close()
        print("tmp", p.name)
        dbutils.fs.cp(
            f"file:{p.name}",
            "dbfs:/Volumes/gbdp/bronze_gbdp/bronze_vol/gbdp/test_dir/tmp.txt",
            True,
        )
        print("cp ok")
    except Exception as exc:
        print("ERROR dbutils cp file:", repr(exc))

    print("\n[7] HTTP GET")
    try:
        import requests

        resp = requests.get("https://example.com", timeout=10)
        print(resp.status_code, len(resp.text))
    except Exception as exc:
        print("ERROR http:", repr(exc))

    print("\n[8] write_parquet_table helper")
    try:
        from gbdp.utils.io import write_parquet_table
        import pyarrow as pa
        from pathlib import Path

        table = pa.Table.from_pylist([{"a": 1, "dt": "2004-05-18"}])
        write_parquet_table(
            table,
            Path("/Volumes/gbdp/bronze_gbdp/bronze_vol/gbdp/test_parquet/part-00001.parquet"),
            force=True,
        )
        print("write_parquet_table ok")
    except Exception as exc:
        print("ERROR write_parquet_table:", repr(exc))


if __name__ == "__main__":
    run_all()
