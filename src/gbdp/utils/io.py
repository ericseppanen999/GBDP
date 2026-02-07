from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional


def _normalize_path(value: str) -> Path:
    if value.startswith("dbfs:/"):
        value = "/dbfs/" + value.replace("dbfs:/", "", 1)
    return Path(value).resolve()


def data_root() -> Path:
    return _normalize_path(os.getenv("GBDP_DATA_ROOT", "./data"))


def bronze_root() -> Path:
    return _normalize_path(os.getenv("GBDP_BRONZE_ROOT", str(data_root() / "bronze")))


def silver_root() -> Path:
    return _normalize_path(os.getenv("GBDP_SILVER_ROOT", str(data_root() / "silver")))


def gold_root() -> Path:
    return _normalize_path(os.getenv("GBDP_GOLD_ROOT", str(data_root() / "gold")))


def manual_root() -> Path:
    return _normalize_path(os.getenv("GBDP_MANUAL_ROOT", str(data_root() / "manual")))


def ensure_dir(path: Path) -> None:
    if _is_dbfs_path(path) and not _dbfs_fuse_available():
        dbutils = _dbutils_fs()
        if dbutils is None:
            raise RuntimeError("dbutils is required to create DBFS directories")
        dbutils.fs.mkdirs(_to_dbfs_uri(path))
        return
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError:
        if _is_dbfs_path(path):
            dbutils = _dbutils_fs()
            if dbutils is None:
                raise
            dbutils.fs.mkdirs(_to_dbfs_uri(path))
            return
        raise


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def request_hash(url: str, params: Dict[str, Any] | None) -> str:
    payload = {"url": url, "params": params or {}}
    return sha256_bytes(stable_json_dumps(payload).encode("utf-8"))


def include_partition_cols_silver() -> bool:
    return os.getenv("GBDP_INCLUDE_PARTITION_COLS_SILVER", "false").lower() in (
        "1",
        "true",
        "yes",
    )


def storage_format() -> str:
    return os.getenv("GBDP_STORAGE_FORMAT", "parquet").lower()


def _dbfs_fuse_available() -> bool:
    p = Path("/dbfs")
    if not p.exists():
        return False
    try:
        # Some serverless environments expose /dbfs but forbid access.
        _ = next(p.iterdir(), None)
        return True
    except Exception:
        return False


def _allow_local_dbfs_io() -> bool:
    # Serverless forbids local filesystem access; default to safe behavior.
    return os.getenv("GBDP_ALLOW_DBFS_LOCAL_IO", "false").lower() in ("1", "true", "yes")


def _is_dbfs_path(path: Path) -> bool:
    p = path.as_posix()
    return p.startswith("/dbfs/") or p.startswith("/Volumes/")


def _to_dbfs_uri(path: Path) -> str:
    path_str = path.as_posix()
    if path_str.startswith("/dbfs/"):
        return "dbfs:/" + path_str[len("/dbfs/") :]
    if path_str.startswith("/Volumes/"):
        return "dbfs:/" + path_str.lstrip("/")
    return path_str


def is_dbfs_path(path: Path) -> bool:
    return _is_dbfs_path(path)


def dbfs_fuse_available() -> bool:
    return _dbfs_fuse_available()


def dbfs_uri(path: Path) -> str:
    if _is_dbfs_path(path):
        return _to_dbfs_uri(path)
    return str(path)


def uc_location(path: Path) -> str:
    # Unity Catalog table LOCATION should use /Volumes/... (no scheme)
    p = path.as_posix()
    if p.startswith("/dbfs/"):
        p = "/" + p[len("/dbfs/") :]
    if p.startswith("dbfs:/"):
        p = "/" + p[len("dbfs:/") :]
    if p.startswith("/Volumes/"):
        return p
    return p


def _from_dbfs_uri(uri: str) -> Path:
    if uri.startswith("dbfs:/"):
        return Path("/dbfs/" + uri[len("dbfs:/") :])
    return Path(uri)


def _dbutils_fs():
    try:
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils
    except Exception:
        return None
    spark = SparkSession.builder.getOrCreate()
    return DBUtils(spark)


def _dbfs_cp(src: str, dst: str, overwrite: bool = False) -> None:
    dbutils = _dbutils_fs()
    if dbutils is None:
        raise RuntimeError("dbutils is required to copy files to DBFS")
    if overwrite:
        try:
            dbutils.fs.rm(dst, True)
        except Exception:
            pass
    dbutils.fs.cp(src, dst)


def _dbfs_put(dst: str, text: str, overwrite: bool = False) -> None:
    dbutils = _dbutils_fs()
    if dbutils is None:
        raise RuntimeError("dbutils is required to write DBFS files")
    dbutils.fs.put(dst, text, overwrite)


def spark_path(path: Path) -> str:
    if _is_dbfs_path(path):
        return _to_dbfs_uri(path)
    return str(path)


def path_exists(path: Path) -> bool:
    if _is_dbfs_path(path) and not _dbfs_fuse_available():
        dbutils = _dbutils_fs()
        if dbutils is None:
            return False
        try:
            dbutils.fs.ls(_to_dbfs_uri(path))
            return True
        except Exception:
            return False
    try:
        return path.exists()
    except OSError:
        if _is_dbfs_path(path):
            dbutils = _dbutils_fs()
            if dbutils is None:
                return False
            try:
                dbutils.fs.ls(_to_dbfs_uri(path))
                return True
            except Exception:
                return False
        return False


def list_dir(path: Path, dirs_only: bool = False) -> List[Path]:
    if _is_dbfs_path(path) and not _dbfs_fuse_available():
        dbutils = _dbutils_fs()
        if dbutils is None:
            return []
        try:
            entries = dbutils.fs.ls(_to_dbfs_uri(path))
        except Exception:
            return []
        results: List[Path] = []
        for e in entries:
            is_dir = False
            if hasattr(e, "isDir"):
                try:
                    is_dir = e.isDir()
                except Exception:
                    try:
                        is_dir = bool(e.isDir)
                    except Exception:
                        is_dir = False
            if dirs_only and not is_dir:
                continue
            results.append(_from_dbfs_uri(e.path))
        return results
    try:
        if dirs_only:
            return [p for p in path.iterdir() if p.is_dir()]
        return list(path.iterdir())
    except Exception:
        return []


def file_size(path: Path) -> int | None:
    if _is_dbfs_path(path) and not _dbfs_fuse_available():
        dbutils = _dbutils_fs()
        if dbutils is None:
            return None
        try:
            entries = dbutils.fs.ls(_to_dbfs_uri(path))
        except Exception:
            return None
        if not entries:
            return 0
        # For files, ls returns a single entry with size; for dirs, size is 0.
        if len(entries) == 1:
            try:
                return int(entries[0].size)
            except Exception:
                return None
        return 0
    try:
        return path.stat().st_size
    except Exception:
        return None


def read_bytes(path: Path) -> bytes:
    if _is_dbfs_path(path):
        if not _allow_local_dbfs_io():
            raise RuntimeError("Local DBFS reads are disabled on serverless; use Spark for DBFS paths.")
        if not _dbfs_fuse_available():
            dbutils = _dbutils_fs()
            if dbutils is None:
                raise RuntimeError("dbutils is required to read DBFS files")
            tmp = tempfile.NamedTemporaryFile(delete=False)
            tmp.close()
            dbutils.fs.cp(_to_dbfs_uri(path), f"file:{tmp.name}")
            with open(tmp.name, "rb") as f:
                data = f.read()
            try:
                os.unlink(tmp.name)
            except Exception:
                pass
            return data
    with open(path, "rb") as f:
        return f.read()


def write_bytes(path: Path, data: bytes, force: bool = False) -> Path:
    if not force and path_exists(path):
        return path
    if _is_dbfs_path(path):
        # Serverless blocks local fs access; only text writes are supported here.
        try:
            text = data.decode("utf-8")
        except Exception as exc:
            raise RuntimeError("Binary DBFS write is not supported on serverless; use Spark writers.") from exc
        _dbfs_put(_to_dbfs_uri(path), text, overwrite=True)
        return path
    try:
        ensure_dir(path.parent)
        with open(path, "wb") as f:
            f.write(data)
        return path
    except OSError:
        if _is_dbfs_path(path):
            # Never use local file copy on serverless.
            try:
                text = data.decode("utf-8")
            except Exception as exc:
                raise RuntimeError("Binary DBFS write is not supported on serverless; use Spark writers.") from exc
            _dbfs_put(_to_dbfs_uri(path), text, overwrite=True)
            return path
        raise


def read_text(path: Path, encoding: str = "utf-8") -> str:
    data = read_bytes(path)
    return data.decode(encoding)


def write_text(path: Path, text: str, encoding: str = "utf-8", force: bool = False) -> Path:
    if _is_dbfs_path(path) and not _dbfs_fuse_available():
        _dbfs_put(_to_dbfs_uri(path), text, overwrite=True)
        return path
    return write_bytes(path, text.encode(encoding), force=force)


def write_parquet_table(table: Any, path: Path, force: bool = False) -> Path:
    if not force and path_exists(path):
        return path
    if _is_dbfs_path(path):
        # Always use Spark for DBFS parquet writes (serverless blocks local fs access)
        try:
            from pyspark.sql import SparkSession
        except Exception as exc:
            raise RuntimeError("pyspark is required for parquet writes on DBFS") from exc
        spark = SparkSession.builder.getOrCreate()
        pdf = table.to_pandas()
        df = spark.createDataFrame(pdf)
        df.write.mode("overwrite").parquet(spark_path(path.parent))
        return path
    try:
        ensure_dir(path.parent)
        try:
            import pyarrow.parquet as pq
        except Exception as exc:
            raise RuntimeError("pyarrow is required for parquet writes") from exc
        pq.write_table(table, path, use_dictionary=False)
        return path
    except OSError:
        if _is_dbfs_path(path):
            dbutils = _dbutils_fs()
            if dbutils is None:
                raise
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
            try:
                import pyarrow.parquet as pq
            except Exception as exc:
                raise RuntimeError("pyarrow is required for parquet writes") from exc
            try:
                pq.write_table(table, tmp.name, use_dictionary=False)
                dbutils.fs.mkdirs(_to_dbfs_uri(path.parent))
                dbutils.fs.cp(f"file:{tmp.name}", _to_dbfs_uri(path), True)
            finally:
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass
            return path
        raise


def read_parquet_rows(path: Path) -> List[Dict[str, Any]]:
    try:
        import pyarrow as pa
        import pyarrow.dataset as ds
        import pyarrow.parquet as pq
    except Exception as exc:
        raise RuntimeError("pyarrow is required for parquet reads") from exc
    if not _is_dbfs_path(path) or _dbfs_fuse_available():
        dataset = ds.dataset(path, format="parquet")
        return dataset.to_table().to_pylist()
    dbutils = _dbutils_fs()
    if dbutils is None:
        return []
    rows: List[Dict[str, Any]] = []
    try:
        entries = dbutils.fs.ls(_to_dbfs_uri(path))
    except Exception:
        return rows
    for entry in entries:
        if not entry.path.lower().endswith(".parquet"):
            continue
        data = read_bytes(_from_dbfs_uri(entry.path))
        table = pq.read_table(pa.BufferReader(data))
        rows.extend(table.to_pylist())
    return rows
