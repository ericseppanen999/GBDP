from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Dict, Optional

from gbdp.utils.io import bronze_root, dbfs_fuse_available, is_dbfs_path, ensure_dir, path_exists, read_bytes, write_bytes, write_text


class ResponseCache:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or (bronze_root() / "cache")
        ensure_dir(self.root)

    def _path(self, key: str) -> Path:
        return self.root / f"{key}.json.gz"

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        path = self._path(key)
        if is_dbfs_path(path) and not dbfs_fuse_available():
            # Serverless cannot read local files; skip cache reads.
            return None
        if not path_exists(path):
            alt = path.with_suffix(".json")
            if not path_exists(alt):
                return None
            data = read_bytes(alt)
            return json.loads(data.decode("utf-8"))
        data = read_bytes(path)
        return json.loads(gzip.decompress(data).decode("utf-8"))

    def set(self, key: str, value: Dict[str, Any]) -> None:
        path = self._path(key)
        ensure_dir(path.parent)
        payload = json.dumps(value, ensure_ascii=True)
        if is_dbfs_path(path) and not dbfs_fuse_available():
            alt = path.with_suffix(".json")
            write_text(alt, payload, force=True)
            return
        compressed = gzip.compress(payload.encode("utf-8"))
        write_bytes(path, compressed, force=True)
