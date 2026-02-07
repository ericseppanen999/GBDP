from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Dict, Optional

from gbdp.utils.io import bronze_root, ensure_dir, path_exists, read_bytes, write_bytes


class ResponseCache:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or (bronze_root() / "cache")
        ensure_dir(self.root)

    def _path(self, key: str) -> Path:
        return self.root / f"{key}.json.gz"

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        path = self._path(key)
        if not path_exists(path):
            return None
        data = read_bytes(path)
        return json.loads(gzip.decompress(data).decode("utf-8"))

    def set(self, key: str, value: Dict[str, Any]) -> None:
        path = self._path(key)
        ensure_dir(path.parent)
        payload = json.dumps(value, ensure_ascii=True).encode("utf-8")
        compressed = gzip.compress(payload)
        write_bytes(path, compressed, force=True)
