from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Dict, Optional

from gbdp.utils.io import ensure_dir, bronze_root


class ResponseCache:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or (bronze_root() / "cache")
        ensure_dir(self.root)

    def _path(self, key: str) -> Path:
        return self.root / f"{key}.json.gz"

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        path = self._path(key)
        if not path.exists():
            return None
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)

    def set(self, key: str, value: Dict[str, Any]) -> None:
        path = self._path(key)
        ensure_dir(path.parent)
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(value, f, ensure_ascii=True)
