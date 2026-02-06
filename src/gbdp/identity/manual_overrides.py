from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List


def load_manual_overrides(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row for row in reader if row.get("canonical_id")]

