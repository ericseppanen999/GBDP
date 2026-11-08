"""configs/npb_text.yaml: what the Japanese in SPAIA's NPB feed means.

Shared by silver (roster roles and positions) and gold (play-by-play outs,
runners and events), so both read one definition.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict


@lru_cache(maxsize=None)
def npb_text() -> Dict[str, Any]:
    import yaml

    with Path("configs/npb_text.yaml").open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["events"] = [(e["event"], tuple(e["phrases"])) for e in cfg["events"]]
    cfg["staff_kinds"] = {str(k): v for k, v in cfg.get("staff_kinds", {}).items()}
    return cfg
