"""configs/npb_text.yaml: what the Japanese in SPAIA's NPB feed means.

Shared by silver (roster roles and positions) and gold (play-by-play outs,
runners and events), so both read one definition.
"""
from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict


@lru_cache(maxsize=None)
def npb_text() -> Dict[str, Any]:
    import yaml

    with Path("configs/npb_text.yaml").open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Each event's phrases become one pattern. A phrase matches as a plain
    # substring; "*" inside one matches any text between its parts, in order
    # (スクイズ*本塁*アウト: a squeeze on which a runner is out at home).
    cfg["events"] = [(e["event"], phrase_pattern(e["phrases"])) for e in cfg["events"]]
    cfg["staff_kinds"] = {str(k): v for k, v in cfg.get("staff_kinds", {}).items()}
    return cfg


def phrase_pattern(phrases) -> "re.Pattern[str]":
    return re.compile("|".join(".*?".join(re.escape(part) for part in p.split("*")) for p in phrases))
