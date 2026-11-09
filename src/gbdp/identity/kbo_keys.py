"""Source IDs for kbo_api players, which the KBO API identifies by name only.

The box-score payload is a rendered table: name, team and stats, no player
code. Names collide in practice, on opposing teams (박건우 for LT and NC, in
the same game) and on one team (Samsung's two pitchers named 이승현). So the
source ID is team + name, and when one team lists the same name twice in a
game the later one gets "#2", "#3", ... in a fixed order (by the row's stats),
so a game's rows stay unique and a rebuild gives the same IDs.

Limit: across games two same-name teammates can't be told apart, so "#1" is
whichever sorts first that day, and a player who changes team gets a new ID.
Identity, dim_player and fact_boxscore_* all key through kbo_api_player_ids, so
they agree.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional


# Not part of the order: raw_json carries the silver file path, which changes
# whenever silver is rewritten, and these are bookkeeping, not the box score.
_UNSTABLE = {"raw_json", "ingested_at_utc", "dt", "source"}


def _stats_key(r: Dict[str, Any]) -> str:
    return json.dumps({k: v for k, v in r.items() if k not in _UNSTABLE}, sort_keys=True, ensure_ascii=False, default=str)


def kbo_api_player_ids(rows: Iterable[Dict[str, Any]]) -> List[Optional[str]]:
    """One source ID per row, in the rows' order; None where the row has no name."""
    rows = list(rows)
    ids: List[Optional[str]] = [None] * len(rows)
    groups: Dict[tuple, List[int]] = {}
    for i, r in enumerate(rows):
        name = r.get("player_name")
        if not name:
            continue
        groups.setdefault((r.get("game_id"), r.get("team_id"), name), []).append(i)
    for (_game, team, name), idx in groups.items():
        base = f"{team}:{name}" if team else str(name)
        ordered = sorted(idx, key=lambda i: _stats_key(rows[i]))
        for n, i in enumerate(ordered, start=1):
            ids[i] = base if n == 1 else f"{base}#{n}"
    return ids
