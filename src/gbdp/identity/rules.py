from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class MatchResult:
    canonical_key: str
    confidence: float
    method: str


def player_match_key(record: Dict[str, str]) -> Optional[MatchResult]:
    source_id = record.get("source_player_id")
    if source_id:
        return MatchResult(f"player:source:{record['source']}:{source_id}", 0.9, "exact_id")
    name = record.get("name")
    dob = record.get("dob")
    if name and dob:
        return MatchResult(f"player:name_dob:{name.lower()}:{dob}", 0.75, "exact_name_dob")
    team = record.get("team_id")
    if name and team:
        return MatchResult(f"player:name_team:{name.lower()}:{team}", 0.5, "name_team")
    return None


def team_match_key(record: Dict[str, str]) -> Optional[MatchResult]:
    source_id = record.get("source_team_id")
    if source_id:
        return MatchResult(f"team:source:{record['source']}:{source_id}", 0.9, "exact_id")
    name = record.get("name")
    league = record.get("league_code")
    if name and league:
        return MatchResult(f"team:name_league:{name.lower()}:{league}", 0.7, "exact_name_league")
    return None

