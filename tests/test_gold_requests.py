"""Gold-layer requests from the semantic layer (SF/SH, dim_team/dim_player for
kbo_api, bridge dedupe, fact_game.game_type) and review items CR-018/CR-019."""
from __future__ import annotations

import json
from datetime import date

import pytest

from gbdp.gold import publish


def _pitch(event, outs, ab=1, n=1, inning=9):
    return {"game_pk": 7, "at_bat_number": ab, "pitch_number": n, "inning": inning, "inning_topbot": "Top",
            "outs_when_up": outs, "events": event, "batter_id": 5, "pitcher_id": 6,
            "raw_json": json.dumps({"bat_score": 0, "post_bat_score": 0})}


# CR-019: a game-ending sacrifice double play is two outs, not one.
@pytest.mark.parametrize("event,outs_before,expected_type,expected_after", [
    ("sac_fly_double_play", 1, "SF", 3),
    ("sac_bunt_double_play", 1, "SH", 3),
    ("strikeout_double_play", 1, "K", 3),
    ("sac_fly", 2, "SF", 3),
    ("grounded_into_double_play", 1, "DP", 3),
    ("triple_play", 0, "TP", 3),
])
def test_final_pa_sacrifice_double_play_ends_the_game(event, outs_before, expected_type, expected_after):
    pa, = publish._statcast_plate_appearances([_pitch(event, outs_before)], date(2026, 6, 10), {})
    assert pa["event_type"] == expected_type
    assert pa["outs_after"] == expected_after
    assert pa["base_state_after"] == 0


def test_final_pa_walk_off_sacrifice_fly_stays_unknown_bases():
    # A walk-off sac fly with 0 out leaves 1 out; the bases after are unknown.
    pa, = publish._statcast_plate_appearances([_pitch("sac_fly", 0)], date(2026, 6, 10), {})
    assert pa["outs_after"] == 1
    assert pa["base_state_after"] is None
