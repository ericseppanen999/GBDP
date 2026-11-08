import json

import pytest

from gbdp.gold.publish import _map_event_type, _map_retrosheet_event


def test_event_mapping():
    assert _map_event_type("home_run") == "HR"
    assert _map_event_type("walk") == "BB"
    assert _map_event_type("strikeout") == "K"
    assert _map_event_type("single") == "1B"


@pytest.mark.parametrize(
    "event,expected",
    [
        ("hit_by_pitch", "HBP"),
        ("sac_fly", "SF"),
        ("sac_bunt", "SH"),
        ("sac_fly_double_play", "SF"),
        ("sac_bunt_double_play", "SH"),
        ("fielders_choice", "FC"),
        ("fielders_choice_out", "FC"),
        ("force_out", "OUT"),
        ("field_out", "OUT"),
        ("double_play", "DP"),
        ("grounded_into_double_play", "DP"),
        ("triple_play", "TP"),
        ("strikeout_double_play", "K"),
        ("intent_walk", "BB"),
        ("field_error", "ERROR"),
        # Inning ended on the bases mid-PA: kept (it holds the third out), not a box-score PA.
        ("truncated_pa", "TRUNCATED"),
        ("catcher_interf", "INT"),
    ],
)
def test_statcast_event_mapping(event, expected):
    assert _map_event_type(event) == expected


@pytest.mark.parametrize(
    "flags,expected",
    [
        ({"sf": "1"}, "SF"),
        ({"sh": "1"}, "SH"),
        # A sacrifice stays a sacrifice (not an at-bat) when the batter also
        # reaches on an error or fielder's choice, or a runner is doubled off.
        ({"sf": "1", "roe": "1"}, "SF"),
        ({"sh": "1", "fc": "1"}, "SH"),
        ({"sf": "1", "gdp": "1"}, "SF"),
        ({"roe": "1"}, "ERROR"),
        ({"fc": "1"}, "FC"),
        ({"gdp": "1"}, "DP"),
        ({"othout": "1"}, "OUT"),
        ({}, "UNKNOWN"),
    ],
)
def test_retrosheet_event_mapping(flags, expected):
    assert _map_retrosheet_event(flags) == expected


def test_no_source_maps_to_the_old_combined_sac():
    for raw in ("sac_fly", "sac_bunt", "sac_fly_double_play", "sac_bunt_double_play"):
        assert _map_event_type(raw) in {"SF", "SH"}
    assert _map_retrosheet_event({"sf": "1"}) != "SAC"


def test_sacrifices_end_a_game_with_one_out():
    from gbdp.gold.publish import _EVENT_OUTS
    assert _EVENT_OUTS["SF"] == 1 and _EVENT_OUTS["SH"] == 1
    assert "SAC" not in _EVENT_OUTS


def test_dashboard_batting_average_excludes_both_sacrifices():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parent.parent / "scripts" / "build_dashboard.py"
    spec = importlib.util.spec_from_file_location("build_dashboard", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sql = json.dumps(mod.build())
    assert "'SAC'" not in sql
    assert "NOT IN ('BB', 'HBP', 'SF', 'SH')" in sql
