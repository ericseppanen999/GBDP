import pytest

from gbdp.gold.publish import _map_event_type


def test_event_mapping():
    assert _map_event_type("home_run") == "HR"
    assert _map_event_type("walk") == "BB"
    assert _map_event_type("strikeout") == "K"
    assert _map_event_type("single") == "1B"


@pytest.mark.parametrize(
    "event,expected",
    [
        ("hit_by_pitch", "HBP"),
        ("sac_fly", "SAC"),
        ("sac_bunt", "SAC"),
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
        ("truncated_pa", "UNKNOWN"),
    ],
)
def test_statcast_event_mapping(event, expected):
    assert _map_event_type(event) == expected
