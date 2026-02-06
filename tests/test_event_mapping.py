from gbdp.gold.publish import _map_event_type


def test_event_mapping():
    assert _map_event_type("home_run") == "HR"
    assert _map_event_type("walk") == "BB"
    assert _map_event_type("strikeout") == "K"
    assert _map_event_type("single") == "1B"
