from gbdp.gold.publish import _base_state_from_statcast


def test_base_state_from_statcast():
    r = {"on_1b": 1, "on_2b": None, "on_3b": 3}
    assert _base_state_from_statcast(r) == 1 + 4
