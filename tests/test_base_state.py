from gbdp.gold.publish import _base_state_from_statcast, _base_state_from_br


def test_base_state_from_statcast():
    r = {"on_1b": 1, "on_2b": None, "on_3b": 3}
    assert _base_state_from_statcast(r) == 1 + 4


def test_base_state_from_br():
    assert _base_state_from_br("1", None, "") == 1
    assert _base_state_from_br("1", "1", "1") == 7
