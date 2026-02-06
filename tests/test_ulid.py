from gbdp.utils.ids import ulid_from_key


def test_ulid_deterministic():
    a = ulid_from_key("player:mlb:123", "2023-01-01")
    b = ulid_from_key("player:mlb:123", "2023-01-01")
    assert a == b
