from gbdp.utils.ids import _CROCKFORD, ulid_from_key


def test_ulid_deterministic():
    assert ulid_from_key("player:mlb:123") == ulid_from_key("player:mlb:123")


def test_ulid_differs_by_key():
    assert ulid_from_key("player:mlb:123") != ulid_from_key("player:mlb:124")


def test_ulid_has_ulid_shape():
    value = ulid_from_key("player:mlb:123")
    assert len(value) == 26
    assert set(value) <= set(_CROCKFORD)


def test_ulid_uses_salt(monkeypatch):
    default = ulid_from_key("player:mlb:123")
    monkeypatch.setenv("GBDP_ID_SALT", "other")
    assert ulid_from_key("player:mlb:123") != default
