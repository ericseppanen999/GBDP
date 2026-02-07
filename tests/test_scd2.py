from datetime import date

from gbdp.gold.publish import _scd2_merge


def test_scd2_merge_new_records():
    prev = []
    new = [{"team_id": "t1", "team_name": "A"}, {"team_id": "t2", "team_name": "B"}]
    out = _scd2_merge(prev, new, "team_id", ["team_name"], date(2024, 5, 2), date(2024, 5, 1))
    assert len(out) == 2
    assert all(r["is_current"] for r in out)
    assert {r["valid_from_dt"] for r in out} == {"2024-05-02"}


def test_scd2_merge_change_closes_previous():
    prev = [{"team_id": "t1", "team_name": "A", "valid_from_dt": "2024-05-01", "valid_to_dt": None, "is_current": True}]
    new = [{"team_id": "t1", "team_name": "A2"}]
    out = _scd2_merge(prev, new, "team_id", ["team_name"], date(2024, 5, 2), date(2024, 5, 1))
    assert len(out) == 2
    closed = [r for r in out if r.get("is_current") is False][0]
    current = [r for r in out if r.get("is_current") is True][0]
    assert closed["valid_to_dt"] == "2024-05-01"
    assert current["valid_from_dt"] == "2024-05-02"
