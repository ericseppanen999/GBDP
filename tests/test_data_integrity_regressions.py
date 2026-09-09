"""
Regression tests for silent data-corruption bugs found and fixed on 2026-09-08/09.
Each test reproduces the exact failure mode that was confirmed in production
before its fix, so these bugs can't come back unnoticed.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from gbdp.bronze.writer import BronzeWriter, RawPayload
from gbdp.quality.checks import _write_parquet
from gbdp.utils.io import read_parquet_rows, _drop_empty_sentinel


def _payload(**overrides) -> RawPayload:
    base = dict(
        source="test_source",
        entity="test_entity",
        dt="2025-01-01",
        url="https://example.invalid/api",
        params={},
        status_code=200,
        fetched_at_utc="2025-01-01T00:00:00Z",
        checksum="deadbeef",
        content_type="application/json",
        body_text="{}",
    )
    base.update(overrides)
    return RawPayload(**base)


# --- Bug 1: pa.Table.from_pylist infers its schema from a subset of rows ---
# (effectively the first one) rather than unioning all of them, so fields
# that only appear in later, differently-shaped rows are dropped from the
# schema ENTIRELY -- not just nulled. Confirmed on audit_quality: 'count',
# 'min_count', and 'ok' vanished completely as soon as a 'uniqueness' row
# (which lacks them) was written before a 'row_count_min' row.
def test_write_parquet_preserves_heterogeneous_row_fields(tmp_path):
    rows = [
        {"check": "uniqueness", "table": "fact_game", "columns": "game_id", "dupes": 0, "dt": "2025-04-01"},
        {"check": "row_count_min", "table": "fact_game", "count": 15, "min_count": 1, "ok": True, "dt": "2025-04-01"},
    ]
    out = tmp_path / "audit_quality.parquet"
    _write_parquet(rows, out)

    written = pq.ParquetFile(str(out)).read().to_pylist()
    row_count_min_row = next(r for r in written if r["check"] == "row_count_min")
    assert "count" in row_count_min_row, "'count' field must not be dropped from the schema entirely"
    assert row_count_min_row["count"] == 15
    assert row_count_min_row["min_count"] == 1
    assert row_count_min_row["ok"] is True


# --- Bug 2: same schema-drop bug, but at the bronze ingestion boundary -------
# (BronzeWriter._to_table), where individual API records can have sparse or
# optional fields. Corruption here propagates through the entire pipeline
# since bronze is the first write.
def test_bronze_to_table_preserves_sparse_record_fields():
    writer = BronzeWriter(root=Path("unused"))
    payload = _payload()
    records = [
        {"a": 1, "b": "x"},
        {"a": 2, "c": "y"},  # different shape: missing 'b', has 'c' instead
    ]
    table = writer._to_table(payload, records)
    rows = table.to_pylist()

    row_a1 = next(r for r in rows if r["a"] == 1)
    row_a2 = next(r for r in rows if r["a"] == 2)
    assert row_a1["b"] == "x"
    assert "c" in row_a2, "'c' must not be dropped from the schema just because row 1 lacked it"
    assert row_a2["c"] == "y"


# --- Bug 3: pyarrow's dataset reader infers its schema from a sample of -----
# files rather than unioning all of them, so a column present only in files
# it didn't sample gets silently dropped from the ENTIRE scan -- including
# files that do have it. Confirmed with the real mlb_statcast bronze
# partition: two files, 82 vs 124 columns, "game_pk" vanished from all 3966
# rows even though 3704 of them genuinely had it.
def test_read_parquet_rows_unions_schema_across_multiple_files(tmp_path):
    import pyarrow as pa

    narrow = pa.Table.from_pylist([{"x": 1, "y": "a"}])
    wide = pa.Table.from_pylist([{"x": 2, "y": "b", "game_pk": "778492"}])

    partition = tmp_path / "dt=2025-01-01"
    partition.mkdir()
    # Name matters: pyarrow samples in some file order, so use a name that
    # sorts BEFORE the wide file to reproduce the exact failure ordering.
    pq.write_table(narrow, partition / "1_narrow.parquet")
    pq.write_table(wide, partition / "2_wide.parquet")

    rows = read_parquet_rows(partition)
    assert len(rows) == 2
    game_pks = {r.get("game_pk") for r in rows}
    assert "778492" in game_pks, "column present in only one file must survive the union"


# --- Bug 4: writers emit a single {"empty": True} row instead of a zero-row -
# file. Nothing filtered that sentinel back out, so every "for r in
# read_X(...)" loop downstream treated it as one real record -- producing a
# phantom row per empty source/entity/date (e.g. game_id/team_id computed
# from an f-string containing None, and null source_id entries polluting
# bridge_source_ids).
def test_drop_empty_sentinel_filters_placeholder_row():
    rows = [{"empty": True, "dt": "2025-01-01"}]
    assert _drop_empty_sentinel(rows) == []


def test_drop_empty_sentinel_keeps_real_rows():
    rows = [{"game_id": "abc123", "dt": "2025-01-01"}]
    assert _drop_empty_sentinel(rows) == rows


def test_gold_read_silver_filters_empty_sentinel(tmp_path, monkeypatch):
    monkeypatch.setenv("GBDP_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("GBDP_SILVER_ROOT", str(tmp_path / "silver"))
    from gbdp.gold.publish import _read_silver

    part = tmp_path / "silver" / "mlb_statcast" / "pitches" / "dt=2025-01-01"
    part.mkdir(parents=True)
    pq.write_table(
        __import__("pyarrow").Table.from_pylist([{"empty": True, "dt": "2025-01-01"}]),
        part / "part-00001.parquet",
    )

    rows = _read_silver(tmp_path / "silver", "mlb_statcast", "pitches", date(2025, 1, 1))
    assert rows == []


def test_identity_read_silver_filters_empty_sentinel(tmp_path):
    from gbdp.identity.resolver import _read_silver as identity_read_silver

    part = tmp_path / "mlb_statsapi" / "rosters" / "dt=2025-01-01"
    part.mkdir(parents=True)
    pq.write_table(
        __import__("pyarrow").Table.from_pylist([{"empty": True, "dt": "2025-01-01"}]),
        part / "part-00001.parquet",
    )

    rows = identity_read_silver(tmp_path, "mlb_statsapi", "rosters", date(2025, 1, 1))
    assert rows == []


# --- Bug 5: the MLB Statcast connector never requested type=details from ---
# Baseball Savant's CSV search endpoint. Without it, the endpoint silently
# returns a small aggregate leaderboard report instead of pitch-by-pitch
# data -- no game_pk, no per-pitch columns at all -- and every pitch fetched
# this way collapsed onto a single bogus game_id downstream.
def test_statcast_connector_requests_pitch_level_detail():
    from gbdp.connectors.mlb_statcast import MlbStatcastConnector
    from gbdp.connectors.base import Partition

    captured = {}

    class FakeConnector(MlbStatcastConnector):
        def http_get(self, url, params=None):
            captured["params"] = params
            return _payload(url=url, params=params or {})

    connector = FakeConnector(writer=None, cache=None, base_url="https://baseballsavant.mlb.com/statcast_search/csv")
    connector.fetch_partition(Partition(dt="2025-04-01", entity="pitches", keys={}))

    assert captured["params"].get("type") == "details", (
        "without type=details, Baseball Savant silently returns a leaderboard "
        "report instead of pitch-by-pitch data"
    )


# --- Contract check: this is the guard that would have caught the type= ---
# details bug directly at ingestion, independent of the connector requesting
# the right params -- if a response ever comes back shaped like the wrong
# report again (for any reason), write_bronze must refuse to write it.
def test_write_bronze_rejects_wrong_shaped_statcast_response():
    from gbdp.connectors.mlb_statcast import MlbStatcastConnector

    class NoopWriter:
        def write_raw(self, payload, force=False):
            pass

        def write_parsed(self, payload, records, force=False):
            pass

    connector = MlbStatcastConnector(writer=NoopWriter(), cache=None, base_url="https://example.invalid")
    payload = _payload(source="mlb_statcast", entity="pitches")

    wrong_report_records = [
        {"player_id": "123", "player_name": "X", "pitch_percent": "10.5", "xwoba": "0.300"}
    ]
    with pytest.raises(RuntimeError, match="contract violation"):
        connector.write_bronze(payload, wrong_report_records)

    correct_records = [
        {"game_pk": "778492", "pitch_type": "FF", "batter": "123", "pitcher": "456"}
    ]
    connector.write_bronze(payload, correct_records)  # must not raise


# --- Bug 7 (found by the contract check above, on real live data): Baseball -
# Savant's CSV export carries a leading BOM. Left in place, csv.DictReader
# folds it into the FIRST column's key only, silently making just that one
# column (pitch_type) unreadable by its real name forever, while every other
# column parsed fine. Confirmed against a real fetch even after the
# type=details fix was live.
def test_statcast_parse_strips_bom_from_first_column_name():
    from gbdp.connectors.mlb_statcast import MlbStatcastConnector

    body_text = '﻿"pitch_type","game_date"\n"FF","2025-04-01"\n'
    payload = _payload(source="mlb_statcast", entity="pitches", body_text=body_text)
    connector = object.__new__(MlbStatcastConnector)

    records = connector.parse_payload(payload)

    assert records[0].keys() == {"pitch_type", "game_date"}, (
        f"BOM leaked into a column key: {list(records[0].keys())}"
    )
    assert records[0]["pitch_type"] == "FF"


# --- Bug 6: the Delta schema-mismatch exception fallback did a bare --------
# mode("overwrite") with no replaceWhere, which replaces the ENTIRE table
# with just the current partition's rows. Confirmed this had already wiped
# most history out of several real UC tables (e.g. mlb_statcast_pitches,
# npb_spaia_game_pbp) down to a single date. This is a static source check,
# not an executed one, since there's no local Spark/Delta to run against --
# it exists to make sure nobody reintroduces the dangerous pattern.
@pytest.mark.parametrize(
    "path",
    [
        "src/gbdp/catalog/managed_publish.py",
        "src/gbdp/silver/writer.py",
    ],
)
def test_no_unscoped_full_table_overwrite_in_schema_mismatch_fallback(path):
    text = Path(path).read_text(encoding="utf-8")
    assert "_add_missing_columns" in text, (
        f"{path}: expected the additive ALTER TABLE ADD COLUMNS + retry fix to be present"
    )

    # Scope the check to the schema-mismatch exception handler specifically
    # (the LAST except-block in the file, which is what a schema conflict on
    # a partition-scoped write falls into) -- the FIRST-write bootstrap path
    # legitimately uses a bare mode("overwrite") since there's nothing to
    # lose on a brand-new table, so we deliberately don't flag that one.
    last_except_idx = text.rindex("except Exception")
    handler_tail = text[last_except_idx:]

    assert "_add_missing_columns" in handler_tail, (
        f"{path}: schema-mismatch handler no longer widens the schema additively"
    )
    assert "overwriteSchema" not in handler_tail, (
        f"{path}: schema-mismatch handler reintroduced overwriteSchema -- this is the exact "
        f"pattern that silently wiped most history out of several production tables"
    )
