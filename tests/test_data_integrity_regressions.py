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


# --- Bug 2b (found on the first real nightly run, on live data): a Python --
# int outside signed-int64 range crashes pyarrow table construction for the
# ENTIRE partition with "Python int too large to convert to C long".
# Confirmed live: NPB's player_batting_detail_saber endpoint returns
# 18446744073709552000 (~2^64, an unsigned-underflow sentinel) for some
# players' RunsCreatedForOrder/PApKForOrder fields.
def test_bronze_to_table_nulls_out_of_range_ints_instead_of_crashing():
    writer = BronzeWriter(root=Path("unused"))
    payload = _payload()
    records = [
        {"name": "Test Player", "runs_created_for_order": 18446744073709552000, "batting_average": 0.300}
    ]
    table = writer._to_table(payload, records)
    row = table.to_pylist()[0]
    assert row["runs_created_for_order"] is None
    assert row["batting_average"] == 0.300


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


# --- Bug 8: NPB SPAIA's /directory endpoint returns one entry PER PLAYER ---
# (confirmed live: 1459 entries), not one per team. _get_team_ids() never
# deduplicated, so every per-team fetch (rosters, batter_list, pitcher_list,
# staff_list) looped ~1459 times instead of ~12 real teams -- almost
# certainly the actual cause of NPB fetches taking over an hour earlier this
# session, and it also produced a roster table bloated ~110x (161,173 rows
# for what should be ~1,449).
def test_npb_get_team_ids_deduplicates():
    from gbdp.connectors.npb_spaia import NpbSpaiaConnector
    import json

    # Simulates /directory: one entry per player, TeamID repeats heavily.
    directory = [{"TeamID": str(team_id)} for team_id in [1, 2, 1, 1, 2, 3, 2, 1]]

    class FakeConnector(NpbSpaiaConnector):
        def http_get(self, url, params=None):
            return _payload(body_text=json.dumps(directory))

    connector = FakeConnector(writer=None, cache=None, base_url="https://spaia.jp/baseball/npb/api")
    ids = connector._get_team_ids()

    assert ids == [1, 2, 3], f"expected 3 deduplicated teams in first-seen order, got {ids}"


# --- Bug 9: silver/npb.py's roster normalizer (and npb_spaia's own player- -
# id lookup) checked p.get("PersonInfoId") but the real API field is
# "PersonInfoID" (capital D). Confirmed live: every roster row's player_id
# came back None despite PersonInfoID being present and correct in the same
# record's raw_json.
def test_npb_roster_extracts_player_id_from_person_info_id():
    from gbdp.silver.npb import _normalize_rosters

    root = None  # unused by this call path once we monkeypatch _read_bronze
    import gbdp.silver.npb as npb_module

    fake_bronze_row = {
        "teams": [
            {
                "team_id": "5",
                "body_text": (
                    '[{"PersonInfoID": "1750802", "PlayerName": "Test Player", "TeamID": "5"}]'
                ),
            }
        ]
    }

    original = npb_module._read_bronze
    npb_module._read_bronze = lambda entity, dt, root: [fake_bronze_row]
    try:
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            out_path = npb_module._normalize_rosters(Path(tmp), date(2023, 10, 18), force=True)
            rows = pq.ParquetFile(str(out_path)).read().to_pylist()
    finally:
        npb_module._read_bronze = original

    assert len(rows) == 1
    assert rows[0]["player_id"] == "1750802", f"player_id not extracted: {rows[0]}"


# --- Bug 10: gold_publish writes ONE Delta table per fact table, one dt at --
# a time, via replaceWhere + mergeSchema. _infer_type() decided a field's
# type purely from that single write's batch of rows, so a date where every
# contributing source happened to leave a field null (e.g. kbo_api boxscore
# rows never populate 2b/3b/hr/bb/so) inferred it as STRING (the all-null
# fallback), while a date with real data inferred it as LONG -- and Delta
# legitimately refuses to merge those types. Confirmed live during a real
# backfill: register_uc failed with "[DELTA_FAILED_TO_MERGE_FIELDS] Failed
# to merge fields 'c_2b' and 'c_2b'" on fact_boxscore_batting.
def test_infer_type_pins_known_numeric_fields_even_when_all_null():
    from gbdp.gold.publish import _infer_type

    assert _infer_type([None, None, None], field_name="2b") == "long"
    assert _infer_type([None, None], field_name="release_speed") == "double"
    # A field with real data must still infer the SAME type an all-null
    # batch for that field now gets, or the fix doesn't actually prevent
    # cross-date drift.
    assert _infer_type([1, 2, None], field_name="2b") == "long"
    # Unrecognized fields keep the original (safe) all-null-to-string
    # fallback -- this fix is a targeted pin, not a blanket type guess.
    assert _infer_type([None, None], field_name="some_unrelated_field") == "string"


def test_infer_type_pin_wins_over_batch_values():
    from gbdp.gold.publish import _infer_type

    assert _infer_type(["7"], field_name="inning") == "long"
    assert _infer_type([18, 21], field_name="ip") == "string"
    assert _infer_type([0.5], field_name="pct") == "string"


@pytest.mark.parametrize(
    "value,type_name,expected",
    [("7", "long", 7), (7.0, "long", 7), ("-", "long", None), ("", "double", None), (3, "double", 3.0), (0.5, "string", "0.5"), (None, "long", None)],
)
def test_coerce_known_type(value, type_name, expected):
    from gbdp.gold.publish import _coerce_known_type

    assert _coerce_known_type(value, type_name) == expected


# --- Bug 14: gold's _write_delta retried a merge failure as an unscoped ------
# overwrite, replacing the whole table with one date. Delta history shows this
# erased every MLB row from fact_plate_appearance (versions 136, 142-144). Same
# pattern as Bug 6; this is a static check for the same reason.
def test_gold_write_delta_only_overwrites_schema_on_first_write():
    text = Path("src/gbdp/gold/publish.py").read_text(encoding="utf-8")
    body = text[text.index("def _write_delta("):text.index("# Public API")]
    assert body.count("overwriteSchema") == 1
    assert body.index("overwriteSchema") < body.index('option("mergeSchema"')


# --- Bug 15: sorting NPB plays mixed int play_seq_no with string event_id, ---
# crashing gold_publish ("'<' not supported between 'str' and 'int'", 2026-09-10).
def test_npb_pa_reconstruction_handles_mixed_sequence_types():
    from gbdp.gold.publish import _reconstruct_npb_pa

    rows = [
        {"game_id": "1", "inning": 1, "top_bottom": "1", "play_seq_no": 2, "event_id": "20"},
        {"game_id": "1", "inning": 1, "top_bottom": "1", "play_seq_no": None, "event_id": "1"},
    ]
    assert [r["inning"] for r in _reconstruct_npb_pa(rows, date(2025, 4, 1), {})] == [1, 1]


# --- Bug 17: bronze from before the type=details fix still holds leaderboard -
# rows. With no game_pk they all hashed to one pitch_id, failing the uniqueness
# check (118 rows on 2004-05-18 during the rebuild).
def test_silver_pitches_drop_rows_without_game_pk(tmp_path, monkeypatch):
    import gbdp.silver.mlb as mlb_module

    rows = [{"player_id": "123", "pitch_percent": "10.5"}, {"game_pk": "778492", "batter": "1", "pitcher": "2", "pitch_number": "1"}]
    monkeypatch.setattr(mlb_module, "_read_bronze", lambda source, entity, dt, root: rows)
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")

    out_path = mlb_module._normalize_pitches(tmp_path, date(2025, 4, 1), force=True)

    written = pq.ParquetFile(str(out_path)).read().to_pylist()
    assert [r["game_pk"] for r in written] == ["778492"]


# --- Bug 18: dim_player bio columns were always null or UNK, because the -----
# roster endpoint returns only a name and ID.
@pytest.mark.parametrize("height,expected", [("6' 3\"", 190.5), ("5' 11\"", 180.3), ("", None), (None, None)])
def test_height_to_cm(height, expected):
    from gbdp.silver.mlb import _height_to_cm

    assert _height_to_cm(height) == expected


def test_player_bios_reach_dim_player(gold_env, monkeypatch):
    import json

    import gbdp.silver.mlb as mlb_module
    from gbdp.gold.publish import _write_dim_player

    silver, gold = gold_env
    person = {"id": 660271, "birthDate": "1994-07-05", "height": "6' 3\"", "weight": 210,
              "batSide": {"code": "L"}, "pitchHand": {"code": "R"}, "birthCountry": "Japan"}
    monkeypatch.setattr(mlb_module, "_read_bronze", lambda source, entity, dt, root: [{"people": json.dumps([person])}])
    day = date(2025, 4, 1)
    mlb_module._normalize_player_bios(silver, day, force=True)
    _write_silver(silver, "mlb_statsapi", "rosters", day, [{"player_id": "660271", "player_name": "X", "team_id": "119"}])

    _write_dim_player(gold, day, {}, True)

    row = read_parquet_rows(gold / "dim_player" / f"dt={day}")[0]
    assert (row["dob"], row["bats"], row["throws"], row["nationality"]) == ("1994-07-05", "L", "R", "Japan")
    assert (row["height_cm"], row["weight_kg"]) == (190.5, 95.3)


def test_npb_kana_fills_alternate_names():
    from gbdp.gold.publish import _player_row

    row = _player_row({"player_id": "1", "raw_json": '{"PlayerNameK": "イケヤマ"}'}, "NPB", {}, "npb_spaia")
    assert row["alternate_names"] == ["イケヤマ"]
    assert _player_row({"player_id": "1"}, "MLB", {}, "mlb_statsapi")["alternate_names"] is None


def _pitch(at_bat, number, inning, half, outs, events="", on_1b=None, score=(0, 0)):
    import json

    return {
        "game_pk": "1", "at_bat_number": at_bat, "pitch_number": number, "inning": inning, "inning_topbot": half,
        "outs_when_up": outs, "events": events, "on_1b": on_1b, "batter_id": "b", "pitcher_id": "p",
        "raw_json": json.dumps({"bat_score": str(score[0]), "post_bat_score": str(score[1])}),
    }


# --- Bug 19: Statcast PAs reused outs_when_up for outs_on_play and outs_after -
# and left runs and base_state_after null.
def test_statcast_pa_state_comes_from_the_next_pa():
    from gbdp.gold.publish import _statcast_plate_appearances

    rows = _statcast_plate_appearances(
        [
            _pitch(1, 1, 1, "Top", 0),
            _pitch(1, 2, 1, "Top", 0, "single"),
            _pitch(2, 1, 1, "Top", 0, on_1b="x"),
            _pitch(2, 2, 1, "Top", 0, "home_run", on_1b="x", score=(0, 2)),
            _pitch(3, 1, 1, "Top", 2, "field_out"),
            _pitch(4, 1, 1, "Bot", 0, "walk"),
        ],
        date(2025, 4, 1),
        {},
    )
    single, homer, third_out, walk = rows

    assert (single["event_type"], single["base_state_before"], single["base_state_after"]) == ("1B", 0, 1)
    assert (single["outs_on_play"], single["runs_scored_on_play"]) == (0, 0)
    assert (homer["runs_scored_on_play"], homer["outs_after"], homer["outs_on_play"]) == (2, 2, 2)
    assert (third_out["outs_after"], third_out["outs_on_play"], third_out["base_state_after"]) == (3, 1, 0)
    # Game-ending PA that isn't an out: no third out, and the next state is unknown.
    assert (walk["outs_after"], walk["base_state_after"]) == (0, None)


def test_fact_pitch_rows_reference_their_pa_even_with_a_pinch_hitter():
    from gbdp.gold.publish import _statcast_pa_id, _statcast_plate_appearances

    first, second = _pitch(1, 1, 1, "Top", 0), _pitch(1, 2, 1, "Top", 0, "walk")
    second["batter_id"] = "pinch_hitter"
    rows = _statcast_plate_appearances([first, second], date(2025, 4, 1), {})
    assert [r["pa_id"] for r in rows] == [_statcast_pa_id("1", 1)]


# --- Bug 20: bronze files are named by checksum, so a refetch with new content
# adds a file beside the old one, and silver read both: every record twice.
def test_read_bronze_rows_uses_only_the_latest_fetch(tmp_path):
    import pyarrow as pa

    from gbdp.utils.io import read_bronze_rows

    part = tmp_path / "dt=2025-04-01"
    part.mkdir()
    old = [{"game": "1", "status": "In Progress", "response_checksum": "a", "ingested_at_utc": "2025-04-02T04:00:00"}]
    new = [{"game": "1", "status": "Final", "response_checksum": "b", "ingested_at_utc": "2025-04-03T04:00:00"}]
    empty = [{"empty": True, "response_checksum": "c", "ingested_at_utc": "2025-04-04T04:00:00"}]
    for name, rows in [("a", old), ("b", new), ("c", empty)]:
        pq.write_table(pa.Table.from_pylist(rows), part / f"{name}.parquet")

    assert [r["status"] for r in read_bronze_rows(part)] == ["Final"]


# --- Bug 21: row_count_min was a hard check on every date, so the nightly job
# would fail all winter on dates with no games.
def test_row_count_min_only_applies_in_season():
    from gbdp.quality.checks import _in_season

    assert _in_season(date(2026, 7, 1))
    assert not _in_season(date(2026, 12, 1))


# --- Bug 22: the first attempt wrote audit_quality and then raised, so a stage
# retry found the file, returned early, and passed the hard failure.
def test_quality_retry_cannot_pass_a_hard_failure(gold_env, monkeypatch):
    import pyarrow as pa
    from gbdp.quality import checks

    _, gold = gold_env
    day = date(2025, 4, 1)
    part = gold / "fact_game" / f"dt={day}"
    part.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{"game_id": "dup"}, {"game_id": "dup"}]), part / "part-00001.parquet")
    cfg = {"uniqueness": [{"table": "gbdp_gold.fact_game", "columns": ["game_id"]}]}

    for _attempt in range(2):
        with pytest.raises(RuntimeError, match="hard invariant"):
            checks._run_for_date(gold, day, cfg, force=False)


# --- Appendix A: the nightly job has to cover a 7-day correction window, and --
# needs --force, or refetched dates can't replace what was stored the first time.
def test_nightly_schedule_state_comes_from_a_bundle_variable():
    # A hardcoded UNPAUSED re-enabled the nightly on every push, undoing a
    # pause made through the API mid-backfill.
    import yaml

    config = yaml.safe_load(Path("databricks.yml").read_text(encoding="utf-8"))
    schedule = config["resources"]["jobs"]["gbdp_nightly"]["schedule"]
    assert schedule["pause_status"] == "${var.nightly_pause_status}"
    assert config["variables"]["nightly_pause_status"]["default"] in ("PAUSED", "UNPAUSED")


def test_nightly_job_runs_the_correction_window_with_force():
    import yaml

    config = yaml.safe_load(Path("databricks.yml").read_text(encoding="utf-8"))
    params = config["resources"]["jobs"]["gbdp_nightly"]["tasks"][0]["spark_python_task"]["parameters"]
    assert params[params.index("--window") + 1] == "nightly"
    assert "--force" in params


def test_nightly_fetches_npb_season_stats_for_the_latest_date_only(monkeypatch):
    import gbdp.pipeline.runner as runner

    seen = []

    class FakeNpb:
        def __init__(self, *a):
            pass

        def list_partitions(self, start, end, entity):
            seen.append((start, end))
            return []

    monkeypatch.setattr(runner, "NpbSpaiaConnector", FakeNpb)
    monkeypatch.setenv("GBDP_NPB_STATS_LATEST_ONLY", "true")
    runner._stage_fetch_npb_stats("2025-04-01", "2025-04-07", {"npb_spaia": {"base_url": "x"}}, None, None, False)
    assert set(seen) == {("2025-04-07", "2025-04-07")}


# --- Bug 24: a UC table first created from an all-STRING placeholder write
# (silver npb_spaia_games) rejected every later partition with real bigint
# scores: DELTA_FAILED_TO_MERGE_FIELDS on away_score, so register_uc failed
# for all of May-June.
def test_type_clash_is_cast_to_the_table_type_not_raised():
    text = Path("src/gbdp/catalog/managed_publish.py").read_text(encoding="utf-8")
    handler_tail = text[text.rindex("except Exception"):]
    assert "delta_failed_to_merge_fields" in handler_tail
    assert "_align_to_table_types(spark, full_name, df)" in handler_tail
    assert "_write_partition(df)" in handler_tail


def test_a_schema_clash_fails_one_table_not_the_whole_publish(monkeypatch):
    from gbdp.catalog import managed_publish as mp

    def fake_write(spark, df, full_name, dt):
        if full_name == "bad":
            raise RuntimeError("[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'W_PIT_P_ID'")
        written.append(full_name)

    written, errors = [], []
    monkeypatch.setattr(mp, "_write_managed", fake_write)
    for name in ("bad", "good"):
        mp._try_write(None, None, name, date(2026, 5, 20), errors)

    assert written == ["good"]
    assert len(errors) == 1 and errors[0].startswith("bad dt=2026-05-20")


def test_publish_raises_a_permanent_error_after_all_tables(monkeypatch):
    from gbdp.catalog import managed_publish as mp
    from gbdp.pipeline.errors import PermanentStageError

    published = []

    def gold(spark, dt, catalog, schema, errors):
        errors.append(f"bad dt={dt}")
        published.append("gold")

    monkeypatch.setattr(mp, "_spark_session", lambda: object())
    monkeypatch.setattr(mp, "_ensure_schema", lambda *a: None)
    monkeypatch.setattr(mp, "_publish_gold", gold)
    for name in ("_publish_silver", "_publish_bronze_parsed", "_publish_bronze_requests"):
        monkeypatch.setattr(mp, name, lambda *a, n=name: published.append(n))

    with pytest.raises(PermanentStageError, match="1 UC table write"):
        mp.publish_uc_managed("2026-05-20", "2026-05-20", "c", "b", "s", "g")
    assert published == ["gold", "_publish_silver", "_publish_bronze_parsed", "_publish_bronze_requests"]


def test_other_publish_errors_still_raise(monkeypatch):
    from gbdp.catalog import managed_publish as mp

    def boom(*a):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(mp, "_write_managed", boom)
    with pytest.raises(RuntimeError, match="connection reset"):
        mp._try_write(None, None, "t", date(2026, 5, 20), [])


def test_permanent_stage_errors_are_not_retried(monkeypatch, tmp_path):
    from gbdp.pipeline import runner
    from gbdp.pipeline.errors import PermanentStageError

    calls = []

    def stage(*a):
        calls.append(1)
        raise PermanentStageError("schema clash")

    monkeypatch.setenv("GBDP_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(runner, "_pipeline_stages", lambda: {"register_uc": stage})
    monkeypatch.setattr(runner, "_write_stage_audit", lambda *a: None)
    with pytest.raises(Exception):
        runner.run_pipeline("2026-05-20", "2026-05-20", "configs/sources.yaml", True,
                            stages=["register_uc"], retries=2, retry_delay_s=0)
    assert len(calls) == 1


def test_only_lossless_casts_are_applied():
    types = pytest.importorskip("pyspark.sql.types")
    from gbdp.catalog.managed_publish import _lossless_cast

    assert _lossless_cast(types.StringType(), types.LongType())
    assert _lossless_cast(types.LongType(), types.IntegerType())
    assert _lossless_cast(types.DoubleType(), types.NullType())
    assert not _lossless_cast(types.LongType(), types.StringType())
    assert not _lossless_cast(types.LongType(), types.DoubleType())


def test_npb_silver_rosters_are_one_row_per_team_and_player(monkeypatch, tmp_path):
    import json

    from gbdp.silver import npb

    team = {"team_id": "1", "body_text": json.dumps({"players": [{"player_id": "9"}, {"player_id": "8"}]})}
    # The same roster payload repeated across bronze rows, as in May-June.
    monkeypatch.setattr(npb, "_read_bronze", lambda entity, dt, root: [{"teams": json.dumps([team])}] * 3)

    out = npb._normalize_rosters(tmp_path, date(2026, 5, 13), True)

    rows = read_parquet_rows(Path(out).parent)
    assert sorted(r["player_id"] for r in rows) == ["8", "9"]


# --- Statcast and NPB plate appearances carried no batting/fielding team.
def test_plate_appearances_get_teams_from_the_game(gold_env):
    from gbdp.gold.publish import _write_fact_game, _write_fact_plate_appearance

    silver, gold = gold_env
    day = date(2025, 4, 1)
    _write_silver(silver, "mlb_statsapi", "games", day, [{"game_id": "777", "home_team_id": "1", "away_team_id": "2"}])
    pitch = {"game_pk": 777, "pitch_number": 1, "outs_when_up": 0, "events": "single", "batter_id": 5, "pitcher_id": 6, "raw_json": "{}"}
    _write_silver(silver, "mlb_statcast", "pitches", day, [
        {**pitch, "at_bat_number": 1, "inning": 1, "inning_topbot": "Top"},
        {**pitch, "at_bat_number": 2, "inning": 1, "inning_topbot": "Bot"},
    ])

    _write_fact_game(gold, day, {}, True)
    _write_fact_plate_appearance(gold, day, {}, True)

    game, = read_parquet_rows(gold / "fact_game" / f"dt={day}")
    pas = sorted(read_parquet_rows(gold / "fact_plate_appearance" / f"dt={day}"), key=lambda r: not r["is_top_inning"])
    assert (pas[0]["batting_team_id"], pas[0]["fielding_team_id"]) == (game["away_team_id"], game["home_team_id"])
    assert (pas[1]["batting_team_id"], pas[1]["fielding_team_id"]) == (game["home_team_id"], game["away_team_id"])
    assert game["home_team_id"] is not None


# --- Bug 23: May-June NPB bronze repeats each roster ~110 times (157,000 rows
# for ~1,430 players), which bloated fact_roster to 4.7M rows.
def test_fact_roster_is_one_row_per_team_and_player(gold_env):
    from gbdp.gold.publish import _write_fact_roster

    silver, gold = gold_env
    day = date(2025, 4, 1)
    _write_silver(silver, "npb_spaia", "rosters", day, [{"team_id": "1", "player_id": "9"}] * 5 + [{"team_id": "1", "player_id": "8"}])

    _write_fact_roster(gold, day, {}, True)

    assert len(read_parquet_rows(gold / "fact_roster" / f"dt={day}")) == 2


def test_league_lookup_does_not_reread_the_config_per_row(monkeypatch):
    from gbdp.gold import publish

    publish._league_id_for_code.cache_clear()
    first = publish._league_id_for_code("NPB")
    monkeypatch.setattr(publish, "path_exists", lambda path: pytest.fail("re-read leagues.yaml"))
    assert publish._league_id_for_code("NPB") == first


# --- Bug 16: the backfill task never passed its job parameters to the CLI, so -
# every run silently used the default last-7-days window and all stages, with
# --force. Went unnoticed because one real request happened to equal the default.
def test_backfill_job_passes_its_parameters_to_the_cli():
    import yaml

    config = yaml.safe_load(Path("databricks.yml").read_text(encoding="utf-8"))
    job = config["resources"]["jobs"]["gbdp_backfill"]
    params = job["tasks"][0]["spark_python_task"]["parameters"]
    flags = {"start_date": "--start", "end_date": "--end", "leagues": "--leagues", "stages": "--stages"}

    assert {p["name"] for p in job["parameters"]} == set(flags)
    for name, flag in flags.items():
        assert params[params.index(flag) + 1] == f"{{{{job.parameters.{name}}}}}"


def test_retrosheet_ip_uses_innings_notation_not_raw_outs():
    from gbdp.silver.retrosheet import _outs_to_ip

    assert _outs_to_ip("19") == "6.1"
    assert _outs_to_ip("18") == "6.0"
    assert _outs_to_ip("") is None


def _write_silver(root, source, entity, dt, rows):
    import pyarrow as pa

    part = root / source / entity / f"dt={dt}"
    part.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), part / "part-00001.parquet")


@pytest.fixture
def gold_env(tmp_path, monkeypatch):
    silver, gold = tmp_path / "silver", tmp_path / "gold"
    gold.mkdir()
    monkeypatch.setenv("GBDP_SILVER_ROOT", str(silver))
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(gold))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    return silver, gold


# --- Bug 11: ulid_from_key hashed dt into every ID, so canonical IDs changed -
# every day. Confirmed in production: 0 of 2,369 entities present on both
# 2026-09-11 and 09-12 kept their ID, leaving the "rolling 30d" feature at one
# game per row and growing dim_player by a full roster per day.
def test_identity_bridge_ids_are_stable_across_dates(gold_env):
    from gbdp.identity.resolver import resolve_identity

    silver, gold = gold_env
    days = ["2025-04-01", "2025-04-02"]
    for day in days:
        _write_silver(silver, "mlb_statsapi", "rosters", day, [{"player_id": "660271", "player_name": "X", "team_id": "119"}])
        _write_silver(
            silver, "mlb_statsapi", "games", day,
            [{"home_team_id": "119", "home_team_name": "LAD", "away_team_id": "147", "away_team_name": "NYY"}],
        )
    resolve_identity(days[0], days[1], force=True)

    bridges = [
        {(r["entity_type"], r["source_id"]): r["canonical_id"] for r in read_parquet_rows(gold / "bridge_source_ids" / f"dt={day}")}
        for day in days
    ]
    assert len(bridges[0]) == 3
    assert bridges[0] == bridges[1]


# The bridge and gold's no-bridge fallback derive IDs separately. If their keys
# differ, one entity gets two IDs depending on whether the bridge covered it.
def test_gold_fallback_ids_match_identity_bridge_ids():
    from gbdp.gold.publish import _fact_roster_row
    from gbdp.identity.rules import player_match_key, team_match_key
    from gbdp.utils.ids import ulid_from_key

    row = _fact_roster_row({"team_id": "119", "player_id": "660271"}, "mlb_statsapi", "MLB", {}, date(2025, 4, 1))

    player = player_match_key({"source": "mlb_statsapi", "source_player_id": "660271"})
    team = team_match_key({"source": "mlb_statsapi", "source_team_id": "119"})
    assert row["player_id"] == ulid_from_key(player.canonical_key)
    assert row["team_id"] == ulid_from_key(team.canonical_key)


# --- Bug 12: box score writers keyed games as game:retrosheet / game:indy / --
# game:kbo / game:lmb while fact_game used the full source name, so no box
# score from those sources could ever join to its game.
@pytest.mark.parametrize("source", ["mlb_statsapi", "kbo_api", "indy_local", "kbo_local", "lmb_local", "retrosheet_local"])
def test_boxscore_game_ids_join_to_fact_game(gold_env, source):
    from gbdp.gold.publish import _fact_game_from_silver, _write_fact_boxscore_batting, _write_fact_boxscore_pitching

    silver, gold = gold_env
    day = date(2025, 4, 1)
    _write_silver(silver, source, "games", day, [{"game_id": "G1", "home_team_id": "A", "away_team_id": "B"}])
    line = {"game_id": "G1", "player_id": "P1", "player_name": "P1", "team_id": "A", "ab": 4, "h": 1}
    _write_silver(silver, source, "boxscore_batting", day, [line])
    _write_silver(silver, source, "boxscore_pitching", day, [{**line, "ip": "6.0"}])

    game_id = _fact_game_from_silver(gold, day, source, "MLB", {})[0]["game_id"]

    for write, table in [(_write_fact_boxscore_batting, "fact_boxscore_batting"), (_write_fact_boxscore_pitching, "fact_boxscore_pitching")]:
        write(gold, day, {}, True)
        assert read_parquet_rows(gold / table / f"dt={day}")[0]["game_id"] == game_id


# --- Bug 13: pitch_id hashed inning/outs/pitcher/batter/description but not --
# the pitch's position, so two identical pitches in one at-bat (say, two fouls
# with two strikes) shared an ID. Confirmed on ~28% of production fact_pitch
# rows.
def test_identical_pitches_in_one_at_bat_get_distinct_ids(gold_env):
    from gbdp.gold.publish import _write_fact_pitch

    silver, gold = gold_env
    day = date(2025, 4, 1)
    pitch = {"game_pk": "778492", "at_bat_number": 7, "inning": 3, "outs_when_up": 1, "pitcher_id": "1", "batter_id": "2", "description": "foul"}
    _write_silver(silver, "mlb_statcast", "pitches", day, [{**pitch, "pitch_number": 4}, {**pitch, "pitch_number": 5}])

    _write_fact_pitch(gold, day, {}, True)

    rows = read_parquet_rows(gold / "fact_pitch" / f"dt={day}")
    assert len({r["pitch_id"] for r in rows}) == 2
