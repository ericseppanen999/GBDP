"""configs/semantic_layer.yaml against what the code really writes.

The fixture silver (tests/semantic_fixture.py) is run through the real
identity and gold code; nothing here lists tables or columns by hand. The
table list comes from publish.py and resolver.py's own source, and the
columns from the parquet they wrote.
"""
from __future__ import annotations

import inspect
import re
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from gbdp.gold import publish
from gbdp.identity import resolver
from gbdp.semantic import layer as semantic
from gbdp.semantic.render import databricks_target, duckdb_target, render


@pytest.fixture(scope="module")
def sl():
    return semantic.load()


def _publish_tables() -> set:
    src = inspect.getsource(publish)
    return set(re.findall(r'_write_gold_table\(\s*root,\s*"(\w+)"', src))


def _resolver_tables() -> set:
    return set(re.findall(r'root / "(\w+)" / f"dt=', inspect.getsource(resolver)))


def _written_columns(gold: Path, table: str) -> set:
    cols: set = set()
    for f in (gold / table).rglob("*.parquet"):
        cols |= set(pq.read_schema(f).names)
    return cols


def test_the_code_writes_the_tables_this_test_expects():
    # Guards the regexes below: if the writers change shape, fail loudly
    # rather than checking an empty list.
    assert {"fact_plate_appearance", "run_expectancy_daily", "fact_contract"} <= _publish_tables()
    assert {"bridge_source_ids", "merge_events"} <= _resolver_tables()


def test_every_table_the_code_writes_is_described(sl):
    described = set(sl.tables) | set(sl.audit_tables)
    missing = (_publish_tables() | _resolver_tables()) - described
    assert not missing, f"gold tables with no semantic-layer entry: {sorted(missing)}"


def test_every_described_table_is_written_by_the_code(sl):
    stray = set(sl.tables) - _publish_tables() - _resolver_tables()
    assert not stray, f"semantic_layer.yaml describes tables nothing writes: {sorted(stray)}"


def test_publish_runs_every_table_writer(sl):
    steps = re.findall(r'\("(\w+)", lambda', inspect.getsource(publish._publish_for_date))
    # run_expectancy_daily is written inside the run_expectancy step.
    assert set(steps) | {"run_expectancy_daily"} == _publish_tables()


def test_columns_match_what_the_code_writes(sl, semantic_gold):
    problems = []
    for name, t in sl.tables.items():
        written = _written_columns(semantic_gold, name)
        if t.placeholder:
            if written:
                problems.append(f"{name}: marked placeholder but written with {sorted(written)}")
            continue
        if not written:
            problems.append(f"{name}: the fixture produced no rows; extend tests/semantic_fixture.py")
            continue
        undocumented = written - set(t.columns)
        absent = set(t.columns) - written
        if undocumented:
            problems.append(f"{name}: written but not described: {sorted(undocumented)}")
        if absent:
            problems.append(f"{name}: described but never written: {sorted(absent)}")
    assert not problems, "\n".join(problems)


def test_merge_events_is_written_as_the_audit_entry_says(semantic_gold, sl):
    # No manual overrides in the fixture: the writer leaves its placeholder row.
    assert _written_columns(semantic_gold, "merge_events") == {"empty"}
    assert "placeholder" in sl.audit_tables["merge_events"]


def test_primary_keys_are_unique_in_the_fixture(sl, semantic_gold):
    import duckdb

    con = duckdb.connect()
    for name, t in sl.tables.items():
        if t.placeholder or not t.primary_key:
            continue
        glob = str(semantic_gold / name / "**" / "*.parquet")
        keys = ", ".join(f'"{c}"' for c in t.primary_key)
        dupes = con.execute(
            f"SELECT count(*) FROM (SELECT {keys} FROM read_parquet('{glob}', union_by_name=true, "
            f"hive_types_autocast=false) GROUP BY {keys} HAVING count(*) > 1)"
        ).fetchone()[0]
        assert dupes == 0, f"{name}: primary key {t.primary_key} is not unique"


def _returned_codes(fn) -> set:
    return set(re.findall(r'return "([A-Z0-9]+)"', inspect.getsource(fn)))


def test_event_type_vocabulary_is_exactly_what_the_code_emits(sl):
    with Path("configs/npb_text.yaml").open(encoding="utf-8") as f:
        npb_events = {e["event"] for e in yaml.safe_load(f)["events"]}
    emitted = _returned_codes(publish._map_event_type) | _returned_codes(publish._map_retrosheet_event)
    emitted |= _returned_codes(publish._npb_event_type) | npb_events
    emitted |= set(re.findall(r'"event_type": "(\w+)"', inspect.getsource(publish._npb_pa_rows)))
    vocab = sl.vocabularies["event_type"]
    assert set(map(str, vocab.values)) == emitted
    # Legacy codes are only in older gold partitions: the code never writes them.
    assert not set(vocab.legacy) & emitted


def test_event_type_groups_only_use_known_codes(sl):
    vocab = sl.vocabularies["event_type"]
    for group, codes in vocab.groups.items():
        assert set(codes) <= set(map(str, vocab.values)) | set(vocab.legacy), group
    # The PA rule: TRUNCATED is the only non-PA row, and it is in no outcome group.
    assert vocab.groups["not_a_pa"] == ("TRUNCATED",)
    # Catcher's interference is a PA but not an at-bat (gbdp-40's rule too).
    assert "INT" not in vocab.groups["at_bat"] and "INT" not in vocab.groups["not_a_pa"]
    assert all("TRUNCATED" not in codes for g, codes in vocab.groups.items() if g != "not_a_pa")
    # Sacrifices: neither kind is an at-bat; only flies (and legacy SAC, which
    # was always counted in OBP's denominator) are in sac_fly.
    for code in ("SF", "SH", "SAC"):
        assert code not in vocab.groups["at_bat"] and code in vocab.groups["sacrifice"]
    assert set(vocab.groups["sac_fly"]) == {"SF", "SAC"} and vocab.groups["sac_bunt"] == ("SH",)
    assert publish._map_event_type("sac_fly_double_play") == "SF"


def test_base_state_encoding_matches_the_code(sl):
    labels = sl.vocabularies["base_state"].values
    assert sorted(labels) == list(range(8))
    assert publish._base_state_from_statcast({"on_1b": "x"}) == 1
    assert publish._base_state_from_statcast({"on_2b": "x"}) == 2
    assert publish._base_state_from_statcast({"on_3b": "x"}) == 4
    assert publish._base_state_from_br("a", None, "c") == 5
    assert publish._npb_runner_bits("一三塁") == 5 and publish._npb_runner_bits("満塁") == 7
    assert labels[5] == "1st & 3rd" and labels[7] == "Loaded"


def test_source_league_matches_the_code(sl, semantic_gold):
    import duckdb

    mapping = sl.vocabularies["source_league"].values
    # Every source any gold row is stamped with has a league.
    sources = set(re.findall(r'"source": "(\w+)"', inspect.getsource(publish)))
    sources |= set(re.findall(r'_read_silver\(root, "(\w+)"', inspect.getsource(publish)))
    assert sources <= set(mapping), sorted(sources - set(mapping))
    for source, league in publish._PA_SOURCE_LEAGUE.items():
        assert mapping[source] == league
    # And it agrees with the league_id publish wrote on fact_game.
    with Path("configs/leagues.yaml").open(encoding="utf-8") as f:
        league_ids = {code: m["league_id"] for code, m in yaml.safe_load(f)["leagues"].items()}
    rows = duckdb.connect().execute(
        f"SELECT DISTINCT source, league_id FROM read_parquet('{semantic_gold}/fact_game/**/*.parquet')"
    ).fetchall()
    assert rows
    for source, league_id in rows:
        assert league_ids[mapping[source]] == league_id, source


def test_sources_listed_per_table_are_real(sl):
    known = set(sl.vocabularies["source_league"].values) | set(sl.tables) | {"configs/leagues.yaml", "generated"}
    for t in sl.tables.values():
        assert set(t.sources) <= known, (t.name, set(t.sources) - known)


def test_unity_catalog_column_names_follow_managed_publish(sl):
    from gbdp.catalog.managed_publish import uc_column_name

    assert uc_column_name("2b") == "c_2b" and uc_column_name("dt") == "dt"
    sql = render(sl.views["v_batting_line"].sql, sl, databricks_target())
    assert "b.c_2b AS doubles" in sql and "b.c_3b AS triples" in sql
    assert 'b."2b" AS doubles' in render(sl.views["v_batting_line"].sql, sl, duckdb_target())
    # External (non-managed) UC tables keep the original names.
    assert "b.`2b` AS doubles" in render(sl.views["v_batting_line"].sql, sl, databricks_target(uc_mode="external"))


def test_every_metric_view_reads_described_tables_only(sl):
    for mv in sl.metric_views:
        tables = sl.base_tables(mv)
        assert tables and set(tables) <= set(sl.tables), mv


def test_a_yaml_slip_is_refused():
    raw = semantic.load_raw()
    raw["metric_views"]["mv_batting"]["dimensions"][0]["Outs"] = None  # what an unquoted ", " produces
    with pytest.raises(semantic.SemanticLayerError, match="unknown keys"):
        semantic.parse(raw)


def test_an_inconsistent_layer_is_refused():
    raw = semantic.load_raw()
    raw["metrics"]["avg"]["metric_view"] = "mv_nope"
    with pytest.raises(semantic.SemanticLayerError, match="mv_nope"):
        semantic.parse(raw)
    raw = semantic.load_raw()
    raw["views"]["v_batting_line"]["sql"] = "SELECT * FROM {{table:not_a_table}}"
    with pytest.raises(semantic.SemanticLayerError, match="not_a_table"):
        semantic.parse(raw)
