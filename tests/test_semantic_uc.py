"""The Unity Catalog DDL generated from the semantic layer."""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import yaml

from gbdp.semantic import layer as semantic
from gbdp.semantic import uc
from gbdp.semantic.render import databricks_target

SL = semantic.load()


def _builder():
    spec = importlib.util.spec_from_file_location("build_semantic_layer", Path("scripts/build_semantic_layer.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_generated_sql_is_deterministic_and_committed():
    mod = _builder()
    first, second = mod.build(), mod.build()
    assert first == second
    assert Path("docs/semantic_layer_uc.sql").read_text(encoding="utf-8") == first, \
        "run: python scripts/build_semantic_layer.py"


def test_statements_cover_every_table_column_view_and_metric_view():
    stmts = uc.statements(SL)
    kinds = {}
    for s in stmts:
        kinds.setdefault(s.kind, []).append(s)
    described = [t for t in SL.tables.values() if not t.placeholder]
    assert {s.table for s in kinds["table_comment"]} == {t.name for t in described}
    assert len(kinds["column_comment"]) == sum(len(t.columns) for t in described)
    assert [s.table for s in kinds["view"]] == list(SL.views)
    assert [s.table for s in kinds["metric_view"]] == list(SL.metric_views)
    # Order matters: views before the metric views that read them.
    order = [s.kind for s in stmts]
    assert order.index("metric_view") > max(i for i, k in enumerate(order) if k == "view")
    # Placeholder tables have no UC table to comment on.
    assert "fact_contract" not in {s.table for s in stmts}


def test_column_comments_use_unity_catalog_names():
    cols = {(s.table, s.column) for s in uc.statements(SL) if s.kind == "column_comment"}
    assert ("fact_boxscore_batting", "c_2b") in cols and ("fact_boxscore_batting", "2b") not in cols
    ext = {(s.table, s.column) for s in uc.statements(SL, uc_mode="external") if s.kind == "column_comment"}
    assert ("fact_boxscore_batting", "2b") in ext


def test_metric_view_yaml_follows_the_spec():
    for name, mv in SL.metric_views.items():
        stmt = next(s.sql for s in uc.statements(SL) if s.kind == "metric_view" and s.table == name)
        assert stmt.startswith(f"CREATE OR REPLACE VIEW gbdp.gold_gbdp.{name}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n")
        body = yaml.safe_load(stmt.split("$$")[1])
        assert body["version"] == 1.1
        assert body["source"] == f"gbdp.gold_gbdp.{mv.source}"
        assert [d["name"] for d in body["dimensions"]] == [d.name for d in mv.dimensions]
        assert [m["name"] for m in body["measures"]] == [m for m, x in SL.metrics.items() if x.metric_view == name]
        for j in body.get("joins", []):
            # PyYAML must quote `on`, or YAML 1.1 readers see the key True.
            assert "on" in j and j["source"].startswith("gbdp.gold_gbdp.v_")
        assert all(m["comment"] and d["comment"] for m in body["measures"] for d in body["dimensions"])
        assert "{{" not in stmt


def test_string_literals_escape_for_databricks():
    t = databricks_target()
    assert t.string("Sources don't separate") == "'Sources don\\'t separate'"
    assert t.string("a\\b") == "'a\\\\b'"
    for s in uc.statements(SL):
        if s.kind in ("table_comment", "column_comment"):
            literal = re.search(r"(?:IS|COMMENT) ('(?:[^'\\]|\\.)*')$", s.sql)
            assert literal, s.sql


class _FakeCatalog:
    def __init__(self, tables):
        self.tables = tables

    def tableExists(self, name):
        return name in self.tables


class _FakeSpark:
    def __init__(self, tables, fail_on=()):
        self.catalog = _FakeCatalog(tables)
        self.tables, self.fail_on, self.ran = tables, fail_on, []

    def table(self, name):
        class _T:
            columns = self.tables[name]
        return _T()

    def sql(self, text):
        if any(f in text for f in self.fail_on):
            raise RuntimeError("[UNRESOLVED_COLUMN] boom\nstack")
        self.ran.append(text)


def test_apply_skips_missing_tables_and_columns_and_reports_failures():
    fq = "gbdp.gold_gbdp."
    tables = {fq + "fact_game": ["game_id", "dt"]}
    spark = _FakeSpark(tables, fail_on=("CREATE OR REPLACE VIEW gbdp.gold_gbdp.v_plate_appearance",))
    result = uc.apply(spark, SL, "gbdp", "gold_gbdp")
    assert "table_comment fact_game" in result.applied
    assert "column_comment fact_game.game_id" in result.applied
    assert any(s.startswith("column_comment fact_game.venue: column does not exist") for s in result.skipped)
    assert any(s.startswith("table_comment dim_player: table does not exist") for s in result.skipped)
    assert result.failed == ["view v_plate_appearance: [UNRESOLVED_COLUMN] boom"]
    # Everything after a failure still ran.
    assert any("mv_run_expectancy" in s for s in spark.ran)


def test_cli_writes_the_same_sql(tmp_path):
    from gbdp.cli import _semantic_layer

    out = tmp_path / "sl.sql"
    _semantic_layer("gbdp", "gold_gbdp", "managed", str(out))
    assert out.read_text(encoding="utf-8") == _builder().build()


def test_the_bundle_has_a_semantic_layer_job_outside_the_nightly():
    bundle = yaml.safe_load(Path("databricks.yml").read_text(encoding="utf-8"))
    jobs = bundle["resources"]["jobs"]
    params = jobs["gbdp_semantic_layer"]["tasks"][0]["spark_python_task"]["parameters"]
    assert params[0] == "semantic-layer" and "${var.gold_schema}" in params
    # Kept out of the nightly until the rebuild lands (SL-012).
    assert all(t["task_key"] != "gbdp_semantic_layer" for t in jobs["gbdp_nightly"]["tasks"])


def test_validation_creates_only_in_the_scratch_schema():
    mod = _builder()

    class _Warehouse:
        def __init__(self):
            self.sql = []

        def query(self, sql):
            self.sql.append(sql)
            if "mv_run_expectancy" in sql and sql.startswith("CREATE"):
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND run_expectancy_daily")
            return []

    wh, lines = _Warehouse(), []
    failures = mod.validate("gbdp", "gold_gbdp", "semantic_check", "managed", wh, out=lines.append)
    assert failures == 1
    writes = [s for s in wh.sql if not s.startswith("SELECT")]
    assert writes[0] == "CREATE SCHEMA IF NOT EXISTS gbdp.semantic_check"
    for s in writes[1:]:
        assert s.startswith("CREATE OR REPLACE VIEW gbdp.semantic_check."), s.splitlines()[0]
        assert "COMMENT ON" not in s and "ALTER TABLE" not in s
    # Views read the real tables and the scratch views.
    assert "FROM gbdp.gold_gbdp.fact_boxscore_batting" in "\n".join(writes)
    assert "source: gbdp.semantic_check.v_plate_appearance" in "\n".join(writes)
    queries = [s for s in wh.sql if s.startswith("SELECT")]
    assert len(queries) == len(SL.metric_views) - 1 and "MEASURE(`avg`)" in queries[0]
    import pytest

    before = len(wh.sql)
    for scratch in ("gold_gbdp", "silver_gbdp", "bronze_gbdp"):
        with pytest.raises(SystemExit):
            mod.validate("gbdp", "other_gold", scratch, "managed", wh)
    with pytest.raises(SystemExit):
        mod.validate("gbdp", "my_gold", "my_gold", "managed", wh)
    assert len(wh.sql) == before  # refused before running anything
