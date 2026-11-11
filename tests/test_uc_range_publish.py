"""register_uc over a date range: one Delta write per table, not one per table
per day (a season's gold was ~3,500 transactions and ~2.5 h on 2026-11-10)."""
from datetime import date

import pytest

from gbdp.catalog import managed_publish as mp


class _Writer:
    def __init__(self, log, df):
        self.log, self.df, self.opts = log, df, {}

    def format(self, f):
        return self

    def mode(self, m):
        self.opts["mode"] = m
        return self

    def option(self, k, v):
        self.opts[k] = v
        return self

    def partitionBy(self, *cols):
        return self

    def saveAsTable(self, name):
        self.log.append((name, self.opts.get("replaceWhere"), self.df.where_clause))


class _DF:
    def __init__(self, log, path, where_clause=None):
        self.log, self.path, self.where_clause = log, path, where_clause
        self.columns = ["id", "dt"]

    def where(self, clause):
        return _DF(self.log, self.path, clause)

    @property
    def write(self):
        return _Writer(self.log, self)


class _Reader:
    def __init__(self, log):
        self.log = log

    def format(self, f):
        return self

    def load(self, path):
        return _DF(self.log, path)


class _Spark:
    def __init__(self):
        self.writes = []
        self.read = _Reader(self.writes)


@pytest.fixture
def lake(tmp_path, monkeypatch):
    gold, silver = tmp_path / "gold", tmp_path / "silver"
    for t in ("fact_game", "fact_pitch"):
        (gold / t / "_delta_log").mkdir(parents=True)
    (silver / "mlb_statsapi" / "games" / "_delta_log").mkdir(parents=True)
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(gold))
    monkeypatch.setenv("GBDP_SILVER_ROOT", str(silver))
    monkeypatch.setenv("GBDP_BRONZE_ROOT", str(tmp_path / "bronze"))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "delta")
    monkeypatch.setattr(mp, "spark_path", lambda p: str(p))
    monkeypatch.setattr(mp, "_sanitize_for_uc", lambda df: df)
    monkeypatch.setattr(mp, "_table_exists", lambda spark, name: True)
    monkeypatch.setattr(mp, "_ensure_schema", lambda *a: None)
    monkeypatch.setattr(mp, "_publish_bronze_parsed", lambda *a: None)
    monkeypatch.setattr(mp, "_publish_bronze_requests", lambda *a: None)
    spark = _Spark()
    monkeypatch.setattr(mp, "_spark_session", lambda: spark)
    return spark, gold


def test_a_range_is_one_write_per_table(lake):
    spark, _ = lake
    mp.publish_uc_managed("2026-05-13", "2026-11-05", "gbdp", "bronze_gbdp", "silver_gbdp", "gold_gbdp")
    where = "dt >= '2026-05-13' AND dt <= '2026-11-05'"
    assert sorted(spark.writes) == sorted([
        ("gbdp.gold_gbdp.fact_game", where, where),
        ("gbdp.gold_gbdp.fact_pitch", where, where),
        ("gbdp.silver_gbdp.mlb_statsapi_games", where, where),
    ])


def test_gold_only_range_skips_silver(lake):
    spark, _ = lake
    mp.publish_uc_managed("2026-05-13", "2026-05-14", "gbdp", "b", "s", "g", gold_only=True)
    assert {w[0] for w in spark.writes} == {"gbdp.g.fact_game", "gbdp.g.fact_pitch"}


def test_only_the_parquet_tables_go_per_day(lake, monkeypatch):
    # gold's audit_* tables are dt= parquet folders, not Delta roots.
    spark, gold = lake
    (gold / "audit_quality" / "dt=2026-05-13").mkdir(parents=True)
    calls = []
    monkeypatch.setattr(mp, "_publish_gold", lambda spark, d, c, s, e, only=None: calls.append((d, only)))
    mp.publish_uc_managed("2026-05-13", "2026-05-15", "gbdp", "b", "s", "g", gold_only=True)
    assert calls == [(date(2026, 5, d), {"audit_quality"}) for d in (13, 14, 15)]
    assert {w[0] for w in spark.writes} == {"gbdp.g.fact_game", "gbdp.g.fact_pitch"}


def test_a_single_day_keeps_the_per_day_path(lake, monkeypatch):
    spark, _ = lake
    days = []
    monkeypatch.setattr(mp, "_publish_gold", lambda spark, d, c, s, e, only=None: days.append(d))
    mp.publish_uc_managed("2026-05-13", "2026-05-13", "gbdp", "b", "s", "g", gold_only=True)
    assert days == [date(2026, 5, 13)] and spark.writes == []
