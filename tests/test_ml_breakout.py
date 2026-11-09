"""ML-ready gold: breakout features and labels without leakage, time splits,
the baseline model, and breakout_candidates scoring (gbdp/ml, gold/publish.py).

Synthetic plate appearances go straight into gold's fact_plate_appearance;
the real ML writers then run date by date, as publish_gold runs them.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List

import pytest
import yaml

from gbdp.gold import publish
from gbdp.ml import breakout, model
from gbdp.ml.config import breakout_split, date_split, ml_config, parse
from gbdp.ml.train import non_overlapping, train
from gbdp.utils.io import read_parquet_rows

START = date(2025, 5, 1)
DAYS = 24  # 2025-05-01 .. 2025-05-24
BASE, RECENT, LABEL = 6, 3, 3
EVENTS = ["1B", "2B", "HR", "BB", "K", "OUT", "OUT", "HBP", "SF", "DP", "TRUNCATED", "SH"]
PLAYERS = [("p1", "mlb_statcast"), ("p2", "mlb_statcast"), ("p3", "npb_spaia")]


def _raw_config(tmp: Path, **breakout_over) -> Dict:
    raw = yaml.safe_load(Path("configs/ml.yaml").read_text(encoding="utf-8"))
    raw["breakout"].update({"baseline_days": BASE, "recent_days": RECENT, "label_days": LABEL,
                            "min_baseline_pa": 4, "min_label_pa": 3, **breakout_over})
    raw["splits"].update(train_end="2025-05-10", validation_end="2025-05-17")
    raw["model"].update(artifact=str(tmp / "model.json"), top_n_per_league=2)
    return raw


@pytest.fixture
def env(tmp_path, monkeypatch):
    raw = _raw_config(tmp_path)
    (tmp_path / "ml.yaml").write_text(yaml.safe_dump(raw), encoding="utf-8")
    monkeypatch.setenv("GBDP_ML_CONFIG", str(tmp_path / "ml.yaml"))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    return tmp_path


def _events(player: str, day: date, salt: str = "") -> List[str]:
    """3-5 deterministic outcomes for a player on a day (none on some days)."""
    h = hashlib.sha1(f"{player}:{day}:{salt}".encode()).digest()
    if h[0] % 7 == 0:
        return []  # day off
    return [EVENTS[b % len(EVENTS)] for b in h[1 : 4 + h[1] % 3]]


def _pas(day: date, salt_after: date | None = None, salt: str = "x", salt_before: date | None = None):
    rows = []
    for player, source in PLAYERS:
        s = ""
        if salt_after is not None and day > salt_after:
            s = salt
        if salt_before is not None and day <= salt_before:
            s = salt
        for i, ev in enumerate(_events(player, day, s)):
            rows.append({"pa_id": f"{player}-{day}-{i}", "batter_id": player, "event_type": ev, "source": source})
    return rows


def _days(n: int = DAYS) -> List[date]:
    return [START + timedelta(days=i) for i in range(n)]


def _publish_ml(gold: Path, days: List[date], pas_for=_pas, refresh: bool = True) -> None:
    """The ML steps of publish_gold, for these dates in order, then its refresh."""
    for d in days:
        publish._write_gold_table(gold, "fact_plate_appearance", d, pas_for(d), True)
        publish._write_feature_batter_daily(gold, d, True)
        publish._write_ml_split(gold, d, True)
        publish._write_ml_breakout_examples(gold, d, True)
        publish._write_breakout_candidates(gold, d, True)
    if refresh and days:
        publish._refresh_ml_after(gold, days[-1], True)


def _examples(gold: Path, d: date) -> Dict[tuple, Dict]:
    path = gold / "ml_breakout_examples" / f"dt={d.isoformat()}"
    rows = read_parquet_rows(path) if path.exists() else []
    return {(r["player_id"], r["league_code"]): {k: v for k, v in r.items() if k != "ingested_at_utc"} for r in rows}


FEATURES = [c for c in breakout.FEATURE_COLUMNS]
LABELS = ["label_pa", "label_woba", "label_breakout", "label_status"]


# ---------------------------------------------------------------------------
# Hand-computed reference (independent of gbdp.ml.breakout)
# ---------------------------------------------------------------------------

W = {"BB": 0.69, "HBP": 0.72, "1B": 0.88, "2B": 1.25, "3B": 1.58, "HR": 2.03}


def _line(player: str, days: List[date], pas_for=_pas):
    pa = num = den = 0
    for d in days:
        for r in pas_for(d):
            if r["batter_id"] != player or r["event_type"] == "TRUNCATED":
                continue
            ev = r["event_type"]
            pa += 1
            num += W.get(ev, 0.0)
            den += ev in {"1B", "2B", "3B", "HR", "K", "OUT", "DP", "TP", "FC", "ERROR", "BB", "HBP", "SF"}
    return pa, (num / den if den else None)


def test_features_and_labels_match_a_hand_computation(env):
    gold = env / "gold"
    _publish_ml(gold, _days())
    checked = 0
    for as_of in _days()[BASE:DAYS - LABEL]:
        for (player, _), row in _examples(gold, as_of).items():
            base_days = [as_of - timedelta(days=i) for i in range(BASE)]
            label_days = [as_of + timedelta(days=i) for i in range(1, LABEL + 1)]
            pa, w = _line(player, base_days)
            assert row["f_base_pa"] == pa and row["f_base_woba"] == pytest.approx(w, abs=1e-6)
            lpa, lw = _line(player, label_days)
            assert row["label_pa"] == lpa
            if lw is not None:
                assert row["label_woba"] == pytest.approx(lw, abs=1e-6)
            if lpa >= 3 and lw is not None:
                assert row["label_status"] == "labeled"
                assert row["label_breakout"] == (round(lw - w, 6) >= 0.05 - 1e-9)
            else:
                assert row["label_status"] == "insufficient_pa" and row["label_breakout"] is None
            checked += 1
    assert checked > 20


# ---------------------------------------------------------------------------
# Leakage
# ---------------------------------------------------------------------------

def test_features_never_read_past_the_as_of_date(env, tmp_path):
    """Change every PA after day X: no feature of any as-of date <= X moves."""
    x = START + timedelta(days=12)
    a, b = env / "a", env / "b"
    _publish_ml(a, _days())
    _publish_ml(b, _days(), pas_for=lambda d: _pas(d, salt_after=x))
    moved_labels = 0
    for d in _days():
        ea, eb = _examples(a, d), _examples(b, d)
        if d <= x:
            assert ea.keys() == eb.keys(), d
            for k in ea:
                assert {c: ea[k][c] for c in FEATURES} == {c: eb[k][c] for c in FEATURES}, (d, k)
                moved_labels += any(ea[k][c] != eb[k][c] for c in LABELS)
    # The change is real: labels whose windows cross X did move.
    assert moved_labels > 0


def test_labels_never_read_the_feature_window_or_past_their_end(env):
    """Change every PA up to day X, and separately every PA after X + label_days:
    label_pa and label_woba of as-of X don't move."""
    x = START + timedelta(days=12)
    end = x + timedelta(days=LABEL)
    a, before, after = env / "a", env / "before", env / "after"
    _publish_ml(a, _days())
    _publish_ml(before, _days(), pas_for=lambda d: _pas(d, salt_before=x))
    _publish_ml(after, _days(), pas_for=lambda d: _pas(d, salt_after=end))
    ea, eb, ec = _examples(a, x), _examples(before, x), _examples(after, x)
    common = ea.keys() & eb.keys()
    assert common and ea.keys() == ec.keys()
    for k in common:
        assert (ea[k]["label_pa"], ea[k]["label_woba"]) == (eb[k]["label_pa"], eb[k]["label_woba"]), k
    for k in ea:
        assert ea[k] == ec[k], k  # nothing at all depends on days past the label window


def test_a_label_is_null_until_its_whole_window_is_published(env):
    gold = env / "gold"
    days = _days(15)
    _publish_ml(gold, days)
    last = days[-1]
    for d in days:
        for row in _examples(gold, d).values():
            if d + timedelta(days=LABEL) > last:
                assert row["label_status"] == "pending"
                assert all(row[c] is None for c in ("label_pa", "label_woba", "label_woba_delta", "label_breakout"))
            else:
                assert row["label_status"] in ("labeled", "insufficient_pa")
    # Publishing the next day completes exactly one more as-of date.
    nxt = last + timedelta(days=1)
    _publish_ml(gold, [nxt])
    filled = nxt - timedelta(days=LABEL)
    assert {r["label_status"] for r in _examples(gold, filled).values()} <= {"labeled", "insufficient_pa"}
    assert all(r["label_status"] == "pending" for r in _examples(gold, filled + timedelta(days=1)).values())


def test_a_gap_in_published_dates_leaves_labels_pending_not_wrong(env):
    gold = env / "gold"
    days = _days(18)
    hole = days[10]
    _publish_ml(gold, [d for d in days if d != hole])
    for d in days:
        if d == hole:
            continue
        lo, hi = breakout.label_window(d, ml_config().breakout)
        for row in _examples(gold, d).values():
            if lo <= hole <= hi:
                assert row["label_status"] == "pending", d
    # Publishing the missing date later fills them (its step and the refresh after it).
    _publish_ml(gold, [hole])
    for d in days:
        lo, hi = breakout.label_window(d, ml_config().breakout)
        if lo <= hole <= hi and hi <= days[-1]:
            assert all(r["label_status"] != "pending" for r in _examples(gold, d).values()), d


def test_republishing_old_dates_gives_what_a_clean_build_gives(env):
    """Re-publish days 8-10 with different PAs, as a correction would, then
    compare every partition with a from-scratch build of the corrected data."""
    fixed = {START + timedelta(days=i) for i in (7, 8, 9)}

    def corrected(d):
        return _pas(d, salt_after=START - timedelta(days=1), salt="fix") if d in fixed else _pas(d)

    gold, clean = env / "gold", env / "clean"
    _publish_ml(gold, _days())
    _publish_ml(gold, sorted(fixed), pas_for=corrected)
    _publish_ml(clean, _days(), pas_for=corrected)
    for d in _days():
        assert _examples(gold, d) == _examples(clean, d), d
        for table in ("breakout_candidates", "ml_split"):
            pa, pc = gold / table / f"dt={d}", clean / table / f"dt={d}"
            ra = sorted(json.dumps({k: v for k, v in r.items() if k != "ingested_at_utc"}, sort_keys=True)
                        for r in (read_parquet_rows(pa) if pa.exists() else []))
            rc = sorted(json.dumps({k: v for k, v in r.items() if k != "ingested_at_utc"}, sort_keys=True)
                        for r in (read_parquet_rows(pc) if pc.exists() else []))
            assert ra == rc, (table, d)


def test_step_windows_stay_on_their_side_of_the_as_of_date():
    cfg = parse(yaml.safe_load(Path("configs/ml.yaml").read_text(encoding="utf-8"))).breakout
    d = date(2025, 6, 1)
    for prefix, as_of in (("d", d), ("a", d - timedelta(days=cfg.label_days))):
        w = breakout.step_windows(d, cfg)
        for name in ("base", "recent", "day"):
            lo, hi = w[f"{prefix}_{name}"]
            assert hi == as_of and lo <= hi
        lo, hi = w[f"{prefix}_label"]
        assert lo == as_of + timedelta(days=1) and hi == as_of + timedelta(days=cfg.label_days)
    assert w["d_base"][1] - w["d_base"][0] == timedelta(days=cfg.baseline_days - 1)


# ---------------------------------------------------------------------------
# Splits
# ---------------------------------------------------------------------------

def test_splits_are_by_date_with_an_embargo_before_each_cut(env):
    s = ml_config().splits
    assert s.embargo_days == LABEL
    d = lambda x: date.fromisoformat(x)  # noqa: E731
    assert [date_split(d(x), s) for x in ("2025-05-10", "2025-05-11", "2025-05-17", "2025-05-18")] == [
        "train", "validation", "validation", "test"]
    # Train rows whose 3-day label window reaches past 05-10 are embargoed.
    assert [breakout_split(d(x), s) for x in ("2025-05-07", "2025-05-08", "2025-05-10", "2025-05-11")] == [
        "train", "embargo", "embargo", "validation"]
    assert [breakout_split(d(x), s) for x in ("2025-05-14", "2025-05-15", "2025-05-18")] == [
        "validation", "embargo", "test"]
    # No train label window overlaps a validation or test date, and no
    # validation label window a test date.
    for i in range(60):
        a = date(2025, 4, 20) + timedelta(days=i)
        split = breakout_split(a, s)
        end = a + timedelta(days=s.embargo_days)
        if split == "train":
            assert end <= s.train_end
        if split == "validation":
            assert end <= s.validation_end


def test_ml_split_and_example_rows_record_the_split(env):
    gold = env / "gold"
    _publish_ml(gold, _days())
    cfg = ml_config()
    for d in _days():
        (row,) = read_parquet_rows(gold / "ml_split" / f"dt={d}")
        assert row["split"] == date_split(d, cfg.splits) and row["breakout_split"] == breakout_split(d, cfg.splits)
        assert (row["train_end"], row["validation_end"], row["embargo_days"]) == ("2025-05-10", "2025-05-17", LABEL)
        assert row["split_version"] == cfg.split_version
        for e in _examples(gold, d).values():
            assert e["split"] == row["breakout_split"] and e["split_version"] == cfg.split_version


def test_re_training_view_splits_plate_appearances_by_date(semantic_gold):
    from gbdp.serve.db import run_sql

    rows = run_sql("SELECT dt, split, count(*) AS n, sum(runs_to_end_of_inning) AS runs "
                   "FROM v_re_training GROUP BY dt, split ORDER BY dt")
    # The fixture's one complete half-inning on 2026-05-01 (7 rows; PA 8 ends
    # the game, so its runs are known too), all after the cuts: test.
    assert rows and all(r["split"] == "test" for r in rows)
    total = run_sql("SELECT count(*) AS n FROM fact_plate_appearance WHERE runs_to_end_of_inning IS NOT NULL "
                    "AND base_state_before IS NOT NULL AND outs_before BETWEEN 0 AND 2")[0]["n"]
    assert sum(r["n"] for r in rows) == total


# ---------------------------------------------------------------------------
# breakout_candidates
# ---------------------------------------------------------------------------

def _artifact(cfg, coef0: float = -2.0) -> Dict:
    n = len(breakout.MODEL_INPUTS)
    return model.build_artifact(cfg, [0.0] * n, [1.0] * n, -1.0, [coef0] + [0.0] * (n - 1), 1.0, {}, {})


def test_no_model_means_no_candidates_and_a_logged_reason(env, caplog):
    gold = env / "gold"
    with caplog.at_level(logging.WARNING, logger="gbdp.gold"):
        _publish_ml(gold, _days(12))
    assert any(_examples(gold, d) for d in _days(12))
    assert not list((gold / "breakout_candidates").glob("dt=*/*.parquet"))
    assert "no model artifact" in caplog.text


def test_a_model_for_another_feature_definition_is_refused(env, caplog):
    other = parse(_raw_config(env, baseline_days=BASE + 1))
    model.write_artifact(env / "model.json", _artifact(other))
    with caplog.at_level(logging.WARNING, logger="gbdp.gold"):
        _publish_ml(env / "gold", _days(12))
    assert not list((env / "gold" / "breakout_candidates").glob("dt=*/*.parquet"))
    assert "trained on features" in caplog.text
    # A hand-edited artifact is refused too.
    art = _artifact(ml_config())
    art["coefficients"][0] = 5.0
    model.write_artifact(env / "model.json", art)
    assert model.load_artifact(ml_config())[0] is None


def test_candidates_are_ranked_model_scores_with_the_old_columns(env):
    cfg = ml_config()
    art = _artifact(cfg)
    model.write_artifact(env / "model.json", art)
    gold = env / "gold"
    _publish_ml(gold, _days())
    seen = 0
    for d in _days():
        path = gold / "breakout_candidates" / f"dt={d}"
        if not path.exists():
            continue
        rows = read_parquet_rows(path)
        ex = _examples(gold, d)
        by_league: Dict[str, List[Dict]] = {}
        for r in rows:
            by_league.setdefault(r["league_code"], []).append(r)
        for league, rs in by_league.items():
            rs.sort(key=lambda r: r["rank"])
            assert [r["rank"] for r in rs] == list(range(1, len(rs) + 1)) and len(rs) <= 2
            assert [r["score"] for r in rs] == sorted((r["score"] for r in rs), reverse=True)
            for r in rs:
                e = ex[(r["player_id"], league)]
                assert r["score"] == pytest.approx(model.predict(art, e), abs=1e-6)
                assert (r["hr"], r["h"], r["ab"]) == (e["f_day_hr"], e["f_day_h"], e["f_day_ab"])
                assert r["model_version"] == art["model_version"] and r["horizon_days"] == LABEL
            # coefficient -2 on base_woba_vs_league: the lowest baseline ranks first
            best = min((e for (p, lg), e in ex.items() if lg == league), key=lambda e: e["f_base_woba_vs_league"])
            assert rs[0]["player_id"] == best["player_id"]
            seen += 1
    assert seen > 10


# ---------------------------------------------------------------------------
# Daily stats, vocabulary
# ---------------------------------------------------------------------------

def test_event_groups_are_the_semantic_layers():
    from gbdp.semantic import layer

    groups = layer.load().vocabularies["event_type"].groups
    assert breakout.NOT_A_PA == set(groups["not_a_pa"])
    assert breakout.AT_BAT == set(groups["at_bat"])
    assert breakout.HIT == set(groups["hit"])
    assert breakout.SAC_FLY == set(groups["sac_fly"])
    assert breakout.SAC_BUNT == set(groups["sac_bunt"])


def test_batter_daily_rows_count_a_hand_line(env):
    pas = [{"batter_id": "p", "source": "mlb_statcast", "event_type": e}
           for e in ("1B", "HR", "BB", "K", "SF", "SH", "TRUNCATED", "INT", "UNKNOWN", "2B")]
    pas.append({"batter_id": "q", "source": "retrosheet_local", "event_type": "HR"})  # not a configured source
    pas.append({"batter_id": None, "source": "mlb_statcast", "event_type": "HR"})
    (row,) = breakout.batter_daily_rows(pas, START, ml_config().breakout, publish._PA_SOURCE_LEAGUE)
    assert (row["pa"], row["ab"], row["h"], row["hr"], row["doubles"], row["bb"], row["so"], row["sf"], row["sh"]) == (
        9, 4, 3, 1, 1, 1, 1, 1, 1)
    assert row["woba_den"] == 4 + 1 + 1  # AB + BB + SF
    assert row["woba_value"] == pytest.approx(0.88 + 2.03 + 0.69 + 1.25)


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def test_logistic_regression_recovers_known_coefficients():
    # Deterministic grid of inputs with labels drawn from known logits.
    z, y = [], []
    for i in range(4000):
        a = ((i * 37) % 101) / 50.0 - 1.0
        b = ((i * 53) % 97) / 48.0 - 1.0
        p = 1 / (1 + math.exp(-(-0.5 + 1.5 * a - 1.0 * b)))
        u = ((i * 7919) % 10007) / 10007.0
        z.append([a, b])
        y.append(1 if u < p else 0)
    b0, w = model.fit_logistic(z, y, l2=0.0)
    assert b0 == pytest.approx(-0.5, abs=0.15) and w[0] == pytest.approx(1.5, abs=0.2)
    assert w[1] == pytest.approx(-1.0, abs=0.2)


def test_auc_and_log_loss_on_small_cases():
    assert model.auc([0, 0, 1, 1], [0.1, 0.4, 0.35, 0.8]) == pytest.approx(0.75)
    assert model.auc([0, 1], [0.5, 0.5]) == 0.5
    assert model.auc([1, 1], [0.1, 0.2]) is None
    assert model.log_loss([1, 0], [0.5, 0.5]) == pytest.approx(math.log(2))


def _labeled_rows(cfg, n_days: int = 80, players: int = 40):
    rows = []
    for di in range(n_days):
        d = date(2025, 4, 1) + timedelta(days=di)
        for pi in range(players):
            h = hashlib.sha1(f"{pi}:{di}".encode()).digest()
            vs = (h[0] - 128) / 1000.0
            rows.append({"player_id": f"p{pi}", "league_code": "MLB" if pi % 3 else "NPB", "dt": d.isoformat(),
                         "f_base_pa": 60 + h[1] % 50, "f_base_woba": 0.32 + vs, "f_base_woba_vs_league": vs,
                         "f_recent_woba": 0.32 + vs + (h[2] - 128) / 2000.0, "f_recent_pa": 15 + h[3] % 10,
                         "f_base_k_rate": 0.2, "f_base_bb_rate": 0.08, "f_base_iso": 0.15, "f_base_babip": 0.3,
                         "baseline_days": 30, "recent_days": 7, "f_day_hr": h[4] % 2,
                         "label_status": "labeled", "label_breakout": (h[5] + (128 - h[0]) // 2) % 256 > 150,
                         "definition_version": cfg.definition_version})
    return rows


def test_training_is_deterministic_and_reports_baselines(env):
    raw = _raw_config(env)
    raw["splits"].update(train_end="2025-05-20", validation_end="2025-06-03")
    raw["model"].update(min_train_rows=100, min_positives=10)
    cfg = parse(raw)
    rows = _labeled_rows(cfg)
    art1, rep1 = train(rows, cfg)
    art2, _ = train(list(reversed(rows)), cfg)
    assert art1 is not None and art1 == art2
    assert json.dumps(art1, sort_keys=True) == json.dumps(art2, sort_keys=True)
    for part in ("validation", "test"):
        m = rep1["metrics"][part]
        assert m["rows"] > 0 and m["model"]["auc"] is not None and m["naive_base_rate"]["auc"] == 0.5
        assert set(m["heuristics_auc"]) == {"below_league_baseline", "recent_minus_baseline", "day_home_runs"}
    # Embargoed rows are in no split.
    assert rep1["splits"]["embargo"]["rows"] == 40 * 2 * LABEL
    # The artifact scores rows built with this definition.
    path = env / "m.json"
    model.write_artifact(path, art1)
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["model_version"] == model.model_version(loaded)


def test_training_refuses_without_enough_labeled_data(env):
    cfg = ml_config()
    rows = _labeled_rows(cfg, n_days=5, players=5)
    art, rep = train(rows, cfg)
    assert art is None and "not enough labeled data" in rep["refused"]
    # Pending and insufficient rows never count.
    for r in rows:
        r["label_status"] = "pending"
    assert train(rows, cfg)[1]["rows_labeled"] == 0


def test_non_overlapping_keeps_one_as_of_date_per_label_window():
    rows = [{"dt": (date(2025, 6, 1) + timedelta(days=i)).isoformat()} for i in range(10)]
    assert [r["dt"] for r in non_overlapping(rows, 3)] == ["2025-06-01", "2025-06-04", "2025-06-07", "2025-06-10"]


def test_committed_artifact_matches_the_committed_config():
    """configs/models/breakout_logreg.json must score what configs/ml.yaml builds."""
    cfg = parse(yaml.safe_load(Path("configs/ml.yaml").read_text(encoding="utf-8")))
    art, why = model.load_artifact(cfg)
    assert art is not None, why
    assert art["definition_version"] == cfg.definition_version
    assert art["split_version"] == cfg.split_version


def test_ml_publish_is_opt_in_and_registered():
    import gbdp.pipeline.runner as runner

    assert "ml_publish" in runner._pipeline_stages()
    assert "ml_publish" in runner._OPT_IN_STAGES


def test_publish_ml_runs_only_the_ml_steps(monkeypatch, tmp_path):
    from datetime import date as _date

    from gbdp.gold import publish

    calls = []
    for name in ("_write_feature_batter_daily", "_write_ml_split", "_write_ml_breakout_examples", "_write_breakout_candidates"):
        monkeypatch.setattr(publish, name, lambda root, dt, force, name=name: calls.append((name, dt.isoformat())) or tmp_path)
    monkeypatch.setattr(publish, "_refresh_ml_after", lambda root, last, force: calls.append(("refresh", last.isoformat())) or [])
    monkeypatch.setattr(publish, "_write_fact_plate_appearance", lambda *a, **k: (_ for _ in ()).throw(AssertionError("gold table written")))
    publish.publish_ml("2026-05-13", "2026-05-14", root=tmp_path)
    assert [c[0] for c in calls] == ["_write_feature_batter_daily", "_write_ml_split", "_write_ml_breakout_examples",
                                     "_write_breakout_candidates"] * 2 + ["refresh"]
    assert [c[1] for c in calls[:4]] == ["2026-05-13"] * 4 and calls[-1] == ("refresh", "2026-05-14")
