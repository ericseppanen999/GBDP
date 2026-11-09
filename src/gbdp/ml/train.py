"""Train and evaluate the breakout baseline on ml_breakout_examples rows.

scripts/train_breakout.py reads the rows from gold and calls train().
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from gbdp.ml import breakout, model
from gbdp.ml.config import MlConfig, breakout_split


def labeled_rows(rows: Sequence[Mapping[str, Any]], cfg: MlConfig) -> List[Dict[str, Any]]:
    """Rows with a label, built under this config's definition, in a fixed order."""
    out = [dict(r) for r in rows
           if r.get("label_status") == "labeled" and r.get("label_breakout") is not None
           and r.get("definition_version") == cfg.definition_version]
    return sorted(out, key=lambda r: (str(r["dt"]), str(r["league_code"]), str(r["player_id"])))


def _split_of(r: Mapping[str, Any], cfg: MlConfig) -> str:
    # The split comes from the config's cut dates, not the stored column, so
    # moving the cuts and retraining doesn't need a gold re-publish first.
    return breakout_split(date.fromisoformat(str(r["dt"])), cfg.splits)


def non_overlapping(rows: Sequence[Mapping[str, Any]], label_days: int) -> List[Mapping[str, Any]]:
    """One as-of date every label_days days (from the first), so no two rows of
    a player share label-window days: an honest count of independent outcomes."""
    if not rows:
        return []
    first = min(date.fromisoformat(str(r["dt"])) for r in rows)
    return [r for r in rows if (date.fromisoformat(str(r["dt"])) - first).days % label_days == 0]


def _heuristics(rows: Sequence[Mapping[str, Any]]) -> Dict[str, List[float]]:
    def val(v: Optional[float]) -> float:
        return 0.0 if v is None else float(v)

    return {
        # Regression to the mean alone: the further below his league, the likelier a jump.
        "below_league_baseline": [-val(r.get("f_base_woba_vs_league")) for r in rows],
        # Hot hand: recent wOBA above the baseline.
        "recent_minus_baseline": [val(r.get("f_recent_woba")) - val(r.get("f_base_woba")) for r in rows],
        # The old breakout_candidates placeholder: home runs on the day.
        "day_home_runs": [val(r.get("f_day_hr")) for r in rows],
    }


def _xy(rows: Sequence[Mapping[str, Any]]) -> Tuple[List[List[Optional[float]]], List[int]]:
    return [breakout.model_inputs(r) for r in rows], [1 if r["label_breakout"] else 0 for r in rows]


def _summary(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"rows": 0}
    dts = sorted(str(r["dt"]) for r in rows)
    by_league: Dict[str, int] = {}
    for r in rows:
        by_league[r["league_code"]] = by_league.get(r["league_code"], 0) + 1
    return {"rows": len(rows), "positives": sum(1 for r in rows if r["label_breakout"]),
            "players": len({(r["league_code"], r["player_id"]) for r in rows}),
            "as_of_dates": len(set(dts)), "first_as_of": dts[0], "last_as_of": dts[-1],
            "rows_by_league": dict(sorted(by_league.items()))}


def train(rows: Sequence[Mapping[str, Any]], cfg: MlConfig) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """(artifact, report). artifact is None when there isn't enough labeled
    data; report["refused"] then says why."""
    data = labeled_rows(rows, cfg)
    splits: Dict[str, List[Dict[str, Any]]] = {"train": [], "validation": [], "test": [], "embargo": []}
    for r in data:
        splits[_split_of(r, cfg)].append(r)
    report: Dict[str, Any] = {
        "definition_version": cfg.definition_version, "split_version": cfg.split_version,
        "rows_seen": len(rows), "rows_labeled": len(data),
        "splits": {k: _summary(v) for k, v in splits.items()},
    }
    train_rows, val_rows, test_rows = splits["train"], splits["validation"], splits["test"]
    problems = []
    if len(train_rows) < cfg.model.min_train_rows:
        problems.append(f"{len(train_rows)} labeled train rows (need {cfg.model.min_train_rows})")
    for name, part in (("train", train_rows), ("validation", val_rows)):
        pos = sum(1 for r in part if r["label_breakout"])
        if pos < cfg.model.min_positives or len(part) - pos < cfg.model.min_positives:
            problems.append(f"{pos} positives / {len(part) - pos} negatives in {name} "
                            f"(need {cfg.model.min_positives} of each)")
    if problems:
        report["refused"] = "not enough labeled data: " + "; ".join(problems)
        return None, report

    x_tr, y_tr = _xy(train_rows)
    means, stds = model.standardizer(x_tr)
    z_tr = [model.standardize(x, means, stds) for x in x_tr]
    x_va, y_va = _xy(val_rows)
    z_va = [model.standardize(x, means, stds) for x in x_va]

    def probs(intercept: float, w: Sequence[float], zs: Sequence[Sequence[float]]) -> List[float]:
        return [model._sigmoid(intercept + sum(a * b for a, b in zip(w, z))) for z in zs]

    grid = []
    best = None
    for l2 in cfg.model.l2_grid:
        b0, w = model.fit_logistic(z_tr, y_tr, l2)
        loss = model.log_loss(y_va, probs(b0, w, z_va))
        grid.append({"l2": l2, "validation_log_loss": round(loss, 6)})
        if best is None or loss < best[0] - 1e-12:
            best = (loss, l2, b0, w)
    _, l2, b0, w = best
    prevalence = sum(y_tr) / len(y_tr)

    def evaluate(part: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        if not part:
            return {"rows": 0}
        x, y = _xy(part)
        p = probs(b0, w, [model.standardize(v, means, stds) for v in x])
        return model.evaluate(y, p, prevalence, _heuristics(part))

    metrics = {
        "l2_grid": grid, "l2": l2, "train_positive_rate": round(prevalence, 6),
        "train": evaluate(train_rows), "validation": evaluate(val_rows), "test": evaluate(test_rows),
        "test_non_overlapping": evaluate(non_overlapping(test_rows, cfg.breakout.label_days)),
    }
    training = {k: report["splits"][k] for k in ("train", "validation", "test")}
    artifact = model.build_artifact(cfg, means, stds, b0, w, l2, training, metrics)
    report["metrics"] = metrics
    report["model_version"] = artifact["model_version"]
    report["coefficients"] = dict(zip(breakout.MODEL_INPUTS, artifact["coefficients"]))
    report["intercept"] = artifact["intercept"]
    return artifact, report
