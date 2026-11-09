"""configs/ml.yaml: what a breakout is, the split cut dates, and the model artifact."""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Tuple

DEFAULT_PATH = "configs/ml.yaml"

# Event types (fact_plate_appearance.event_type) the wOBA weights may name.
WEIGHTED_EVENTS = ("BB", "HBP", "1B", "2B", "3B", "HR")


@dataclass(frozen=True)
class BreakoutConfig:
    sources: Tuple[str, ...]
    baseline_days: int
    recent_days: int
    label_days: int
    min_baseline_pa: int
    min_label_pa: int
    margin_woba: float
    woba_weights: Dict[str, float]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "sources": list(self.sources), "baseline_days": self.baseline_days, "recent_days": self.recent_days,
            "label_days": self.label_days, "min_baseline_pa": self.min_baseline_pa,
            "min_label_pa": self.min_label_pa, "margin_woba": self.margin_woba,
            "woba_weights": dict(sorted(self.woba_weights.items())),
        }

    def feature_spec(self) -> Dict[str, Any]:
        """What the feature values depend on (not the label or row thresholds)."""
        return {"sources": sorted(self.sources), "baseline_days": self.baseline_days,
                "recent_days": self.recent_days, "woba_weights": dict(sorted(self.woba_weights.items()))}


@dataclass(frozen=True)
class SplitConfig:
    name: str
    train_end: date
    validation_end: date
    embargo_days: int

    def as_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "train_end": self.train_end.isoformat(),
                "validation_end": self.validation_end.isoformat(), "embargo_days": self.embargo_days}


@dataclass(frozen=True)
class ModelConfig:
    artifact: str
    top_n_per_league: int
    min_train_rows: int
    min_positives: int
    l2_grid: Tuple[float, ...]


@dataclass(frozen=True)
class MlConfig:
    breakout: BreakoutConfig
    splits: SplitConfig
    model: ModelConfig

    @property
    def definition_version(self) -> str:
        return short_hash(self.breakout.as_dict())

    @property
    def feature_spec_hash(self) -> str:
        return short_hash(self.breakout.feature_spec())

    @property
    def split_version(self) -> str:
        return f"{self.splits.name}-{short_hash(self.splits.as_dict())}"


def short_hash(obj: Any) -> str:
    text = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]


def config_path() -> str:
    return os.environ.get("GBDP_ML_CONFIG") or DEFAULT_PATH


def ml_config() -> MlConfig:
    """The ML config at GBDP_ML_CONFIG, else configs/ml.yaml (cached per path)."""
    return _load(config_path())


@lru_cache(maxsize=None)
def _load(path: str) -> MlConfig:
    import yaml

    with Path(path).open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return parse(raw)


def parse(raw: Dict[str, Any]) -> MlConfig:
    b, s, m = raw["breakout"], raw["splits"], raw["model"]
    weights = {str(k): float(v) for k, v in b["woba_weights"].items()}
    breakout = BreakoutConfig(
        sources=tuple(b["sources"]), baseline_days=int(b["baseline_days"]), recent_days=int(b["recent_days"]),
        label_days=int(b["label_days"]), min_baseline_pa=int(b["min_baseline_pa"]),
        min_label_pa=int(b["min_label_pa"]), margin_woba=float(b["margin_woba"]), woba_weights=weights,
    )
    splits = SplitConfig(
        name=str(s["name"]), train_end=date.fromisoformat(str(s["train_end"])),
        validation_end=date.fromisoformat(str(s["validation_end"])), embargo_days=breakout.label_days,
    )
    model = ModelConfig(
        artifact=str(m["artifact"]), top_n_per_league=int(m["top_n_per_league"]),
        min_train_rows=int(m["min_train_rows"]), min_positives=int(m["min_positives"]),
        l2_grid=tuple(float(x) for x in m["l2_grid"]),
    )
    errors = []
    if not 0 < breakout.recent_days <= breakout.baseline_days:
        errors.append("recent_days must be between 1 and baseline_days")
    if not 0 < breakout.label_days < breakout.baseline_days:
        # Re-publishing a date changes features up to baseline_days - 1 days
        # later and labels up to label_days - 1 days later; gold refreshes the
        # former, which then covers the latter.
        errors.append("label_days must be between 1 and baseline_days - 1")
    if set(weights) - set(WEIGHTED_EVENTS):
        errors.append(f"woba_weights may only name {WEIGHTED_EVENTS}")
    if splits.validation_end <= splits.train_end:
        errors.append("validation_end must be after train_end")
    if errors:
        raise ValueError("configs/ml.yaml: " + "; ".join(errors))
    return MlConfig(breakout=breakout, splits=splits, model=model)


def date_split(dt: date, cfg: SplitConfig) -> str:
    """train / validation / test by date alone (run-expectancy rows)."""
    if dt <= cfg.train_end:
        return "train"
    if dt <= cfg.validation_end:
        return "validation"
    return "test"


def breakout_split(as_of: date, cfg: SplitConfig) -> str:
    """The date split, except "embargo" where the label window crosses a cut.

    A row's label covers (as_of, as_of + embargo_days]. A train row with
    as_of > train_end - embargo_days would be labeled with validation-period
    games (and a validation row with test-period games), so it is in no split.
    """
    split = date_split(as_of, cfg)
    for cut in (cfg.train_end, cfg.validation_end):
        if cut - timedelta(days=cfg.embargo_days) < as_of <= cut:
            return "embargo"
    return split
