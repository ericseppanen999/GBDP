"""The breakout baseline: L2-regularized logistic regression, dependency-free.

Small and transparent on purpose: nine standardized inputs (breakout.py's
MODEL_INPUTS), one coefficient each, fit by Newton's method. Training is
deterministic (no random init, no shuffling), so the same rows give the same
artifact byte for byte.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from gbdp.ml import breakout
from gbdp.ml.config import MlConfig

MODEL_KIND = "logistic_regression"
_EPS = 1e-15


def _sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def _solve(a: List[List[float]], b: List[float]) -> List[float]:
    """Gaussian elimination with partial pivoting (a is small and positive definite)."""
    n = len(b)
    m = [row[:] + [b[i]] for i, row in enumerate(a)]
    for col in range(n):
        piv = max(range(col, n), key=lambda r: abs(m[r][col]))
        m[col], m[piv] = m[piv], m[col]
        p = m[col][col]
        if abs(p) < 1e-300:
            raise ValueError("singular Hessian")
        for r in range(col + 1, n):
            f = m[r][col] / p
            if f:
                for c in range(col, n + 1):
                    m[r][c] -= f * m[col][c]
    x = [0.0] * n
    for r in range(n - 1, -1, -1):
        x[r] = (m[r][n] - sum(m[r][c] * x[c] for c in range(r + 1, n))) / m[r][r]
    return x


def standardizer(xs: Sequence[Sequence[Optional[float]]]) -> Tuple[List[float], List[float]]:
    """Per-input mean and standard deviation over the non-null training values."""
    d = len(xs[0])
    means, stds = [], []
    for j in range(d):
        vals = [x[j] for x in xs if x[j] is not None]
        mu = sum(vals) / len(vals) if vals else 0.0
        var = sum((v - mu) ** 2 for v in vals) / len(vals) if vals else 0.0
        means.append(mu)
        stds.append(math.sqrt(var) if var > 1e-12 else 1.0)
    return means, stds


def standardize(x: Sequence[Optional[float]], means: Sequence[float], stds: Sequence[float]) -> List[float]:
    # A null input is imputed with the training mean: 0 once standardized.
    return [0.0 if v is None else (v - m) / s for v, m, s in zip(x, means, stds)]


def fit_logistic(z: Sequence[Sequence[float]], y: Sequence[int], l2: float,
                 max_iter: int = 50, tol: float = 1e-10) -> Tuple[float, List[float]]:
    """(intercept, weights) minimizing sum log-loss + l2/2 * |weights|^2 (intercept unpenalized)."""
    d = len(z[0])
    beta = [0.0] * (d + 1)  # beta[0] is the intercept
    for _ in range(max_iter):
        grad = [0.0] * (d + 1)
        hess = [[0.0] * (d + 1) for _ in range(d + 1)]
        for xi, yi in zip(z, y):
            row = (1.0,) + tuple(xi)
            p = _sigmoid(sum(b * v for b, v in zip(beta, row)))
            w = p * (1.0 - p)
            r = p - yi
            for j in range(d + 1):
                vj = row[j]
                grad[j] += r * vj
                wj = w * vj
                hj = hess[j]
                for k in range(j, d + 1):
                    hj[k] += wj * row[k]
        for j in range(1, d + 1):
            grad[j] += l2 * beta[j]
            hess[j][j] += l2
        for j in range(d + 1):
            for k in range(j):
                hess[j][k] = hess[k][j]
        step = _solve(hess, grad)
        beta = [b - s for b, s in zip(beta, step)]
        if max(abs(s) for s in step) < tol:
            break
    return beta[0], beta[1:]


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def auc(y: Sequence[int], score: Sequence[float]) -> Optional[float]:
    """ROC AUC (Mann-Whitney, ties count half). None without both classes."""
    pos = sum(y)
    neg = len(y) - pos
    if not pos or not neg:
        return None
    order = sorted(range(len(y)), key=lambda i: score[i])
    rank_sum = 0.0
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and score[order[j + 1]] == score[order[i]]:
            j += 1
        avg_rank = (i + j) / 2.0 + 1.0
        rank_sum += avg_rank * sum(y[order[k]] for k in range(i, j + 1))
        i = j + 1
    return (rank_sum - pos * (pos + 1) / 2.0) / (pos * neg)


def log_loss(y: Sequence[int], p: Sequence[float]) -> Optional[float]:
    if not y:
        return None
    total = 0.0
    for yi, pi in zip(y, p):
        pi = min(max(pi, _EPS), 1 - _EPS)
        total -= math.log(pi) if yi else math.log(1 - pi)
    return total / len(y)


def brier(y: Sequence[int], p: Sequence[float]) -> Optional[float]:
    return sum((pi - yi) ** 2 for yi, pi in zip(y, p)) / len(y) if y else None


def _r(v: Optional[float], nd: int = 6) -> Optional[float]:
    return None if v is None else round(v, nd)


def evaluate(y: Sequence[int], p: Sequence[float], prevalence: float,
             heuristics: Dict[str, Sequence[float]]) -> Dict[str, Any]:
    """Model metrics next to the naive baseline (predict the training base rate
    for everyone: AUC 0.5) and simple ranking heuristics (AUC only)."""
    naive = [prevalence] * len(y)
    out: Dict[str, Any] = {
        "rows": len(y), "positives": int(sum(y)),
        "positive_rate": _r(sum(y) / len(y)) if y else None,
        "model": {"auc": _r(auc(y, p)), "log_loss": _r(log_loss(y, p)), "brier": _r(brier(y, p))},
        "naive_base_rate": {"auc": 0.5 if y else None, "log_loss": _r(log_loss(y, naive)),
                            "brier": _r(brier(y, naive))},
    }
    out["heuristics_auc"] = {name: _r(auc(y, s)) for name, s in sorted(heuristics.items())}
    return out


# ---------------------------------------------------------------------------
# Artifact
# ---------------------------------------------------------------------------

def model_version(artifact: Dict[str, Any]) -> str:
    from gbdp.ml.config import short_hash

    body = {k: artifact[k] for k in ("inputs", "means", "stds", "intercept", "coefficients", "feature_spec")}
    return f"breakout_logreg-{short_hash(body)}"


def build_artifact(cfg: MlConfig, means: List[float], stds: List[float], intercept: float,
                   weights: List[float], l2: float, training: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
    art: Dict[str, Any] = {
        "kind": MODEL_KIND,
        "inputs": list(breakout.MODEL_INPUTS),
        "means": [round(v, 8) for v in means],
        "stds": [round(v, 8) for v in stds],
        "intercept": round(intercept, 8),
        "coefficients": [round(v, 8) for v in weights],
        "l2": l2,
        "feature_spec": cfg.breakout.feature_spec(),
        "feature_spec_hash": cfg.feature_spec_hash,
        "label_definition": cfg.breakout.as_dict(),
        "definition_version": cfg.definition_version,
        "splits": cfg.splits.as_dict(),
        "split_version": cfg.split_version,
        "training_data": training,
        "metrics": metrics,
    }
    art["model_version"] = model_version(art)
    return art


def write_artifact(path: Path, artifact: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")


def load_artifact(cfg: MlConfig) -> Tuple[Optional[Dict[str, Any]], str]:
    """(artifact, "") when it can score rows built with cfg, else (None, why not)."""
    path = Path(cfg.model.artifact)
    if not path.exists():
        return None, f"no model artifact at {path} (train one with scripts/train_breakout.py)"
    try:
        art = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return None, f"model artifact {path} is unreadable: {exc}"
    if art.get("kind") != MODEL_KIND:
        return None, f"model artifact {path} is a {art.get('kind')!r}, not {MODEL_KIND}"
    if art.get("inputs") != list(breakout.MODEL_INPUTS):
        return None, f"model artifact {path} was trained on inputs {art.get('inputs')}, not {list(breakout.MODEL_INPUTS)}"
    if art.get("feature_spec_hash") != cfg.feature_spec_hash:
        return None, (f"model artifact {path} was trained on features {art.get('feature_spec')}, "
                      f"but configs/ml.yaml now builds {cfg.breakout.feature_spec()}")
    n = len(art["inputs"])
    if not all(len(art.get(k) or ()) == n for k in ("means", "stds", "coefficients")):
        return None, f"model artifact {path} has the wrong number of coefficients"
    if art.get("model_version") != model_version(art):
        return None, f"model artifact {path} was edited by hand: its model_version doesn't match its contents"
    return art, ""


def predict(artifact: Dict[str, Any], row: Dict[str, Any]) -> float:
    z = standardize(breakout.model_inputs(row), artifact["means"], artifact["stds"])
    return _sigmoid(artifact["intercept"] + sum(w * v for w, v in zip(artifact["coefficients"], z)))
