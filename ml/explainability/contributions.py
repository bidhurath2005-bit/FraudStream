"""Per-prediction feature contributions computed from the ACTUAL fitted model.

* XGBoost: exact TreeSHAP values via ``Booster.predict(pred_contribs=True)`` (log-odds space).
* Logistic regression: coef_j * x_j on the standardised design matrix (log-odds space,
  relative to the training mean for numeric features).
* Random forest: Saabas decision-path attribution (change in leaf class-1 fraction along
  each tree's path, averaged over trees) — a local decomposition, NOT Shapley values.
* Isolation forest: no per-feature attribution is produced.

One-hot columns are summed back to their source categorical feature.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from ml.features.definitions import CATEGORICAL_FEATURES


def _source_feature(transformed_name: str) -> str:
    name = transformed_name.split("__", 1)[-1]
    for cat in CATEGORICAL_FEATURES:
        if name.startswith(cat + "_"):
            return cat
    return name


def _aggregate(names: list[str], values: np.ndarray) -> dict[str, float]:
    out: dict[str, float] = {}
    for n, v in zip(names, values, strict=True):
        src = _source_feature(n)
        out[src] = out.get(src, 0.0) + float(v)
    return out


def explain(pipeline: Any, row: pd.DataFrame | None, algorithm: str, *, xt: np.ndarray | None = None,
            names: list[str] | None = None) -> dict[str, Any] | None:
    """``xt``/``names`` may be passed pre-transformed (one row) to avoid re-running preprocessing."""
    pre, est = pipeline.named_steps["pre"], pipeline.named_steps["model"]
    if xt is None:
        xt = pre.transform(row)
        xt = xt.toarray() if hasattr(xt, "toarray") else np.asarray(xt)
    xt = np.asarray(xt).reshape(1, -1)
    names = names or list(pre.get_feature_names_out())
    if algorithm == "xgboost":
        import xgboost as xgb

        contrib = est.get_booster().predict(xgb.DMatrix(xt, nthread=1), pred_contribs=True)[0]
        return {"method": "xgboost_treeshap", "space": "log_odds", "base_value": float(contrib[-1]),
                "contributions": _aggregate(names, contrib[:-1])}
    if algorithm == "logistic_regression":
        contrib = xt[0] * est.coef_[0]
        return {"method": "linear_coef_x_value", "space": "log_odds", "base_value": float(est.intercept_[0]),
                "contributions": _aggregate(names, contrib)}
    if algorithm == "random_forest":
        total = np.zeros(xt.shape[1])
        base = 0.0
        for tree in est.estimators_:
            t = tree.tree_
            val = t.value[:, 0, :]
            frac = val[:, 1] / np.maximum(val.sum(axis=1), 1e-12) if val.shape[1] > 1 else np.zeros(len(val))
            path = tree.decision_path(xt).indices
            base += frac[path[0]]
            for parent, child in zip(path[:-1], path[1:], strict=False):
                total[t.feature[parent]] += frac[child] - frac[parent]
        n = len(est.estimators_)
        return {"method": "saabas_path_attribution", "space": "probability_score", "base_value": base / n,
                "contributions": _aggregate(names, total / n)}
    return None


def top_contributions(expl: dict[str, Any] | None, k: int = 6) -> list[dict[str, float]]:
    if not expl:
        return []
    items = sorted(expl["contributions"].items(), key=lambda kv: abs(kv[1]), reverse=True)[:k]
    return [{"feature": f, "contribution": round(v, 5)} for f, v in items]
