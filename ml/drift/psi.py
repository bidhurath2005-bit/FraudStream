"""Data-drift monitoring with the Population Stability Index (PSI).

PSI = sum_i (cur_i - ref_i) * ln(cur_i / ref_i) over bins fixed on the REFERENCE
(training) distribution. Conventional interpretation (configurable):
  < 0.10 stable, 0.10-0.25 moderate shift, >= 0.25 significant shift.

PSI measures *data* drift (input distribution change). It does NOT establish concept
drift (a change in P(fraud | features)); that requires labelled outcomes and is
estimated separately from analyst-confirmed / simulator ground-truth labels.
"""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import numpy as np
import pandas as pd

from ml.features.definitions import CATEGORICAL_FEATURES, NUMERIC_FEATURES

EPS = 1e-4
DEFAULT_THRESHOLDS = {"moderate": 0.10, "significant": 0.25}


def _numeric_profile(values: np.ndarray, bins: int = 10) -> dict[str, Any]:
    qs = np.unique(np.quantile(values, np.linspace(0, 1, bins + 1)[1:-1]))
    edges = [-np.inf, *qs.tolist(), np.inf]
    counts, _ = np.histogram(values, bins=edges)
    return {"type": "numeric", "edges": [float(e) for e in qs], "proportions": (counts / max(1, counts.sum())).tolist()}


def _categorical_profile(values: Iterable[str]) -> dict[str, Any]:
    s = pd.Series(list(values)).astype(str).value_counts(normalize=True)
    return {"type": "categorical", "proportions": {k: float(v) for k, v in s.items()}}


def reference_profile(df: pd.DataFrame, scores: np.ndarray | None = None) -> dict[str, Any]:
    prof: dict[str, Any] = {"features": {}, "rows": int(len(df))}
    for c in NUMERIC_FEATURES:
        prof["features"][c] = _numeric_profile(df[c].astype(float).to_numpy())
    for c in CATEGORICAL_FEATURES:
        prof["features"][c] = _categorical_profile(df[c])
    if scores is not None:
        prof["score"] = _numeric_profile(np.asarray(scores, dtype=float))
    return prof


def psi(ref: np.ndarray, cur: np.ndarray) -> float:
    ref = np.clip(np.asarray(ref, dtype=float), EPS, None)
    cur = np.clip(np.asarray(cur, dtype=float), EPS, None)
    return float(np.sum((cur - ref) * np.log(cur / ref)))


def feature_psi(profile: dict[str, Any], values: Iterable[Any]) -> float:
    if profile["type"] == "numeric":
        vals = np.asarray(list(values), dtype=float)
        edges = [-np.inf, *profile["edges"], np.inf]
        counts, _ = np.histogram(vals, bins=edges)
        cur = counts / max(1, counts.sum())
        return psi(np.asarray(profile["proportions"]), cur)
    ref_p = profile["proportions"]
    cur_s = pd.Series(list(values)).astype(str).value_counts(normalize=True)
    cats = sorted(set(ref_p) | set(cur_s.index))
    return psi(np.array([ref_p.get(k, 0.0) for k in cats]), np.array([float(cur_s.get(k, 0.0)) for k in cats]))


def status_for(value: float, thresholds: dict[str, float] = DEFAULT_THRESHOLDS) -> str:
    if value >= thresholds["significant"]:
        return "SIGNIFICANT"
    if value >= thresholds["moderate"]:
        return "MODERATE"
    return "STABLE"


def compute_drift(reference: dict[str, Any], current: pd.DataFrame, scores: Iterable[float] | None = None,
                  thresholds: dict[str, float] = DEFAULT_THRESHOLDS, min_rows: int = 200) -> dict[str, Any]:
    if len(current) < min_rows:
        return {"status": "INSUFFICIENT_DATA", "rows": int(len(current)), "min_rows": min_rows, "features": {}}
    feats = {}
    for name, prof in reference["features"].items():
        if name not in current.columns:
            continue
        v = feature_psi(prof, current[name].tolist())
        feats[name] = {"psi": round(v, 5), "status": status_for(v, thresholds)}
    out: dict[str, Any] = {"rows": int(len(current)), "features": feats, "thresholds": thresholds}
    if scores is not None and "score" in reference:
        sv = feature_psi(reference["score"], list(scores))
        out["score_psi"] = {"psi": round(sv, 5), "status": status_for(sv, thresholds)}
    worst = max((f["psi"] for f in feats.values()), default=0.0)
    out["max_psi"] = worst
    out["status"] = status_for(worst, thresholds)
    out["drifted_features"] = sorted([k for k, f in feats.items() if f["status"] != "STABLE"],
                                     key=lambda k: -feats[k]["psi"])
    return out
