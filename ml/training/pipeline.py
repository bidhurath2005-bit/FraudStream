"""Model training pipeline: features -> temporal split -> 4 models -> validation threshold
-> held-out test evaluation -> versioned artifacts + registry entries.

Baseline-first: logistic regression is trained first; more complex models are only
preferred if they beat it on VALIDATION PR-AUC. The final test split is used only for
reporting, never for model or threshold selection.
"""
from __future__ import annotations

import json
import logging
import platform
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import sklearn
import xgboost
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from ml.drift.psi import reference_profile
from ml.evaluation.metrics import choose_threshold, classification_report, measure_latency
from ml.features.definitions import CATEGORICAL_FEATURES, FEATURE_CONFIG_VERSION, NUMERIC_FEATURES
from ml.preprocessing.splits import Splits, temporal_split, xy

log = logging.getLogger("fraudstream.ml.training")

SUPERVISED = ("logistic_regression", "random_forest", "xgboost")
ANOMALY = "isolation_forest"


@dataclass
class TrainingConfig:
    seed: int = 7
    train_frac: float = 0.6
    val_frac: float = 0.2
    holdout_customer_frac: float = 0.15
    threshold_strategy: str = "max_f1"
    target_recall: float = 0.9
    models: tuple[str, ...] = SUPERVISED + (ANOMALY,)
    hyperparameters: dict[str, dict[str, Any]] = field(default_factory=lambda: {
        "logistic_regression": {"C": 1.0, "max_iter": 2000, "class_weight": "balanced"},
        "random_forest": {"n_estimators": 150, "max_depth": 14, "min_samples_leaf": 3,
                          "class_weight": "balanced_subsample"},
        "xgboost": {"n_estimators": 300, "max_depth": 5, "learning_rate": 0.08, "subsample": 0.9,
                    "colsample_bytree": 0.9, "min_child_weight": 2},
        "isolation_forest": {"n_estimators": 200, "contamination": "auto", "max_samples": 4096},
    })


def _preprocessor(scale: bool) -> ColumnTransformer:
    num = StandardScaler() if scale else "passthrough"
    return ColumnTransformer([
        ("num", num, list(NUMERIC_FEATURES)),
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), list(CATEGORICAL_FEATURES)),
    ])


def build_pipeline(name: str, hp: dict[str, Any], seed: int, pos_weight: float) -> Pipeline:
    if name == "logistic_regression":
        est: Any = LogisticRegression(random_state=seed, **hp)
        return Pipeline([("pre", _preprocessor(True)), ("model", est)])
    if name == "random_forest":
        est = RandomForestClassifier(random_state=seed, n_jobs=-1, **hp)
    elif name == "xgboost":
        est = xgboost.XGBClassifier(random_state=seed, n_jobs=4, eval_metric="aucpr",
                                    scale_pos_weight=pos_weight, tree_method="hist", **hp)
    elif name == "isolation_forest":
        est = IsolationForest(random_state=seed, n_jobs=-1, **hp)
    else:
        raise ValueError(name)
    return Pipeline([("pre", _preprocessor(name == "isolation_forest")), ("model", est)])


class AnomalyScorer:
    """Wraps IsolationForest: maps raw anomaly score to its percentile among TRAINING scores
    (0 = most normal, 1 = more anomalous than every training event). Not a probability."""

    def __init__(self, pipeline: Pipeline, train_raw: np.ndarray) -> None:
        self.pipeline = pipeline
        self.reference = np.sort(train_raw)

    def raw(self, x: pd.DataFrame) -> np.ndarray:
        return -self.pipeline.score_samples(x)

    def score(self, x: pd.DataFrame) -> np.ndarray:
        return np.searchsorted(self.reference, self.raw(x), side="right") / len(self.reference)

    def score_transformed(self, xt: np.ndarray, fast: Any = None) -> np.ndarray:
        """Same as score() for input already passed through the fitted preprocessor.
        ``fast`` may be an ml.inference.fast_transform.FastIsolationForest (identical output)."""
        raw = fast.anomaly(xt) if fast is not None else -self.pipeline.named_steps["model"].score_samples(xt)
        return np.searchsorted(self.reference, raw, side="right") / len(self.reference)


def model_score(model: Any, x: pd.DataFrame) -> np.ndarray:
    if isinstance(model, AnomalyScorer):
        return model.score(x)
    return model.predict_proba(x)[:, 1]


def _environment() -> dict[str, str]:
    return {"python": platform.python_version(), "sklearn": sklearn.__version__, "xgboost": xgboost.__version__,
            "numpy": np.__version__, "pandas": pd.__version__, "platform": platform.platform()}


def train_all(features: pd.DataFrame, dataset_meta: dict[str, Any], artifacts_dir: Path,
              config: TrainingConfig | None = None) -> dict[str, Any]:
    """Train every configured model. Returns an experiment summary (also written to disk)."""
    cfg = config or TrainingConfig()
    t_start = time.perf_counter()
    started = datetime.now(timezone.utc)
    version = started.strftime("v%Y%m%dT%H%M%SZ")
    splits: Splits = temporal_split(features, train_frac=cfg.train_frac, val_frac=cfg.val_frac,
                                    holdout_customer_frac=cfg.holdout_customer_frac)
    x_tr, y_tr = xy(splits.train)
    x_va, y_va = xy(splits.validation)
    x_te, y_te = xy(splits.test)
    x_un, y_un = xy(splits.unseen_customer_test)
    if y_tr.sum() == 0 or y_va.sum() == 0:
        raise ValueError("training and validation splits must both contain positive examples")
    pos_weight = float((len(y_tr) - y_tr.sum()) / max(1, y_tr.sum()))
    split_info = {
        "strategy": "temporal (event_time ordered) + customer holdout",
        "train_end": splits.train_end.isoformat(), "validation_end": splits.validation_end.isoformat(),
        "sizes": {"train": len(x_tr), "validation": len(x_va), "test": len(x_te), "unseen_customer_test": len(x_un)},
        "positives": {"train": int(y_tr.sum()), "validation": int(y_va.sum()), "test": int(y_te.sum()),
                      "unseen_customer_test": int(y_un.sum())},
        "imbalance": {"train_neg_to_pos": round(pos_weight, 2)},
    }
    log.info("split %s", split_info)
    results: list[dict[str, Any]] = []
    for name in cfg.models:
        hp = dict(cfg.hyperparameters.get(name, {}))
        t0 = time.perf_counter()
        pipe = build_pipeline(name, hp, cfg.seed, pos_weight)
        if name == ANOMALY:
            pipe.fit(x_tr)  # unsupervised: labels are NOT used for fitting
            model: Any = AnomalyScorer(pipe, -pipe.score_samples(x_tr))
        else:
            pipe.fit(x_tr, y_tr)
            model = pipe
        train_seconds = time.perf_counter() - t0
        pipe.named_steps["model"].set_params(n_jobs=1)  # per-event inference: avoid thread-pool overhead
        s_va = model_score(model, x_va)
        thr = choose_threshold(y_va.to_numpy(), s_va, cfg.threshold_strategy, cfg.target_recall)
        val_m = classification_report(y_va.to_numpy(), s_va, thr)
        test_m = classification_report(y_te.to_numpy(), model_score(model, x_te), thr)
        unseen_m = classification_report(y_un.to_numpy(), model_score(model, x_un), thr) if len(x_un) and y_un.sum() else None
        latency = measure_latency(lambda r, m=model: model_score(m, r), x_te.reset_index(drop=True), 200)
        mdir = artifacts_dir / "models" / name / version
        mdir.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, mdir / "model.joblib", compress=3)
        ref = reference_profile(x_tr, model_score(model, x_tr.sample(min(20000, len(x_tr)), random_state=cfg.seed)))
        meta = {
            "model_name": name, "version": version, "algorithm": name,
            "kind": "anomaly" if name == ANOMALY else "supervised",
            "score_semantics": ("anomaly percentile vs training data (not a probability)" if name == ANOMALY
                                else "uncalibrated class-weighted score in [0,1] (not a calibrated probability)"),
            "trained_at": datetime.now(timezone.utc).isoformat(), "training_seconds": round(train_seconds, 3),
            "dataset": {k: dataset_meta.get(k) for k in ("name", "version", "source", "content_sha256", "rows")},
            "feature_config_version": FEATURE_CONFIG_VERSION,
            "features": {"numeric": list(NUMERIC_FEATURES), "categorical": list(CATEGORICAL_FEATURES)},
            "hyperparameters": hp, "seed": cfg.seed, "threshold": thr,
            "threshold_strategy": cfg.threshold_strategy, "split": split_info,
            "metrics": {"validation": val_m, "test": test_m, "unseen_customer_test": unseen_m},
            "inference_latency": latency, "environment": _environment(),
            "artifact_path": str(mdir.relative_to(artifacts_dir).as_posix()) + "/model.joblib",
        }
        (mdir / "metadata.json").write_text(json.dumps(meta, indent=2, default=str))
        (mdir / "reference_profile.json").write_text(json.dumps(ref))
        results.append(meta)
        log.info("trained %s val_pr_auc=%.4f test_pr_auc=%.4f", name, val_m["pr_auc"] or 0, test_m["pr_auc"] or 0)

    sup = [r for r in results if r["kind"] == "supervised"]
    best = max(sup, key=lambda r: r["metrics"]["validation"]["pr_auc"] or 0) if sup else None
    summary = {
        "experiment_version": version, "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.perf_counter() - t_start, 2),
        "dataset": dataset_meta, "split": split_info, "config": {
            "seed": cfg.seed, "threshold_strategy": cfg.threshold_strategy, "models": list(cfg.models)},
        "models": [{"model_name": r["model_name"], "version": r["version"], "metrics": r["metrics"],
                    "threshold": r["threshold"], "inference_latency": r["inference_latency"]} for r in results],
        "recommended_supervised": best["model_name"] if best else None,
        "selection_rule": "highest validation PR-AUC among supervised models (test set not used)",
    }
    exp_dir = artifacts_dir / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / f"{version}.json").write_text(json.dumps(summary, indent=2, default=str))
    _update_registry(artifacts_dir, results, best)
    return summary


def _update_registry(artifacts_dir: Path, results: list[dict[str, Any]], best: dict[str, Any] | None) -> None:
    """File registry consumed by the API on startup and by the Spark job.
    Activation of a model only happens automatically if NO model is active yet (bootstrap)."""
    path = artifacts_dir / "registry.json"
    reg = json.loads(path.read_text()) if path.exists() else {"models": [], "active": {}}
    for r in results:
        reg["models"].append({k: r[k] for k in ("model_name", "version", "kind", "artifact_path", "trained_at",
                                                "feature_config_version", "threshold")})
    if best and not reg["active"].get("supervised"):
        reg["active"]["supervised"] = {"model_name": best["model_name"], "version": best["version"]}
    an = next((r for r in results if r["kind"] == "anomaly"), None)
    if an and not reg["active"].get("anomaly"):
        reg["active"]["anomaly"] = {"model_name": an["model_name"], "version": an["version"]}
    path.write_text(json.dumps(reg, indent=2))
