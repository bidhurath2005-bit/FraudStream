"""Model loading and inference shared by the API (local profile), the Kafka consumer and
the Spark job. Raises ModelUnavailableError instead of returning fabricated scores."""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

from ml.explainability.contributions import explain, top_contributions
from ml.inference.fast_transform import FastIsolationForest, FastPreprocessor
from ml.features.definitions import ALL_FEATURES, CATEGORICAL_FEATURES, FEATURE_CONFIG_VERSION, NUMERIC_FEATURES
from ml.training.pipeline import AnomalyScorer


class ModelUnavailableError(RuntimeError):
    pass


@dataclass
class LoadedModel:
    model_name: str
    version: str
    kind: str
    threshold: float
    model: Any
    metadata: dict[str, Any]
    reference_profile: dict[str, Any] | None = None
    fast: FastPreprocessor | None = None
    fast_iforest: FastIsolationForest | None = None

    @property
    def pipeline(self) -> Any:
        return self.model.pipeline if isinstance(self.model, AnomalyScorer) else self.model

    @property
    def estimator(self) -> Any:
        return self.pipeline.named_steps["model"]

    @property
    def label(self) -> str:
        return f"{self.model_name}:{self.version}"


@dataclass
class ScoreResult:
    model_score: float
    model_threshold: float
    model_version: str
    anomaly_score: float | None
    anomaly_version: str | None
    inference_ms: float
    explanation: dict[str, Any] | None = None
    top_contributions: list[dict[str, float]] = field(default_factory=list)


def load_model(artifacts_dir: Path, model_name: str, version: str) -> LoadedModel:
    mdir = artifacts_dir / "models" / model_name / version
    meta_path, art = mdir / "metadata.json", mdir / "model.joblib"
    if not art.exists() or not meta_path.exists():
        raise ModelUnavailableError(f"artifact missing for {model_name}:{version} under {mdir}")
    meta = json.loads(meta_path.read_text())
    if meta.get("feature_config_version") != FEATURE_CONFIG_VERSION:
        raise ModelUnavailableError(
            f"{model_name}:{version} uses feature config {meta.get('feature_config_version')}, "
            f"runtime is {FEATURE_CONFIG_VERSION}")
    ref_path = mdir / "reference_profile.json"
    lm = LoadedModel(model_name, version, meta["kind"], float(meta["threshold"]), joblib.load(art), meta,
                     json.loads(ref_path.read_text()) if ref_path.exists() else None)
    lm.fast = FastPreprocessor(lm.pipeline.named_steps["pre"])
    est = lm.estimator
    if lm.kind == "anomaly":
        lm.fast_iforest = FastIsolationForest(est)
    elif hasattr(est, "get_booster"):
        est.set_params(n_jobs=1)            # single-row inference: avoid thread-pool start-up cost
        est.get_booster().set_param({"nthread": 1})
    return lm


def read_registry(artifacts_dir: Path) -> dict[str, Any]:
    p = artifacts_dir / "registry.json"
    if not p.exists():
        raise ModelUnavailableError(f"no model registry at {p}; run scripts/train_models.ps1 first")
    return json.loads(p.read_text())


def _frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=list(ALL_FEATURES))
    for c in NUMERIC_FEATURES:
        df[c] = df[c].astype(float)
    for c in CATEGORICAL_FEATURES:
        df[c] = df[c].astype(str)
    return df


class Scorer:
    """Holds the active supervised + anomaly models; swap is atomic under a lock."""

    def __init__(self, artifacts_dir: Path) -> None:
        self.artifacts_dir = artifacts_dir
        self._lock = threading.RLock()
        self.supervised: LoadedModel | None = None
        self.anomaly: LoadedModel | None = None

    def load(self, supervised: tuple[str, str] | None, anomaly: tuple[str, str] | None) -> None:
        sup = load_model(self.artifacts_dir, *supervised) if supervised else None
        an = load_model(self.artifacts_dir, *anomaly) if anomaly else None
        if sup is not None and sup.kind != "supervised":
            raise ModelUnavailableError(f"{sup.label} is not a supervised model")
        with self._lock:
            self.supervised, self.anomaly = sup, an

    def load_from_registry(self) -> None:
        act = read_registry(self.artifacts_dir).get("active", {})
        s, a = act.get("supervised"), act.get("anomaly")
        self.load((s["model_name"], s["version"]) if s else None, (a["model_name"], a["version"]) if a else None)

    @property
    def ready(self) -> bool:
        return self.supervised is not None

    def score_many(self, rows: list[dict[str, Any]], explain_rows: bool = True) -> list[ScoreResult]:
        with self._lock:
            sup, an = self.supervised, self.anomaly
        if sup is None:
            raise ModelUnavailableError("no active supervised model loaded")
        if not rows:
            return []
        t0 = time.perf_counter()
        # Fast path: fitted preprocessing applied once with NumPy (identical output, see fast_transform.py)
        xs = sup.fast.transform(rows) if sup.fast else sup.pipeline.named_steps["pre"].transform(_frame(rows))
        s = sup.estimator.predict_proba(xs)[:, 1]
        a = None
        if an is not None and isinstance(an.model, AnomalyScorer):
            xa = an.fast.transform(rows) if an.fast else an.pipeline.named_steps["pre"].transform(_frame(rows))
            a = an.model.score_transformed(xa, an.fast_iforest if len(rows) <= 32 else None)  # vectorised sklearn for large batches
        per_row_ms = (time.perf_counter() - t0) * 1000 / len(rows)
        out = []
        for i in range(len(rows)):
            expl = (explain(sup.model, None, sup.model_name, xt=xs[i], names=sup.fast.feature_names if sup.fast else None)
                    if explain_rows else None)
            out.append(ScoreResult(
                model_score=float(s[i]), model_threshold=sup.threshold, model_version=sup.label,
                anomaly_score=float(a[i]) if a is not None else None,
                anomaly_version=an.label if an is not None else None, inference_ms=per_row_ms,
                explanation=expl, top_contributions=top_contributions(expl)))
        return out

    def score(self, features: dict[str, Any], explain_row: bool = True) -> ScoreResult:
        return self.score_many([features], explain_row)[0]
