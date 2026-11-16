"""The optimised inference path must be numerically identical to the sklearn pipeline."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from ml.explainability.contributions import explain
from ml.features.engineering import FeatureStore
from ml.inference.scorer import _frame, load_model, read_registry
from simulator.generator import SimulationConfig, TransactionGenerator
from streaming.schemas.events import MERCHANT_CATEGORIES

ART = Path(__file__).resolve().parents[2] / "ml" / "artifacts"
pytestmark = pytest.mark.skipif(not (ART / "registry.json").exists(), reason="models not trained")


@pytest.fixture(scope="module")
def rows():
    cfg = SimulationConfig(mode="mixed", tps=30, duration_seconds=60, customer_count=80, suspicious_pct=15, seed=21)
    store = FeatureStore()
    out = [store.process(e).features for e in TransactionGenerator(cfg, datetime(2026, 4, 1, 3, tzinfo=timezone.utc)).events()]
    # include a category value the encoder may never have seen (handle_unknown="ignore")
    out.append({**out[0], "merchant_category": "never-seen-category"})
    return out


def _all_models():
    reg = read_registry(ART)
    return [(m["model_name"], m["version"]) for m in reg["models"]]


@pytest.mark.parametrize("name,version", _all_models())
def test_fast_preprocessor_matches_column_transformer(rows, name, version):
    lm = load_model(ART, name, version)
    ref = lm.pipeline.named_steps["pre"].transform(_frame(rows))
    fast = lm.fast.transform(rows)
    assert fast.shape == ref.shape
    assert np.allclose(fast, ref, rtol=0, atol=1e-12)
    assert lm.fast.feature_names == list(lm.pipeline.named_steps["pre"].get_feature_names_out())


@pytest.mark.parametrize("name,version", _all_models())
def test_fast_scores_and_explanations_match_pipeline(rows, name, version):
    lm = load_model(ART, name, version)
    x = _frame(rows)
    xt = lm.fast.transform(rows)
    if lm.kind == "anomaly":
        sk_raw = -lm.estimator.score_samples(xt)
        assert np.allclose(lm.fast_iforest.anomaly(xt), sk_raw, rtol=0, atol=1e-12)
        assert np.array_equal(lm.model.score(x), lm.model.score_transformed(xt, lm.fast_iforest))
        return
    assert np.allclose(lm.model.predict_proba(x)[:, 1], lm.estimator.predict_proba(xt)[:, 1], atol=1e-12)
    for i in (0, len(rows) // 2, len(rows) - 1):
        a = explain(lm.model, x.iloc[[i]], name)
        b = explain(lm.model, None, name, xt=xt[i], names=lm.fast.feature_names)
        assert a["contributions"].keys() == b["contributions"].keys()
        assert all(a["contributions"][k] == pytest.approx(b["contributions"][k], abs=1e-9) for k in a["contributions"])


def test_categories_known_to_encoder_are_valid_schema_values():
    lm = load_model(ART, *next((n, v) for n, v in _all_models() if n == "xgboost"))
    assert set(lm.fast.lookups[0]) <= set(MERCHANT_CATEGORIES)
