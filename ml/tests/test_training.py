"""ML pipeline tests. Training runs on a SMALL in-memory synthetic dataset written to
pytest's tmp_path; the project's data/ and ml/artifacts/ are only ever READ."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ml.datasets.public import (
    DatasetValidationError,
    creditcard_features,
    ieee_to_events,
    paysim_to_events,
    validate_frame,
)
from ml.drift.psi import compute_drift, psi, reference_profile
from ml.evaluation.metrics import choose_threshold, classification_report
from ml.features.definitions import ALL_FEATURES, FEATURE_CONFIG_VERSION
from ml.features.engineering import FeatureStore, build_feature_frame
from ml.inference.scorer import ModelUnavailableError, Scorer, load_model, read_registry
from ml.preprocessing.splits import temporal_split, xy
from ml.training.pipeline import TrainingConfig, model_score, train_all
from simulator.generator import SimulationConfig, TransactionGenerator

ROOT = Path(__file__).resolve().parents[2]
REAL_ARTIFACTS = ROOT / "ml" / "artifacts"
T0 = datetime(2026, 2, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def small_frame() -> pd.DataFrame:
    cfg = SimulationConfig(tps=1.0, duration_seconds=4 * 3600, customer_count=400, suspicious_pct=4, seed=99)
    return build_feature_frame(TransactionGenerator(cfg, T0).events())


@pytest.fixture(scope="module")
def trained(small_frame, tmp_path_factory):
    out = tmp_path_factory.mktemp("artifacts")
    cfg = TrainingConfig()
    cfg.hyperparameters["random_forest"]["n_estimators"] = 40
    cfg.hyperparameters["xgboost"]["n_estimators"] = 80
    summary = train_all(small_frame, {"name": "test-synth", "version": "t"}, out, cfg)
    return out, summary


# ------------------------------------------------------------------ leakage-resistant splits
def test_temporal_split_has_no_time_overlap(small_frame):
    s = temporal_split(small_frame)
    assert s.train["meta_event_time"].max() < s.validation["meta_event_time"].min()
    assert s.validation["meta_event_time"].max() < s.test["meta_event_time"].min()
    assert s.test["meta_event_time"].min() > s.validation_end


def test_unseen_customer_split_is_disjoint_from_training(small_frame):
    s = temporal_split(small_frame)
    train_c = set(s.train["meta_customer_id"]) | set(s.validation["meta_customer_id"]) | set(s.test["meta_customer_id"])
    unseen_c = set(s.unseen_customer_test["meta_customer_id"])
    assert unseen_c and not unseen_c & train_c
    assert s.unseen_customer_test["meta_event_time"].min() > s.validation_end


def test_split_rows_are_not_duplicated(small_frame):
    s = temporal_split(small_frame)
    used = pd.concat([s.train, s.validation, s.test, s.unseen_customer_test])
    assert used["meta_event_id"].is_unique
    # Rows not used anywhere must be exactly the held-out customers' events BEFORE the test
    # period: they are excluded on purpose (training on them would leak those customers).
    dropped = small_frame[~small_frame["meta_event_id"].isin(used["meta_event_id"])]
    held = set(s.unseen_customer_test["meta_customer_id"])
    assert len(dropped) > 0
    assert set(dropped["meta_customer_id"]) <= held | set(dropped["meta_customer_id"]) - set(used["meta_customer_id"])
    assert not set(dropped["meta_customer_id"]) & set(s.train["meta_customer_id"])
    assert (dropped["meta_event_time"] <= s.validation_end).all()


def test_model_inputs_exclude_labels_and_identifiers(small_frame):
    x, y = xy(small_frame)
    assert list(x.columns) == list(ALL_FEATURES)
    assert not {"meta_label", "meta_scenario", "meta_event_id", "meta_customer_id"} & set(x.columns)


# ------------------------------------------------------------------ training
def test_training_produces_all_models_and_metrics(trained):
    out, summary = trained
    names = {m["model_name"] for m in summary["models"]}
    assert names == {"logistic_regression", "random_forest", "xgboost", "isolation_forest"}
    for m in summary["models"]:
        t = m["metrics"]["test"]
        for k in ("precision", "recall", "f1", "roc_auc", "pr_auc", "false_positive_rate", "false_negative_rate",
                  "confusion_matrix"):
            assert k in t
        assert m["inference_latency"]["p50_ms"] > 0
    assert summary["selection_rule"].startswith("highest validation PR-AUC")
    reg = read_registry(out)
    assert reg["active"]["supervised"]["model_name"] == summary["recommended_supervised"]


def test_threshold_is_chosen_on_validation_not_test(trained, small_frame):
    out, summary = trained
    s = temporal_split(small_frame)
    xv, yv = xy(s.validation)
    for m in summary["models"]:
        lm = load_model(out, m["model_name"], m["version"])
        expected = choose_threshold(yv.to_numpy(), model_score(lm.model, xv))
        assert lm.threshold == pytest.approx(expected)


def test_serialization_roundtrip_gives_identical_predictions(trained, small_frame):
    out, summary = trained
    x, _ = xy(small_frame.head(200))
    for m in summary["models"]:
        lm = load_model(out, m["model_name"], m["version"])
        a = model_score(lm.model, x)
        b = model_score(load_model(out, m["model_name"], m["version"]).model, x)
        assert np.allclose(a, b)
        assert ((a >= 0) & (a <= 1)).all()


def test_training_is_reproducible(small_frame, tmp_path):
    cfg = TrainingConfig(models=("logistic_regression", "xgboost"))
    cfg.hyperparameters["xgboost"]["n_estimators"] = 50
    a = train_all(small_frame, {"name": "r"}, tmp_path / "a", cfg)
    b = train_all(small_frame, {"name": "r"}, tmp_path / "b", cfg)
    for ma, mb in zip(a["models"], b["models"], strict=True):
        for k in ("pr_auc", "precision", "recall", "confusion_matrix"):
            assert ma["metrics"]["test"][k] == mb["metrics"]["test"][k]


def test_class_imbalance_handling_is_configured(trained):
    out, summary = trained
    xgb = load_model(out, "xgboost", summary["experiment_version"]).model.named_steps["model"]
    assert xgb.get_params()["scale_pos_weight"] > 5
    lr = load_model(out, "logistic_regression", summary["experiment_version"]).model.named_steps["model"]
    assert lr.get_params()["class_weight"] == "balanced"


def test_isolation_forest_is_fit_without_labels(trained):
    out, summary = trained
    meta = json.loads((out / "models" / "isolation_forest" / summary["experiment_version"] / "metadata.json").read_text())
    assert meta["kind"] == "anomaly" and "not a probability" in meta["score_semantics"]


def test_feature_config_mismatch_is_rejected(trained):
    out, summary = trained
    p = out / "models" / "xgboost" / summary["experiment_version"] / "metadata.json"
    meta = json.loads(p.read_text())
    meta["feature_config_version"] = "fc-0.0"
    p2 = out / "models" / "xgboost" / "vbad"
    p2.mkdir()
    (p2 / "metadata.json").write_text(json.dumps(meta))
    (p2 / "model.joblib").write_bytes((p.parent / "model.joblib").read_bytes())
    with pytest.raises(ModelUnavailableError):
        load_model(out, "xgboost", "vbad")


def test_scorer_without_model_raises_instead_of_fabricating(tmp_path):
    with pytest.raises(ModelUnavailableError):
        Scorer(tmp_path).score({})
    with pytest.raises(ModelUnavailableError):
        Scorer(tmp_path).load_from_registry()


def test_online_features_match_training_features_and_score(trained, small_frame):
    """The online FeatureStore and the offline frame builder must produce identical vectors."""
    out, _ = trained
    cfg = SimulationConfig(tps=1.0, duration_seconds=4 * 3600, customer_count=400, suspicious_pct=4, seed=99)
    events = list(TransactionGenerator(cfg, T0).events())[:500]
    store = FeatureStore()
    offline = small_frame.set_index("meta_event_id")
    sc = Scorer(out)
    sc.load_from_registry()
    for e in events:
        f = store.process(e).features
        row = offline.loc[e.event_id]
        for c in ALL_FEATURES:
            assert row[c] == pytest.approx(f[c]) if isinstance(f[c], float) else row[c] == f[c], c
    r = sc.score(f)
    assert 0 <= r.model_score <= 1 and r.model_version.startswith(sc.supervised.model_name)
    assert r.anomaly_score is not None and 0 <= r.anomaly_score <= 1


# ------------------------------------------------------------------ saved project artifacts (read-only)
@pytest.mark.skipif(not (REAL_ARTIFACTS / "registry.json").exists(), reason="project models not trained yet")
def test_saved_project_artifacts_load_and_score_without_modification():
    files = sorted(p for p in REAL_ARTIFACTS.rglob("*") if p.is_file())
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    sc = Scorer(REAL_ARTIFACTS)
    sc.load_from_registry()
    assert sc.supervised.metadata["feature_config_version"] == FEATURE_CONFIG_VERSION
    cfg = SimulationConfig(mode="fraud", tps=20, duration_seconds=120, customer_count=50, seed=4)
    store = FeatureStore()
    results = []
    for e in TransactionGenerator(cfg, T0).events():
        f = store.process(e).features
        results.append((sc.score(f, explain_row=False).model_score, e.ground_truth.is_suspicious))
    sus = [s for s, lbl in results if lbl]
    norm = [s for s, lbl in results if not lbl]
    assert sus and norm and np.mean(sus) > np.mean(norm)
    r = sc.score(f)
    assert r.explanation and r.explanation["method"] in ("xgboost_treeshap", "saabas_path_attribution", "linear_coef_x_value")
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    assert before == after, "artifacts must not be modified by inference"


def test_xgboost_treeshap_contributions_sum_to_margin(trained, small_frame):
    out, summary = trained
    lm = load_model(out, "xgboost", summary["experiment_version"])
    x, _ = xy(small_frame.head(5))
    from ml.explainability.contributions import explain

    for i in range(5):
        e = explain(lm.model, x.iloc[[i]], "xgboost")
        margin = lm.model.named_steps["model"].predict(
            lm.model.named_steps["pre"].transform(x.iloc[[i]]), output_margin=True)[0]
        assert sum(e["contributions"].values()) + e["base_value"] == pytest.approx(float(margin), abs=1e-3)
        assert set(e["contributions"]) <= set(ALL_FEATURES)


# ------------------------------------------------------------------ metrics, drift, datasets
def test_classification_report_values():
    y = np.array([0, 0, 0, 1, 1])
    s = np.array([0.1, 0.6, 0.2, 0.9, 0.4])
    r = classification_report(y, s, 0.5)
    assert r["confusion_matrix"] == {"tn": 2, "fp": 1, "fn": 1, "tp": 1}
    assert r["precision"] == 0.5 and r["recall"] == 0.5 and r["false_positive_rate"] == pytest.approx(1 / 3)
    assert "accuracy" not in r


def test_psi_detects_shift_and_ignores_identical(small_frame):
    x, _ = xy(small_frame)
    ref = reference_profile(x)
    same = compute_drift(ref, x, min_rows=10)
    assert same["status"] == "STABLE" and same["max_psi"] < 0.01
    shifted = x.copy()
    shifted["amount"] = shifted["amount"] * 6
    shifted["log_amount"] = np.log1p(shifted["amount"])
    d = compute_drift(ref, shifted, min_rows=10)
    assert d["features"]["amount"]["status"] == "SIGNIFICANT" and "amount" in d["drifted_features"]
    assert compute_drift(ref, x.head(5))["status"] == "INSUFFICIENT_DATA"
    assert psi(np.array([0.5, 0.5]), np.array([0.5, 0.5])) == pytest.approx(0)


def test_public_dataset_adapters_validate_and_map():
    paysim = pd.DataFrame({"step": [1, 1, 2], "type": ["PAYMENT", "TRANSFER", "CASH_OUT"], "amount": [10.0, 900, 900],
                           "nameOrig": ["C1", "C1", "C2"], "nameDest": ["M1", "C9", "C8"], "isFraud": [0, 1, 1],
                           "oldbalanceOrg": [1, 2, 3], "newbalanceOrig": [0, 0, 0]})
    prof = validate_frame(paysim, "paysim")
    assert prof["positives"] == 2
    evs = paysim_to_events(paysim)
    assert all("oldbalanceOrg" not in e and "newbalanceOrig" not in e for e in evs)  # post-event leakage dropped
    frame = build_feature_frame(evs)
    assert len(frame) == 3 and frame["meta_label"].tolist() == [0, 1, 1]
    ieee = pd.DataFrame({"TransactionID": [1, 2], "isFraud": [0, 1], "TransactionDT": [86400, 86500],
                         "TransactionAmt": [20.0, 300.0], "ProductCD": ["W", "C"], "card1": [111, 111]})
    validate_frame(ieee, "ieee")
    assert len(ieee_to_events(ieee)) == 2
    cc = pd.DataFrame({"Time": [0, 1], "Amount": [5.0, 7.0], "Class": [0, 1], **{f"V{i}": [0.1, 0.2] for i in range(1, 29)}})
    x, y, _ = creditcard_features(cc)
    assert "Class" not in x.columns and y.tolist() == [0, 1]
    with pytest.raises(DatasetValidationError):
        validate_frame(paysim.drop(columns=["isFraud"]), "paysim")
    with pytest.raises(DatasetValidationError):
        validate_frame(paysim.assign(isFraud=[0, 2, 1]), "paysim")
