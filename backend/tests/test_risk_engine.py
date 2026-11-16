from __future__ import annotations

import pytest

from app.risk_engine.engine import DEFAULT_RULES, RiskEngine, RiskSettings, RuleConfig, engine_from_config, normalise_model_score
from ml.features.engineering import CustomerState, compute_features

BASE = compute_features({
    "event_id": "e0000000", "customer_id": "C", "event_time": "2026-01-01T12:00:00+00:00", "amount": 50.0,
    "status": "APPROVED", "device_id": "d", "merchant_id": "M", "merchant_category": "grocery", "channel": "ONLINE",
    "transaction_type": "PURCHASE", "location": {"city": "X", "country": "GB", "lat": 0.0, "lon": 0.0}}, CustomerState())


def f(**kw):
    return {**BASE, **kw}


def ev(engine, feats, score=None, thr=0.5, anomaly=None):
    return engine.evaluate(feats, model_score=score, model_threshold=thr if score is not None else None,
                           model_version="m:v" if score is not None else None, anomaly_score=anomaly)


def test_normal_transaction_low_risk_allow():
    d = ev(RiskEngine(), f(), score=0.05)
    assert d.risk_category == "LOW" and d.recommended_action == "ALLOW" and d.triggered_rules == []
    assert d.detection_method == "NONE"


def test_model_threshold_maps_to_medium_boundary():
    assert normalise_model_score(0.8, 0.8) == pytest.approx(0.5)
    assert normalise_model_score(1.0, 0.8) == pytest.approx(1.0)
    d = ev(RiskEngine(), f(), score=0.8, thr=0.8)
    assert d.risk_score == pytest.approx(45.0) and d.risk_category == "MEDIUM" and d.detection_method == "ML"


def test_high_model_score_is_high_and_block_simulation():
    d = ev(RiskEngine(), f(), score=0.99, thr=0.5)
    assert d.risk_category == "HIGH" and d.recommended_action == "BLOCK_SIMULATION"


def test_block_simulation_can_be_disabled():
    d = ev(RiskEngine(settings=RiskSettings(block_simulation_enabled=False)), f(), score=0.99, thr=0.5)
    assert d.risk_category == "HIGH" and d.recommended_action == "REVIEW"


@pytest.mark.parametrize("feats,code", [
    (dict(amount=6000.0), "LARGE_AMOUNT"),
    (dict(hist_tx_count=10, amount_to_mean_ratio=9.0), "LARGE_AMOUNT"),
    (dict(hist_tx_count=10, amount_zscore=5.0), "AMOUNT_DEVIATION"),
    (dict(tx_count_1m=5), "HIGH_VELOCITY_1M"),
    (dict(tx_count_5m=9), "HIGH_VELOCITY_5M"),
    (dict(is_new_device=1, amount_to_mean_ratio=4.0), "NEW_DEVICE_HIGH_SPEND"),
    (dict(geo_velocity_kmh=5000.0), "IMPOSSIBLE_TRAVEL"),
    (dict(is_new_location=1), "NEW_LOCATION"),
    (dict(failed_count_1h=4), "REPEATED_FAILURES"),
    (dict(is_night=1, has_history=1, hour_of_day=3, amount_to_mean_ratio=4.0), "ODD_HOUR_HIGH_SPEND"),
    (dict(is_high_risk_category=1, is_new_merchant=1, merchant_category="crypto_exchange"), "HIGH_RISK_MERCHANT_NEW"),
])
def test_each_rule_triggers(feats, code):
    d = ev(RiskEngine(), f(**feats))
    assert code in [r["code"] for r in d.triggered_rules]
    assert d.detection_method == "RULES" and d.model_score is None
    assert "rules only" in d.explanation.lower()


def test_correlated_rules_in_same_family_are_not_double_counted():
    eng = RiskEngine()
    both = ev(eng, f(tx_count_1m=6, tx_count_5m=12))            # velocity family: two rules
    one = ev(eng, f(tx_count_1m=6))
    assert {r["code"] for r in both.triggered_rules} == {"HIGH_VELOCITY_1M", "HIGH_VELOCITY_5M"}
    assert both.risk_score == one.risk_score == pytest.approx(35.0)
    cross = ev(eng, f(tx_count_1m=6, failed_count_1h=4))         # two different families combine
    assert cross.risk_score == pytest.approx(100 * (1 - 0.65 * 0.65))


def test_disabled_rule_and_custom_params():
    rules = [RuleConfig(**{**r.__dict__, "enabled": r.code != "HIGH_VELOCITY_1M"}) for r in DEFAULT_RULES]
    assert ev(RiskEngine(rules), f(tx_count_1m=6)).triggered_rules == []
    rules = [RuleConfig(**{**r.__dict__, "params": {"count": 10}}) if r.code == "HIGH_VELOCITY_1M" else r for r in DEFAULT_RULES]
    assert ev(RiskEngine(rules), f(tx_count_1m=6)).triggered_rules == []


def test_anomaly_only_counts_above_percentile_and_hybrid_method():
    eng = RiskEngine()
    assert "anomaly" not in ev(eng, f(), score=0.1, anomaly=0.9).components
    d = ev(eng, f(tx_count_1m=6), score=0.9, thr=0.5, anomaly=0.999)
    assert "anomaly" in d.components and d.detection_method == "HYBRID"


def test_decision_contains_required_fields():
    d = ev(RiskEngine(), f(amount=7000.0), score=0.7).to_dict()
    for k in ("risk_score", "risk_category", "triggered_rules", "relevant_features", "model_version", "predicted_at",
              "recommended_action", "explanation", "engine_version"):
        assert k in d
    assert "not a probability" in d["explanation"]


def test_invalid_settings_rejected():
    with pytest.raises(ValueError):
        RiskEngine(settings=RiskSettings(medium_threshold=80, high_threshold=70))
    with pytest.raises(ValueError):
        RiskEngine([RuleConfig("UNKNOWN", "x", "x", "x", 0.1)])


def test_engine_from_runtime_config_roundtrip():
    eng = engine_from_config({"rules": [r.__dict__ for r in DEFAULT_RULES], "settings": {"high_threshold": 90.0}})
    assert eng.settings.high_threshold == 90.0 and len(eng.rules) == len(DEFAULT_RULES)
    assert engine_from_config(None).settings.high_threshold == 70.0
