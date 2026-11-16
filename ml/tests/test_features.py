from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone

import pytest

from ml.features.definitions import ALL_FEATURES, FORBIDDEN_FEATURE_FIELDS, LATENESS_TOLERANCE_SECONDS
from ml.features.engineering import (
    CustomerState,
    FeatureStore,
    FeatureValidationError,
    build_feature_frame,
    classify_arrival,
    compute_features,
    update_state,
    validate_features,
)
from simulator.generator import SimulationConfig, TransactionGenerator

T0 = datetime(2026, 3, 2, 10, 0, tzinfo=timezone.utc)


def ev(i, sec, amount=50.0, *, cid="C1", device="d1", city=("London", "GB", 51.5, -0.13), status="APPROVED",
       merchant="M1", category="grocery", label=False):
    return {"event_id": f"e{i:08d}", "transaction_id": f"T{i}", "customer_id": cid, "event_time": T0 + timedelta(seconds=sec),
            "amount": amount, "status": status, "device_id": device, "merchant_id": merchant,
            "merchant_category": category, "channel": "ONLINE", "transaction_type": "PURCHASE",
            "location": {"city": city[0], "country": city[1], "lat": city[2], "lon": city[3]},
            "ground_truth": {"is_suspicious": label, "scenario": "x"}}


def test_first_event_has_neutral_imputed_history():
    f = compute_features(ev(0, 0), CustomerState())
    validate_features(f)
    assert f["has_history"] == 0 and f["hist_tx_count"] == 0 and f["tx_count_1m"] == 0
    assert f["is_new_device"] == 0 and f["amount_to_mean_ratio"] == 1.0 and f["amount_zscore"] == 0.0
    assert f["seconds_since_prev"] == 7 * 24 * 3600


def test_velocity_windows_exclude_current_and_respect_bounds():
    st = CustomerState()
    for i, s in enumerate([0, 100, 250, 280, 290]):
        update_state(st, ev(i, s))
    f = compute_features(ev(9, 300), st)
    assert f["tx_count_1m"] == 3          # 250, 280, 290 (>= t-60)
    assert f["tx_count_5m"] == 5          # all within 300 s (>= t-300)
    assert f["tx_count_1h"] == 5
    assert f["seconds_since_prev"] == pytest.approx(10)
    assert f["spend_sum_15m"] == pytest.approx(250)


def test_failed_transactions_counted_and_excluded_from_spend():
    st = CustomerState()
    update_state(st, ev(0, 0, 100))
    for i in range(1, 4):
        update_state(st, ev(i, i * 10, 999, status="DECLINED"))
    f = compute_features(ev(10, 60, 40), st)
    assert f["failed_count_1h"] == 3 and f["spend_sum_1h"] == pytest.approx(100)
    assert f["hist_mean_amount"] == pytest.approx(100)  # failed amounts excluded
    assert f["failed_ratio_hist"] == pytest.approx(0.75)


def test_new_device_location_merchant_and_geo_velocity():
    st = CustomerState()
    update_state(st, ev(0, 0))
    f = compute_features(ev(1, 600, device="d2", city=("Tokyo", "JP", 35.68, 139.65), merchant="M9"), st)
    assert f["is_new_device"] == 1 and f["is_new_location"] == 1 and f["is_new_merchant"] == 1
    assert f["geo_velocity_kmh"] > 900  # ~9,560 km in 10 minutes


def test_amount_deviation():
    st = CustomerState()
    for i, a in enumerate([40, 50, 60, 45, 55, 50]):
        update_state(st, ev(i, i * 3600, a))
    f = compute_features(ev(99, 7 * 3600, 900), st)
    assert f["amount_zscore"] > 10 and f["amount_to_mean_ratio"] == pytest.approx(900 / 50)


def test_point_in_time_correctness_future_events_do_not_change_past_features():
    cfg = SimulationConfig(tps=30, duration_seconds=120, customer_count=60, suspicious_pct=10, seed=5)
    events = list(TransactionGenerator(cfg, T0).events())
    half = len(events) // 2
    prefix = build_feature_frame(events[:half])
    full = build_feature_frame(events)
    cols = list(ALL_FEATURES)
    merged = prefix.merge(full, on="meta_event_id", suffixes=("_p", "_f"))
    assert len(merged) == len(prefix)
    for c in cols:
        assert (merged[f"{c}_p"] == merged[f"{c}_f"]).all(), c


def test_ground_truth_label_never_influences_features():
    st1, st2 = CustomerState(), CustomerState()
    a, b = ev(0, 0, label=False), ev(0, 0, label=True)
    b["ground_truth"]["scenario"] = "account_takeover"
    assert compute_features(a, st1) == compute_features(b, st2)
    feats = compute_features(a, st1)
    assert not FORBIDDEN_FEATURE_FIELDS & set(feats)
    assert set(feats) == set(ALL_FEATURES)


def test_feature_frame_keeps_labels_only_in_meta_columns():
    cfg = SimulationConfig(tps=20, duration_seconds=60, customer_count=40, suspicious_pct=20, seed=8)
    df = build_feature_frame(TransactionGenerator(cfg, T0).events())
    feature_cols = [c for c in df.columns if not c.startswith("meta_")]
    assert sorted(feature_cols) == sorted(ALL_FEATURES)
    assert df["meta_label"].sum() > 0


def test_validate_rejects_leakage_nan_and_out_of_range():
    f = compute_features(ev(0, 0), CustomerState())
    for bad in ({"is_suspicious": 1}, {"amount": float("nan")}, {"is_night": 3}, {"channel": ""}):
        with pytest.raises(FeatureValidationError):
            validate_features({**f, **bad})
    g = dict(f)
    del g["amount"]
    with pytest.raises(FeatureValidationError):
        validate_features(g)


def test_duplicate_and_late_classification():
    st = CustomerState()
    update_state(st, ev(0, 10_000))
    assert classify_arrival(ev(0, 10_000), st) == "duplicate"
    assert classify_arrival(ev(1, 10_000 - LATENESS_TOLERANCE_SECONDS - 1), st) == "late"
    assert classify_arrival(ev(2, 10_000 - 60), st) == "ok"   # out-of-order within tolerance


def test_out_of_order_event_only_sees_earlier_events_in_windows():
    st = CustomerState()
    update_state(st, ev(0, 0))
    update_state(st, ev(1, 100))
    f = compute_features(ev(2, 50), st)   # arrives after ev1 but happened before it
    assert f["tx_count_1m"] == 1 and f["seconds_since_prev"] == pytest.approx(50)


def test_feature_store_process_without_apply_does_not_mutate_and_apply_dedups():
    store = FeatureStore()
    e = ev(0, 0)
    assert store.process(e, apply=False).status == "ok"
    assert store.process(e, apply=False).status == "ok"
    store.apply(e)
    store.apply(copy.deepcopy(e))
    assert store.process(e).status == "duplicate"
    assert store.snapshot("C1")["total_count"] == 1


def test_warm_start_rebuilds_state_equivalently():
    events = [ev(i, i * 30, 20 + i) for i in range(20)]
    a, b = FeatureStore(), FeatureStore()
    for e in events:
        a.process(e)
    assert b.warm(reversed(events)) == 20
    nxt = ev(100, 20 * 30 + 5, 75)
    assert a.process(nxt, apply=False).features == b.process(nxt, apply=False).features
