"""Model registry API, risk configuration, simulation control, analytics and reports."""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from sqlalchemy import func, select

from app.database.session import SessionLocal
from app.models.entities import Transaction

from app.core.runtime import get_runtime

API = "/api/v1"


def test_models_listed_with_real_metrics_and_active(client, analyst_h):
    models = client.get(f"{API}/models", headers=analyst_h).json()
    assert {m["model_name"] for m in models} >= {"logistic_regression", "random_forest", "xgboost", "isolation_forest"}
    for m in models:
        assert m["metrics"]["test"]["pr_auc"] is not None and m["dataset"]["name"].startswith("synthetic")
    act = client.get(f"{API}/models/active", headers=analyst_h).json()
    assert act["active"]["supervised"]["status"] == "ACTIVE" and act["model_error"] is None
    assert act["loaded_in_api"]["supervised"] == f"{act['active']['supervised']['model_name']}:{act['active']['supervised']['version']}"
    comp = client.get(f"{API}/models/compare", headers=analyst_h).json()
    assert len(comp["models"]) == 4 and "Synthetic" in comp["evaluation_note"]
    assert client.get(f"{API}/models/experiments", headers=analyst_h).json()[0]["recommended"]


def test_controlled_activation_and_rollback(client, admin_h, analyst_h):
    models = client.get(f"{API}/models?kind=supervised", headers=admin_h).json()
    current = next(m for m in models if m["status"] == "ACTIVE")
    other = next(m for m in models if m["status"] != "ACTIVE")
    assert client.post(f"{API}/models/{other['id']}/activate", headers=admin_h, json={"reason": "x"}).status_code == 422
    r = client.post(f"{API}/models/{other['id']}/activate", headers=admin_h, json={"reason": "compare in shadow"})
    assert r.status_code == 200 and r.json()["previous"]["id"] == current["id"]
    assert get_runtime().scorer.supervised.model_name == other["model_name"]
    statuses = {m["id"]: m["status"] for m in client.get(f"{API}/models?kind=supervised", headers=admin_h).json()}
    assert statuses[other["id"]] == "ACTIVE" and statuses[current["id"]] == "CANDIDATE"   # kept for rollback
    rb = client.post(f"{API}/models/rollback", headers=admin_h, json={"kind": "supervised", "reason": "restore best"})
    assert rb.status_code == 200 and rb.json()["activated"]["id"] == current["id"]
    assert get_runtime().scorer.supervised.model_name == current["model_name"]
    # registry file in the (temp copy of the) artifacts dir reflects the active model for Spark
    reg = json.loads((get_runtime().settings.artifacts_dir / "registry.json").read_text())
    assert reg["active"]["supervised"]["model_name"] == current["model_name"]
    audit = client.get(f"{API}/audit?entity_type=model_version", headers=admin_h).json()["items"]
    assert {"MODEL_ACTIVATED", "MODEL_DEACTIVATED"} <= {a["action"] for a in audit}


def _rule_ver(client, h, code):
    # Phase 12: configuration updates must send the version they were based on (expected_updated_at)
    return next(x for x in client.get(f"{API}/risk/rules", headers=h).json() if x["code"] == code)["updated_at"]


def _ver(client, h, path):
    return client.get(f"{API}{path}", headers=h).json()["updated_at"]


def test_risk_rule_and_settings_updates_take_effect(client, admin_h, analyst_h):
    r = client.patch(f"{API}/risk/rules/HIGH_VELOCITY_1M", headers=admin_h,
                     json={"params": {"count": 6}, "expected_updated_at": _rule_ver(client, admin_h, "HIGH_VELOCITY_1M")})
    assert r.status_code == 200 and r.json()["params"]["count"] == 6
    rule = next(x for x in get_runtime().engine.rules if x.code == "HIGH_VELOCITY_1M")
    assert rule.params["count"] == 6
    assert client.patch(f"{API}/risk/rules/HIGH_VELOCITY_1M", headers=admin_h, json={"params": {"bogus": 1}}).status_code == 422
    client.patch(f"{API}/risk/rules/HIGH_VELOCITY_1M", headers=admin_h,
                 json={"params": {"count": 4}, "expected_updated_at": _rule_ver(client, admin_h, "HIGH_VELOCITY_1M")})
    s = client.get(f"{API}/risk/settings", headers=analyst_h).json()["settings"]
    bad = {**s, "medium_threshold": 80, "high_threshold": 70}
    assert client.put(f"{API}/risk/settings", headers=admin_h, json=bad).status_code == 422
    ok = client.put(f"{API}/risk/settings", headers=admin_h,
                    json={**s, "high_threshold": 75.0, "expected_updated_at": _ver(client, admin_h, "/risk/settings")})
    assert ok.status_code == 200 and get_runtime().engine.settings.high_threshold == 75.0
    client.put(f"{API}/risk/settings", headers=admin_h, json={**s, "expected_updated_at": _ver(client, admin_h, "/risk/settings")})
    assert client.put(f"{API}/settings/drift", headers=admin_h, json={"value": {"moderate": 0.3, "significant": 0.2}}).status_code == 422
    assert client.put(f"{API}/settings/drift", headers=admin_h,
                      json={"value": {"window_rows": 1500},
                            "expected_updated_at": _ver(client, admin_h, "/settings/drift")}).status_code == 200


def _wait(client, h, pred, timeout=30.0):
    end = time.time() + timeout
    while time.time() < end:
        st = client.get(f"{API}/simulation/status", headers=h).json()
        if pred(st):
            return st
        time.sleep(0.3)
    raise AssertionError(f"timeout; last status {st}")


def test_simulation_start_pause_resume_stop_and_persistence(client, analyst_h):
    tx0 = client.get(f"{API}/transactions?size=1", headers=analyst_h).json()["total"]
    r = client.post(f"{API}/simulation/start", headers=analyst_h,
                    json={"mode": "mixed", "tps": 40, "duration_seconds": 60, "customer_count": 2000,
                          "suspicious_pct": 10, "seed": 7})
    assert r.status_code == 201, r.text
    assert client.post(f"{API}/simulation/start", headers=analyst_h, json={}).status_code == 409
    _wait(client, analyst_h, lambda s: s.get("events_generated", 0) >= 20)
    assert client.post(f"{API}/simulation/pause", headers=analyst_h).json()["status"] == "PAUSED"
    time.sleep(0.6)
    n_paused = client.get(f"{API}/simulation/status", headers=analyst_h).json()["events_generated"]
    time.sleep(1.0)
    assert client.get(f"{API}/simulation/status", headers=analyst_h).json()["events_generated"] <= n_paused + 1
    assert client.post(f"{API}/simulation/resume", headers=analyst_h).json()["status"] == "RUNNING"
    _wait(client, analyst_h, lambda s: s.get("events_generated", 0) >= n_paused + 20)
    st = client.post(f"{API}/simulation/stop", headers=analyst_h).json()
    assert st["status"] == "STOPPED" and st["error"] is None
    tx1 = client.get(f"{API}/transactions?size=1", headers=analyst_h).json()["total"]
    assert tx1 - tx0 == st["events_generated"]   # every generated event persisted exactly once
    runs = client.get(f"{API}/simulation/runs", headers=analyst_h).json()
    assert runs[0]["status"] == "STOPPED" and runs[0]["events_generated"] == st["events_generated"]
    assert client.post(f"{API}/simulation/stop", headers=analyst_h).status_code == 409


def test_inject_and_saved_config_and_replay(client, analyst_h):
    r = client.post(f"{API}/simulation/inject", headers=analyst_h, json={"scenario": "velocity_burst", "seed": 99})
    assert r.status_code == 200
    evs = r.json()["events"]
    assert len(evs) >= 6 and all(e["status"] == "created" for e in evs)
    assert any(e["risk_category"] in ("MEDIUM", "HIGH") for e in evs)
    assert client.post(f"{API}/simulation/inject", headers=analyst_h, json={"scenario": "nope"}).status_code == 422
    c = client.post(f"{API}/simulation/configs", headers=analyst_h,
                    json={"name": "tiny burst", "config": {"mode": "fraud", "tps": 50, "max_events": 30, "seed": 5}})
    assert c.status_code == 201
    run = client.post(f"{API}/simulation/configs/{c.json()['id']}/run", headers=analyst_h)
    assert run.status_code == 201
    _wait(client, analyst_h, lambda s: s["status"] in ("COMPLETED", "FAILED"))
    rid = client.get(f"{API}/simulation/runs", headers=analyst_h).json()[0]["id"]
    rep = client.post(f"{API}/simulation/runs/{rid}/replay", headers=analyst_h)
    assert rep.status_code == 201 and rep.json()["config"]["seed"] == 5
    final = _wait(client, analyst_h, lambda s: s["status"] in ("COMPLETED", "FAILED"))
    assert final["status"] == "COMPLETED" and final["events_generated"] == 30
    assert client.delete(f"{API}/simulation/configs/{c.json()['id']}", headers=analyst_h).status_code == 204


def test_analytics_endpoints_return_real_aggregates(client, analyst_h):
    ov = client.get(f"{API}/analytics/overview", headers=analyst_h).json()
    assert ov["profile"] == "local" and ov["total_transactions"] > 0
    dr = ov["detection_rate"]
    assert dr["denominator"] > 0 and dr["value"] == dr["numerator"] / dr["denominator"]
    assert ov["detection_latency_ms"]["samples"] > 0 and ov["detection_latency_ms"]["p95"] >= ov["detection_latency_ms"]["p50"]
    tl = client.get(f"{API}/analytics/timeline?minutes=60&bucket_seconds=60", headers=analyst_h).json()
    assert sum(p["total"] for p in tl["points"]) > 0
    assert len(tl["points"]) in (60, 61)                      # zero-filled: one point per bucket, no gaps
    ts = [datetime.fromisoformat(p["t"]).timestamp() for p in tl["points"]]
    assert all(t % 60 == 0 for t in ts) and all(b - a == 60 for a, b in zip(ts, ts[1:]))   # real minute buckets
    db = SessionLocal()
    try:
        recent = db.scalar(select(func.count(Transaction.id)).where(
            Transaction.processed_at >= datetime.fromtimestamp(ts[0], timezone.utc)))
    finally:
        db.close()
    assert sum(p["total"] for p in tl["points"]) == recent             # aggregation loses/duplicates nothing
    rd = client.get(f"{API}/analytics/risk-distribution", headers=analyst_h).json()
    assert len(rd["bins"]) == 20 and sum(b["count"] for b in rd["bins"]) == ov["total_transactions"]
    pat = client.get(f"{API}/analytics/patterns", headers=analyst_h).json()
    assert any(p["scenario"] == "velocity_burst" for p in pat["scenarios"]) and pat["rule_hits"]
    cm = client.get(f"{API}/analytics/confusion", headers=analyst_h).json()["risk_engine_vs_simulator"]
    assert cm["tp"] + cm["fp"] + cm["fn"] + cm["tn"] == ov["total_transactions"]
    assert client.get(f"{API}/analytics/merchants", headers=analyst_h).json()["categories"]
    assert len(client.get(f"{API}/analytics/hourly", headers=analyst_h).json()["hours"]) == 24
    assert client.get(f"{API}/analytics/velocity?minutes=60", headers=analyst_h).json()["customers"]
    geo = client.get(f"{API}/analytics/geo?minutes=60", headers=analyst_h).json()
    assert geo["available"] and "SIMULATED" in geo["label"] and all(p["simulated"] for p in geo["points"])
    infra = client.get(f"{API}/infrastructure/status", headers=analyst_h).json()
    assert infra["database"]["status"] == "UP" and infra["kafka"]["status"] == "NOT_USED"
    assert infra["pipeline"]["counters"]["processed"] > 0
    sysst = client.get(f"{API}/system/status", headers=analyst_h).json()
    assert sysst["profile"] == "local" and "LOCAL DEVELOPMENT MODE" in sysst["profile_description"]


def test_drift_monitoring_on_live_predictions(client, analyst_h):
    r = client.get(f"{API}/drift/current", headers=analyst_h)
    assert r.status_code == 200
    d = r.json()
    assert d["status"] in ("STABLE", "MODERATE", "SIGNIFICANT", "INSUFFICIENT_DATA")
    if d["status"] != "INSUFFICIENT_DATA":
        assert "amount" in d["features"] and "Concept drift" in d["interpretation"]
        assert client.get(f"{API}/drift/history", headers=analyst_h).json()


def test_reports_download(client, analyst_h):
    md = client.get(f"{API}/reports/model-evaluation?format=md", headers=analyst_h)
    assert md.status_code == 200 and "| xgboost |" in md.text and "attachment" in md.headers["content-disposition"]
    js = client.get(f"{API}/reports/model-evaluation?format=json", headers=analyst_h).json()
    assert js and js[0]["metrics"]["test"]
    csv_ = client.get(f"{API}/reports/alerts.csv", headers=analyst_h)
    assert csv_.status_code == 200 and csv_.text.startswith("alert_uid,")
    perf = client.get(f"{API}/reports/performance", headers=analyst_h).json()
    assert "benchmarks_measured" in perf and perf["overview"]["total_transactions"] > 0


def test_dataset_upload_validation(client, admin_h, analyst_h):
    good = "step,type,amount,nameOrig,nameDest,isFraud\n1,PAYMENT,10,C1,M1,0\n1,TRANSFER,900,C1,C2,1\n"
    files = {"file": ("paysim.csv", good, "text/csv")}
    assert client.post(f"{API}/datasets/upload", headers=analyst_h, data={"kind": "paysim"}, files=files).status_code == 403
    r = client.post(f"{API}/datasets/upload", headers=admin_h, data={"kind": "paysim"}, files=files)
    assert r.status_code == 201 and r.json()["profile"]["positives"] == 1
    assert client.post(f"{API}/datasets/upload", headers=admin_h, data={"kind": "paysim"}, files=files).status_code == 409
    bad = {"file": ("x.csv", "a,b\n1,2\n", "text/csv")}
    assert client.post(f"{API}/datasets/upload", headers=admin_h, data={"kind": "paysim"}, files=bad).status_code == 422
    exe = {"file": ("evil.exe", "MZ\x00\x00", "application/octet-stream")}
    assert client.post(f"{API}/datasets/upload", headers=admin_h, data={"kind": "paysim"}, files=exe).status_code == 415
    assert any(d["source"] == "paysim" for d in client.get(f"{API}/datasets", headers=admin_h).json()["datasets"])
