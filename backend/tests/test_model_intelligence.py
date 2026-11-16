"""Model Intelligence endpoints: artifacts listing, live inference latency, experiment detail,
stored drift reports. Real models / real pipeline; artifacts are the per-session temp COPY."""
from __future__ import annotations

import hashlib
import os
import stat

import pytest
from app.core.runtime import get_runtime
from app.database.session import SessionLocal
from app.models.entities import DriftReport

API = "/api/v1"


def test_artifacts_listing_matches_files_on_disk(client, analyst_h):
    r = client.get(f"{API}/models/artifacts", headers=analyst_h)
    assert r.status_code == 200
    body = r.json()
    d = get_runtime().settings.artifacts_dir
    on_disk = sorted(p.relative_to(d).as_posix() for p in d.rglob("*") if p.is_file())
    assert [f["path"] for f in body["files"]] == on_disk
    xgb = next(f for f in body["files"] if f["path"].endswith("xgboost/v20261114T124627Z/model.joblib"))
    p = d / xgb["path"]
    assert xgb["bytes"] == p.stat().st_size and xgb["sha256"] == hashlib.sha256(p.read_bytes()).hexdigest()
    assert body["artifacts_dir_writable"] is True          # temp copy is writable
    assert body["registry_active"]["supervised"] == body["database_active"]["supervised"]
    assert client.get(f"{API}/models/active", headers=analyst_h).json()["artifacts_writable"] is True


def test_artifacts_read_only_is_reported(client, analyst_h, admin_h):
    d = get_runtime().settings.artifacts_dir
    if os.name == "nt":
        pytest.skip("Windows ignores directory read-only bits for os.access; covered by the container tests")
    mode = d.stat().st_mode
    os.chmod(d, stat.S_IRUSR | stat.S_IXUSR)
    try:
        assert client.get(f"{API}/models/artifacts", headers=analyst_h).json()["artifacts_dir_writable"] is False
        assert client.get(f"{API}/models/active", headers=analyst_h).json()["artifacts_writable"] is False
    finally:
        os.chmod(d, mode)


def test_live_inference_latency_from_persisted_predictions(client, analyst_h, make_events):
    for ev in make_events(n_normal=6, seed=41):
        assert client.post(f"{API}/transactions/ingest", headers=analyst_h, json=ev.to_wire()).status_code == 202
    body = client.get(f"{API}/models/live-inference", headers=analyst_h).json()
    assert body["window_rows"] >= 6 and body["total_predictions"] >= body["window_rows"]
    active = get_runtime().scorer.supervised.label
    m = next(x for x in body["models"] if x["model_version"] == active)
    assert m["samples"] >= 6 and 0 <= m["p50_ms"] <= m["p95_ms"] <= m["p99_ms"] <= m["max_ms"]
    assert client.get(f"{API}/models/live-inference?limit=5", headers=analyst_h).status_code == 422


def test_experiment_detail(client, analyst_h):
    exps = client.get(f"{API}/models/experiments", headers=analyst_h).json()
    e = client.get(f"{API}/models/experiments/{exps[0]['id']}", headers=analyst_h).json()
    assert e["experiment_version"] == exps[0]["experiment_version"]
    assert e["summary"]["dataset"]["rows"] > 0 and e["summary"]["recommended_supervised"]
    assert {m["model_name"] for m in e["model_versions"]} >= {"xgboost", "isolation_forest"}
    assert client.get(f"{API}/models/experiments/999999", headers=analyst_h).status_code == 404


def test_stored_drift_report_roundtrip(client, analyst_h):
    report = {"rows": 321, "status": "MODERATE", "max_psi": 0.17, "thresholds": {"moderate": 0.1, "significant": 0.25},
              "features": {"amount": {"psi": 0.17, "status": "MODERATE"}}, "score_psi": {"psi": 0.05, "status": "STABLE"},
              "drifted_features": ["amount"]}
    with SessionLocal() as db:
        row = DriftReport(model_version="unit:test", window_rows=321, status="MODERATE", max_psi=0.17, report=report)
        db.add(row)
        db.commit()
        rid = row.id
    got = client.get(f"{API}/drift/reports/{rid}", headers=analyst_h).json()
    assert got["report"] == report and got["rows"] == 321 and got["model_version"] == "unit:test"
    hist = client.get(f"{API}/drift/history", headers=analyst_h).json()
    h = next(x for x in hist if x["id"] == rid)
    assert h["score_psi"] == 0.05 and h["drifted_features"] == ["amount"]
    assert client.get(f"{API}/drift/reports/999999", headers=analyst_h).status_code == 404
