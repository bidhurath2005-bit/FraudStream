"""Local pipeline: ingest -> features -> real ML inference -> risk -> persistence -> alerts.
Includes idempotency (duplicate events) and failure handling."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from app.core.runtime import get_runtime
from app.database.session import SessionLocal
from app.models.entities import Alert, AuditLog, DeadLetter, Prediction, Transaction
from app.risk_engine.detection import detect
from app.services.persistence import persist_scored_event

API = "/api/v1"


def counts():
    db = SessionLocal()
    try:
        return tuple(db.scalar(select(func.count(m.id))) for m in (Transaction, Prediction, Alert))
    finally:
        db.close()


def ingest(client, h, ev):
    return client.post(f"{API}/transactions/ingest", headers=h, json=ev.to_wire())


def test_ingest_scores_with_real_model_and_persists(client, analyst_h, make_events):
    evs = make_events(n_normal=5)
    for ev in evs:
        r = ingest(client, analyst_h, ev)
        assert r.status_code == 202, r.text
        body = r.json()
        assert body["status"] == "created" and body["model_error"] is None
        d = body["decision"]
        assert d["model_version"].startswith(get_runtime().scorer.supervised.model_name)
        assert 0 <= d["risk_score"] <= 100 and d["risk_category"] in ("LOW", "MEDIUM", "HIGH")
    t = client.get(f"{API}/transactions/{body['transaction_pk']}", headers=analyst_h).json()
    assert t["transaction_id"] == evs[-1].transaction_id and t["synthetic"] is True
    p = t["prediction"]
    assert p["features"]["hist_tx_count"] >= 0 and p["model_score"] is not None
    assert "is_suspicious" not in p["features"] and "scenario" not in p["features"]
    assert p["explanation_method"] in ("xgboost_treeshap", "saabas_path_attribution", "linear_coef_x_value")


def test_suspicious_scenario_generates_alert_with_required_fields(client, analyst_h, make_events):
    evs = make_events("account_takeover", n_normal=15)
    alerts = []
    for ev in evs:
        r = ingest(client, analyst_h, ev).json()
        if r.get("alert_uid"):
            alerts.append(r["alert_uid"])
    assert alerts, "account takeover produced no alert"
    a = client.get(f"{API}/alerts/{alerts[-1]}", headers=analyst_h).json()
    for k in ("alert_uid", "transaction_id", "customer_id", "risk_score", "risk_category", "detection_method",
              "triggered_rules", "model_version", "explanation", "created_at", "status"):
        assert a[k] is not None, k
    assert a["status"] == "NEW" and a["risk_category"] in ("MEDIUM", "HIGH")
    assert any(x["action"] == "ALERT_CREATED" for x in a["audit"])


def test_duplicate_event_creates_no_duplicate_transaction_or_alert(client, analyst_h, make_events):
    evs = make_events("velocity_burst", n_normal=3)
    first = [ingest(client, analyst_h, e).json() for e in evs]
    assert any(r.get("alert_uid") for r in first)
    before = counts()
    again = [ingest(client, analyst_h, e).json() for e in evs]          # exact re-delivery
    assert all(r["status"] == "duplicate" for r in again)
    assert counts() == before


def test_duplicate_after_restart_is_caught_by_database_constraint(client, analyst_h, make_events):
    """Simulates a consumer restart: in-memory dedup state is lost, the DB must still dedupe."""
    evs = make_events("large_amount", n_normal=2)
    for e in evs:
        ingest(client, analyst_h, e)
    before = counts()
    rt = get_runtime()
    rt.feature_store.clear()                                             # 'restart' without warm start
    again = [ingest(client, analyst_h, e).json() for e in evs]
    assert [r["status"] for r in again] == ["duplicate"] * len(evs)
    assert counts() == before
    rt.warm_feature_store(SessionLocal())


def test_persist_scored_event_is_idempotent_at_db_boundary(client, make_events):
    ev = make_events("large_amount", n_normal=1)[-1]
    rt = get_runtime()
    feats = rt.feature_store.process(ev, apply=False).features
    scored = detect(ev.to_wire(), feats, rt.scorer, rt.engine, ingested_at=datetime.now(timezone.utc))
    results = []
    for _ in range(3):
        db = SessionLocal()
        try:
            inf = scored["inference"]
            results.append(persist_scored_event(db, ev, feats, scored["decision"], profile="local", ingested_at=None,
                                                inference_ms=inf["inference_ms"], anomaly_version=None,
                                                contributions=[], explanation_method=None))
            db.commit()
        finally:
            db.close()
    assert [r.status for r in results] == ["created", "duplicate", "duplicate"]
    db = SessionLocal()
    try:
        n_tx = db.scalar(select(func.count()).where(Transaction.event_id == ev.event_id))
        n_al = db.scalar(select(func.count(Alert.id)).join(Transaction).where(Transaction.event_id == ev.event_id))
        n_audit = db.scalar(select(func.count(AuditLog.id)).where(AuditLog.action == "ALERT_CREATED",
                                                                  AuditLog.entity_id == (results[0].alert_uid or "-")))
    finally:
        db.close()
    assert n_tx == 1 and n_al == (1 if results[0].alert_uid else 0)
    assert n_audit == (1 if results[0].alert_uid else 0)


def test_invalid_payload_rejected_and_dead_lettered(client, analyst_h, make_events):
    good = make_events(n_normal=1)[0].to_wire()
    db = SessionLocal()
    n0 = db.scalar(select(func.count(DeadLetter.id)))
    db.close()
    r = client.post(f"{API}/transactions/ingest", headers=analyst_h, json={**good, "amount": -10})
    assert r.status_code == 422 and "SCHEMA_VIOLATION" in r.json()["detail"]
    db = SessionLocal()
    assert db.scalar(select(func.count(DeadLetter.id))) == n0 + 1
    db.close()


def test_late_event_beyond_tolerance_goes_to_dead_letters(client, analyst_h, make_events):
    evs = make_events(n_normal=3)
    for e in evs:
        ingest(client, analyst_h, e)
    late = make_events(n_normal=1)[0].model_copy(update={
        "customer_id": evs[-1].customer_id, "event_id": "late-event-0001", "transaction_id": "TXLATE0001",
        "event_time": evs[-1].event_time - timedelta(hours=2)})
    r = ingest(client, analyst_h, late).json()
    assert r["status"] == "late"
    db = SessionLocal()
    assert db.scalar(select(DeadLetter).where(DeadLetter.event_id == "late-event-0001")).reason == "LATE_EVENT_BEYOND_TOLERANCE"
    db.close()


def test_model_unavailable_falls_back_to_rules_only_and_says_so(client, analyst_h, make_events):
    rt = get_runtime()
    saved = rt.scorer.supervised
    rt.scorer.supervised = None
    try:
        evs = make_events("velocity_burst", n_normal=1)
        results = [ingest(client, analyst_h, e).json() for e in evs]
    finally:
        rt.scorer.supervised = saved
    assert all(r["model_error"] for r in results)
    assert all(r["decision"]["model_version"] is None for r in results)
    assert any(r["decision"]["detection_method"] == "RULES" for r in results)
    assert "rules only" in results[-1]["decision"]["explanation"].lower()


def test_transactions_list_filters_pagination_and_sorting(client, analyst_h):
    r = client.get(f"{API}/transactions?size=5&page=1&sort=risk_score&order=desc", headers=analyst_h).json()
    assert r["total"] > 5 and len(r["items"]) == 5
    scores = [i["risk_score"] for i in r["items"]]
    assert scores == sorted(scores, reverse=True)
    p2 = client.get(f"{API}/transactions?size=5&page=2&sort=risk_score&order=desc", headers=analyst_h).json()
    assert not {i["id"] for i in r["items"]} & {i["id"] for i in p2["items"]}
    hi = client.get(f"{API}/transactions?risk_category=HIGH&size=50", headers=analyst_h).json()
    assert all(i["risk_category"] == "HIGH" for i in hi["items"])
    assert client.get(f"{API}/transactions/999999999", headers=analyst_h).status_code == 404
