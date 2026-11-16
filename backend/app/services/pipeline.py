"""LOCAL-profile event pipeline (in-process, single node).

simulator/API -> validation -> FeatureStore (point-in-time features) -> detect()
-> idempotent persistence -> live broadcast.

This is a *simplified replacement* for Kafka + Spark used in the LOCAL DEVELOPMENT
profile. It reuses exactly the same schema, feature, model, risk and persistence code as
the distributed profile, but it is single-process and not fault-tolerant across crashes
beyond what the database provides (feature state is rebuilt from the DB on restart).
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any

from app.database.session import session_scope, with_retry
from app.risk_engine.detection import detect
from app.services.persistence import persist_scored_event, record_dead_letter
from streaming.schemas.events import TransactionEvent

log = logging.getLogger("fraudstream.pipeline")


def live_message(scored: dict[str, Any], tx_pk: int | None, alert_uid: str | None, processed_at: datetime,
                 latency_ms: float | None) -> dict[str, Any]:
    ev, d = scored["event"], scored["decision"]
    return {
        "type": "transaction", "id": tx_pk, "event_id": ev["event_id"], "transaction_id": ev["transaction_id"],
        "customer_id": ev["customer_id"], "merchant_id": ev["merchant_id"], "merchant_name": ev.get("merchant_name", ""),
        "merchant_category": ev["merchant_category"], "amount": ev["amount"], "currency": ev["currency"],
        "event_time": ev["event_time"], "processed_at": processed_at.isoformat(), "status": ev["status"],
        "channel": ev["channel"], "city": ev["location"]["city"], "country": ev["location"]["country"],
        "risk_score": d["risk_score"], "risk_category": d["risk_category"], "detection_method": d["detection_method"],
        "recommended_action": d["recommended_action"], "model_version": d["model_version"],
        "alert_uid": alert_uid, "latency_ms": latency_ms, "model_error": scored.get("model_error"),
    }


class LocalPipeline:
    def __init__(self, runtime: Any) -> None:
        self.rt = runtime
        self._lock = threading.Lock()
        self.counters = {"processed": 0, "duplicates": 0, "late": 0, "errors": 0, "alerts": 0, "model_unavailable": 0}

    def process(self, event: TransactionEvent, *, ingested_at: datetime | None = None) -> dict[str, Any]:
        ingested_at = ingested_at or datetime.now(timezone.utc)
        rt = self.rt
        with self._lock:
            res = rt.feature_store.process(event, apply=False)
            if res.status == "duplicate":
                self.counters["duplicates"] += 1
                return {"status": "duplicate", "event_id": event.event_id}
            if res.status == "late":
                self.counters["late"] += 1
                with session_scope() as db:
                    record_dead_letter(db, "local-pipeline", "LATE_EVENT_BEYOND_TOLERANCE", event.to_wire(), event.event_id)
                return {"status": "late", "event_id": event.event_id}
            assert res.features is not None
            wire = event.to_wire()
            scored = detect(wire, res.features, rt.scorer if rt.scorer.ready else None, rt.engine,
                            ingested_at=ingested_at)
            if scored["model_error"]:
                self.counters["model_unavailable"] += 1

            def _tx() -> Any:
                with session_scope() as db:
                    inf = scored["inference"]
                    return persist_scored_event(
                        db, event, res.features or {}, scored["decision"], profile="local", ingested_at=ingested_at,
                        inference_ms=inf["inference_ms"], anomaly_version=inf["anomaly_version"],
                        contributions=inf["contributions"], explanation_method=inf["explanation_method"])
            try:
                pr = with_retry(_tx)
            except Exception:
                self.counters["errors"] += 1
                log.exception("persistence failed for event %s", event.event_id)
                raise
            # state is only advanced after the event is durably stored (or already was)
            rt.feature_store.apply(event)
            if pr.status == "duplicate":
                self.counters["duplicates"] += 1
                return {"status": "duplicate", "event_id": event.event_id}
            self.counters["processed"] += 1
            if pr.alert_uid:
                self.counters["alerts"] += 1
        now = datetime.now(timezone.utc)
        msg = live_message(scored, pr.transaction_pk, pr.alert_uid, now, (now - ingested_at).total_seconds() * 1000)
        rt.broker.publish(msg)
        return {"status": "created", "transaction_pk": pr.transaction_pk, "alert_uid": pr.alert_uid,
                "decision": scored["decision"], "model_error": scored["model_error"]}
