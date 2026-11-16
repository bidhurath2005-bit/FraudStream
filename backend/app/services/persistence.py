"""Idempotent persistence of scored events — shared by the LOCAL pipeline and the
DISTRIBUTED Kafka persistence consumer.

Idempotency guarantees:
* ``transactions.event_id`` and ``transactions.transaction_id`` are UNIQUE.
* ``predictions.transaction_id`` and ``alerts.transaction_id`` are UNIQUE, so at most one
  prediction and one alert can ever exist per transaction, even if a message is
  re-delivered after a consumer restart.
* The transaction, prediction, alert and audit rows are written in ONE database
  transaction: either all exist or none do.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.entities import Alert, Customer, DeadLetter, Merchant, Prediction, Transaction
from app.services import audit
from streaming.schemas.events import TransactionEvent

ALERT_CATEGORIES = frozenset({"MEDIUM", "HIGH"})


@dataclass
class PersistResult:
    status: str                      # created | duplicate
    transaction_pk: int | None = None
    alert_id: int | None = None
    alert_uid: str | None = None


def _insert_ignore(db: Session, model: type, values: dict[str, Any]) -> None:
    dialect = db.get_bind().dialect.name
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert
    db.execute(insert(model).values(**values).on_conflict_do_nothing())


def persist_scored_event(db: Session, event: TransactionEvent, features: dict[str, Any],
                         decision: dict[str, Any], *, profile: str, ingested_at: datetime | None,
                         inference_ms: float | None, anomaly_version: str | None,
                         contributions: list[dict[str, Any]] | None, explanation_method: str | None) -> PersistResult:
    """Persist one scored event. Caller owns commit/rollback. Duplicate -> no writes."""
    existing = db.scalar(select(Transaction.id).where(Transaction.event_id == event.event_id))
    if existing is not None:
        return PersistResult("duplicate", existing)
    now = datetime.now(timezone.utc)
    _insert_ignore(db, Customer, {"id": event.customer_id, "home_city": event.location.city,
                                  "synthetic": event.synthetic, "first_seen_at": now})
    _insert_ignore(db, Merchant, {"id": event.merchant_id, "name": event.merchant_name,
                                  "category": event.merchant_category})
    gt = event.ground_truth
    latency = (now - ingested_at).total_seconds() * 1000 if ingested_at else None
    tx = Transaction(
        event_id=event.event_id, transaction_id=event.transaction_id, customer_id=event.customer_id,
        account_id=event.account_id, merchant_id=event.merchant_id, amount=event.amount, currency=event.currency,
        event_time=event.event_time, ingested_at=ingested_at, processed_at=now,
        transaction_type=str(event.transaction_type), merchant_category=event.merchant_category,
        channel=str(event.channel), device_id=event.device_id, ip_address=event.ip_address,
        city=event.location.city, country=event.location.country, lat=event.location.lat, lon=event.location.lon,
        location_simulated=event.location.simulated, status=str(event.status), schema_version=event.schema_version,
        synthetic=event.synthetic, source=event.source, profile=profile,
        sim_is_suspicious=gt.is_suspicious if gt else None, sim_scenario=gt.scenario if gt else None,
        pipeline_latency_ms=latency,
    )
    db.add(tx)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        existing = db.scalar(select(Transaction.id).where(Transaction.event_id == event.event_id))
        return PersistResult("duplicate", existing)
    db.add(Prediction(
        transaction_id=tx.id, model_version=decision.get("model_version"), anomaly_version=anomaly_version,
        model_score=decision.get("model_score"), model_threshold=decision.get("model_threshold"),
        anomaly_score=decision.get("anomaly_score"), risk_score=decision["risk_score"],
        risk_category=decision["risk_category"], recommended_action=decision["recommended_action"],
        detection_method=decision["detection_method"], triggered_rules=decision["triggered_rules"],
        components=decision["components"], features=features, contributions=contributions or [],
        explanation_method=explanation_method, explanation=decision["explanation"], inference_ms=inference_ms,
        engine_version=decision["engine_version"],
        predicted_at=datetime.fromisoformat(decision["predicted_at"]),
    ))
    result = PersistResult("created", tx.id)
    if decision["risk_category"] in ALERT_CATEGORIES:
        alert = Alert(alert_uid=str(uuid.uuid4()), transaction_id=tx.id, customer_id=event.customer_id,
                      risk_score=decision["risk_score"], risk_category=decision["risk_category"],
                      detection_method=decision["detection_method"], triggered_rules=decision["triggered_rules"],
                      model_version=decision.get("model_version"), explanation=decision["explanation"], status="NEW")
        db.add(alert)
        db.flush()
        audit.record(db, None, "ALERT_CREATED", "alert", alert.alert_uid, field="status", new="NEW",
                     details={"risk_score": alert.risk_score, "transaction_id": event.transaction_id})
        result.alert_id, result.alert_uid = alert.id, alert.alert_uid
    return result


def record_dead_letter(db: Session, source: str, reason: str, payload: Any, event_id: str | None = None) -> None:
    if not isinstance(payload, dict):
        payload = {"raw": str(payload)[:4000]}
    db.add(DeadLetter(source=source, reason=reason[:300], event_id=event_id, payload=payload))
