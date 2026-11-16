"""Kafka persistence consumer (consumer group ``fraudstream-persistence``).

    transactions.normal | transactions.suspicious  ->  PostgreSQL (idempotent)
                                                   ->  fraud.alerts (alert-created notifications)
                                                   ->  Redis pub/sub (live dashboard feed)
                                                   ->  fraud.dlq (poison messages only)

Guarantees
* Offsets are committed only AFTER the database transaction commits (at-least-once).
* Re-delivery (consumer crash/restart, rebalance) cannot create duplicate transactions,
  predictions, alerts or audit rows: ``persist_scored_event`` checks the event id and the
  schema enforces UNIQUE(event_id), UNIQUE(prediction.transaction_id), UNIQUE(alert.transaction_id).
* Transient database failures are retried with capped exponential backoff and the offset
  is NOT advanced (no data is skipped); only messages that can never succeed (malformed /
  schema-invalid) are dead-lettered, then committed.
* ``fraud.alerts`` notifications are emitted after the DB commit; on re-delivery the alert
  already exists so no second notification is produced for it.

Run:  python -m app.streaming.persistence_consumer
"""
from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.exc import IntegrityError

from app.database.session import SessionLocal, init_engine, is_transient
from app.services.persistence import PersistResult, persist_scored_event
from app.services.pipeline import live_message
from streaming.kafka.client import PERSISTENCE_GROUP, Topics, consumer_config, encode, make_producer
from streaming.schemas.events import EventValidationError, parse_event

log = logging.getLogger("fraudstream.persistence")
REQUIRED_DECISION_KEYS = ("risk_score", "risk_category", "recommended_action", "detection_method",
                          "triggered_rules", "components", "explanation", "predicted_at", "engine_version")


class PoisonMessage(ValueError):
    """A message that can never be processed successfully (goes to the DLQ)."""


@dataclass
class ConsumerStats:
    consumed: int = 0
    created: int = 0
    duplicates: int = 0
    alerts: int = 0
    dead_lettered: int = 0
    db_retries: int = 0
    commits: int = 0


def decode_scored(value: bytes | None) -> tuple[dict[str, Any], Any]:
    if not value:
        raise PoisonMessage("EMPTY_PAYLOAD")
    try:
        scored = json.loads(value)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PoisonMessage(f"MALFORMED_JSON: {exc.__class__.__name__}") from exc
    if not isinstance(scored, dict) or not isinstance(scored.get("decision"), dict) or not isinstance(scored.get("features"), dict):
        raise PoisonMessage("SCORED_SCHEMA: missing event/decision/features")
    missing = [k for k in REQUIRED_DECISION_KEYS if k not in scored["decision"]]
    if missing:
        raise PoisonMessage(f"SCORED_SCHEMA: decision missing {missing}")
    try:
        event = parse_event(scored.get("event"))
    except EventValidationError as exc:
        raise PoisonMessage(exc.reason) from exc
    return scored, event


class PersistenceConsumer:
    def __init__(self, bootstrap: str, *, topics: Topics | None = None, group_id: str = PERSISTENCE_GROUP,
                 redis_client: Any | None = None, client_id: str = "fraudstream-persistence",
                 batch_size: int = 200, commit_offsets: bool = True, max_db_backoff: float = 10.0) -> None:
        from confluent_kafka import Consumer

        self.topics = topics or Topics.from_env()
        self.redis = redis_client
        self.consumer = Consumer(consumer_config(bootstrap, group_id, client_id))
        self.producer = make_producer(bootstrap, f"{client_id}-out")
        self.stats = ConsumerStats()
        self.batch_size = batch_size
        self.commit_offsets = commit_offsets      # False ONLY in tests that simulate a crash before commit
        self.max_db_backoff = max_db_backoff
        self._stop = threading.Event()
        self.last_error: str | None = None

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------ per-message
    def _persist(self, scored: dict[str, Any], event: Any) -> PersistResult:
        ingested = datetime.fromisoformat(scored["ingested_at"]) if scored.get("ingested_at") else None
        inf = scored.get("inference") or {}
        attempt = 0
        while True:
            db = SessionLocal()
            try:
                res = persist_scored_event(
                    db, event, scored["features"], scored["decision"], profile="distributed", ingested_at=ingested,
                    inference_ms=inf.get("inference_ms"), anomaly_version=inf.get("anomaly_version"),
                    contributions=inf.get("contributions"), explanation_method=inf.get("explanation_method"))
                db.commit()
                return res
            except IntegrityError as exc:
                db.rollback()
                raise PoisonMessage(f"INTEGRITY_ERROR: {str(exc.orig)[:200]}") from exc
            except Exception as exc:  # noqa: BLE001
                db.rollback()
                if not is_transient(exc) or self._stop.is_set():
                    raise
                attempt += 1
                self.stats.db_retries += 1
                self.last_error = f"database unavailable: {exc.__class__.__name__}"
                delay = min(self.max_db_backoff, 0.25 * 2 ** min(attempt, 6))
                log.warning("transient DB error (attempt %d), retrying in %.1fs: %s", attempt, delay, exc.__class__.__name__)
                self._heartbeat()
                self._stop.wait(delay)
            finally:
                db.close()

    def _dead_letter(self, msg: Any, reason: str) -> None:
        self.producer.produce(self.topics.dlq, key=msg.key(), value=msg.value() or b"", headers={
            "dlq_reason": reason[:500], "dlq_stage": "persistence", "source_topic": msg.topic(),
            "source_partition": str(msg.partition()), "source_offset": str(msg.offset()),
            "dlq_at": datetime.now(timezone.utc).isoformat()})
        if self.producer.flush(30):
            raise RuntimeError("DLQ record not acknowledged; refusing to commit offset")
        self.stats.dead_lettered += 1

    def handle(self, msg: Any) -> str:
        self.stats.consumed += 1
        try:
            scored, event = decode_scored(msg.value())
            res = self._persist(scored, event)
        except PoisonMessage as exc:
            log.warning("dead-lettering %s[%d]@%d: %s", msg.topic(), msg.partition(), msg.offset(), exc)
            self._dead_letter(msg, str(exc))
            return "dead_letter"
        if res.status == "duplicate":
            self.stats.duplicates += 1
            return "duplicate"
        self.stats.created += 1
        now = datetime.now(timezone.utc)
        lat = (now - datetime.fromisoformat(scored["ingested_at"])).total_seconds() * 1000 if scored.get("ingested_at") else None
        if self.redis is not None:
            try:
                self.redis.publish("fraudstream:live", json.dumps(live_message(scored, res.transaction_pk, res.alert_uid, now, lat), default=str))
            except Exception as exc:  # noqa: BLE001 - live feed is best effort; DB is the source of truth
                self.last_error = f"redis publish failed: {exc.__class__.__name__}"
        if res.alert_uid:
            self.stats.alerts += 1
            d = scored["decision"]
            self.producer.produce(self.topics.alerts, key=event.customer_id.encode(), value=encode({
                "type": "alert_created", "alert_uid": res.alert_uid, "transaction_id": event.transaction_id,
                "event_id": event.event_id, "customer_id": event.customer_id, "risk_score": d["risk_score"],
                "risk_category": d["risk_category"], "detection_method": d["detection_method"],
                "triggered_rules": [r["code"] for r in d["triggered_rules"]], "model_version": d.get("model_version"),
                "created_at": now.isoformat()}), headers={"event_id": event.event_id})
        return "created"

    def _heartbeat(self) -> None:
        hb = {"ts": time.time(), "group": PERSISTENCE_GROUP, "stats": asdict(self.stats), "last_error": self.last_error}
        # Local liveness file for the container health check (independent of Redis availability).
        health_file = os.environ.get("CONSUMER_HEALTH_FILE")
        if health_file:
            try:
                with open(health_file, "w", encoding="utf-8") as fh:
                    json.dump(hb, fh)
            except OSError:
                pass
        if self.redis is None:
            return
        try:
            self.redis.set("fraudstream:consumer:heartbeat", json.dumps(hb), ex=120)
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------ loop
    def run(self, *, idle_exit_seconds: float | None = None, max_messages: int | None = None) -> ConsumerStats:
        from confluent_kafka import KafkaError

        self.consumer.subscribe([self.topics.normal, self.topics.suspicious])
        idle_since = time.monotonic()
        last_hb = 0.0
        try:
            while not self._stop.is_set():
                msgs = self.consumer.consume(num_messages=self.batch_size, timeout=0.2)
                if time.monotonic() - last_hb > 5:
                    self._heartbeat()
                    last_hb = time.monotonic()
                if not msgs:
                    if idle_exit_seconds is not None and time.monotonic() - idle_since > idle_exit_seconds:
                        break
                    continue
                idle_since = time.monotonic()
                done = []
                for m in msgs:
                    if m.error():
                        if m.error().code() != KafkaError._PARTITION_EOF:
                            log.warning("consumer error: %s", m.error())
                        continue
                    if self._stop.is_set():
                        break
                    self.handle(m)
                    done.append(m)
                    if max_messages is not None and self.stats.consumed >= max_messages:
                        break
                if self.producer.flush(30):
                    raise RuntimeError("alert notifications not acknowledged; offsets not committed")
                if self.commit_offsets and done:
                    for m in done:
                        self.consumer.store_offsets(m)
                    self.consumer.commit(asynchronous=False)
                    self.stats.commits += 1
                if max_messages is not None and self.stats.consumed >= max_messages:
                    break
        finally:
            self._heartbeat()
            self.consumer.close()
        return self.stats


def main() -> None:
    from app.core.config import get_settings
    from app.core.logging import configure_logging

    configure_logging()
    s = get_settings()
    if not s.kafka_bootstrap_servers:
        raise SystemExit("KAFKA_BOOTSTRAP_SERVERS is required")
    init_engine(s.database_url)
    redis_client = None
    if s.redis_url:
        from app.services.redis_guard import guarded_from_url

        # Phase 12: circuit breaker - the live-feed publish and the heartbeat must not stall persistence while
        # Redis is down (measured: ~3.9 s per failed call -> ~0.25 events/s persisted)
        redis_client = guarded_from_url(s.redis_url, "redis (persistence consumer)")
    c = PersistenceConsumer(s.kafka_bootstrap_servers, redis_client=redis_client)
    signal.signal(signal.SIGINT, lambda *_: c.stop())
    signal.signal(signal.SIGTERM, lambda *_: c.stop())
    log.info("persistence consumer started (group=%s, prefix=%r)", PERSISTENCE_GROUP, c.topics.prefix)
    stats = c.run()
    log.info("persistence consumer stopped: %s", asdict(stats))


if __name__ == "__main__":
    main()
