"""Kafka stream processor (Python, NON-SPARK).

    transactions.raw --(validate, dedup, features, ML, risk)--> transactions.validated
                                                              -> transactions.normal | transactions.suspicious
                                                              -> fraud.dlq (invalid / late / failed)

This is a genuine Kafka consumer/producer built on the SAME schema, feature, model and
risk code as every other path. In the full distributed profile the Spark Structured
Streaming job (Phase 9) performs this stage; this processor exists for Kafka-level
integration and as a lightweight alternative, and it reports itself as
``processor=python-kafka`` so it is never mistaken for Spark.

Delivery: at-least-once. Offsets are committed only after every output record of the
batch has been acknowledged by Kafka (acks=all). After a crash, uncommitted input is
re-consumed; duplicates are suppressed by event-id dedup here and by unique
constraints at the persistence boundary.

Run:  python -m app.streaming.processor
"""
from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.risk_engine.detection import detect, is_suspicious
from app.streaming.runtime_watch import RuntimeWatcher
from ml.features.engineering import FeatureStore
from streaming.kafka.client import (
    PROCESSOR_GROUP,
    Topics,
    consumer_config,
    encode,
    make_producer,
)
from streaming.schemas.events import EventValidationError, parse_event

log = logging.getLogger("fraudstream.processor")
MAX_PROCESSING_ATTEMPTS = 3


@dataclass
class Output:
    topic: str
    key: bytes | None
    value: bytes
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class ProcessorStats:
    consumed: int = 0
    valid: int = 0
    invalid: int = 0
    duplicates: int = 0
    late: int = 0
    processing_failures: int = 0
    to_normal: int = 0
    to_suspicious: int = 0
    model_unavailable: int = 0
    batches_committed: int = 0
    delivery_retries: int = 0


def dlq_record(topics: Topics, value: bytes, key: bytes | None, reason: str, source: dict[str, Any]) -> Output:
    return Output(topics.dlq, key, value, {
        "dlq_reason": reason[:500], "dlq_stage": "processor", "source_topic": str(source.get("topic")),
        "source_partition": str(source.get("partition")), "source_offset": str(source.get("offset")),
        "dlq_at": datetime.now(timezone.utc).isoformat()})


def route_raw(value: bytes | None, key: bytes | None, kafka_ts_ms: int | None, source: dict[str, Any],
              topics: Topics, store: FeatureStore, watcher: RuntimeWatcher, stats: ProcessorStats) -> list[Output]:
    """Pure routing logic for ONE raw record (no Kafka I/O). Raises only for unexpected errors."""
    stats.consumed += 1
    if not value:   # None (tombstone) or b"" (empty record)
        stats.invalid += 1
        return [dlq_record(topics, b"", key, "EMPTY_PAYLOAD", source)]
    try:
        event = parse_event(value)
    except EventValidationError as exc:
        stats.invalid += 1
        return [dlq_record(topics, value, key, exc.reason, source)]
    res = store.process(event, apply=False)
    if res.status == "duplicate":
        stats.duplicates += 1
        return []
    if res.status == "late":
        stats.late += 1
        return [dlq_record(topics, value, key, "LATE_EVENT_BEYOND_TOLERANCE", source)]
    assert res.features is not None
    ingested_at = datetime.fromtimestamp(kafka_ts_ms / 1000, timezone.utc) if kafka_ts_ms and kafka_ts_ms > 0 else None
    scored = detect(event.to_wire(), res.features, watcher.scorer if watcher.scorer.ready else None, watcher.engine,
                    ingested_at=ingested_at)
    scored["processor"] = "python-kafka"
    scored["runtime_config_source"] = watcher.source
    if scored["model_error"]:
        stats.model_unavailable += 1
    store.apply(event)   # state advanced once outputs are built; re-delivery is then a duplicate
    stats.valid += 1
    k = event.customer_id.encode()
    hdr = {"event_id": event.event_id, "schema_version": event.schema_version, "processor": "python-kafka"}
    out = [Output(topics.validated, k, encode(event.to_wire()), hdr)]
    if is_suspicious(scored):
        stats.to_suspicious += 1
        out.append(Output(topics.suspicious, k, encode(scored), hdr))
    else:
        stats.to_normal += 1
        out.append(Output(topics.normal, k, encode(scored), hdr))
    return out


class StreamProcessor:
    def __init__(self, bootstrap: str, artifacts_dir: Path, *, topics: Topics | None = None,
                 group_id: str = PROCESSOR_GROUP, redis_client: Any | None = None,
                 warm_events: Any | None = None, batch_size: int = 500, client_id: str = "fraudstream-processor") -> None:
        from confluent_kafka import Consumer

        self.topics = topics or Topics.from_env()
        self.bootstrap = bootstrap
        self.redis = redis_client
        self.watcher = RuntimeWatcher(artifacts_dir, redis_client)
        self.watcher.refresh(force=True)
        self.store = FeatureStore()
        self.warm_loader = warm_events
        self.stats = ProcessorStats()
        self.batch_size = batch_size
        self.producer = make_producer(bootstrap, f"{client_id}-out")
        self.consumer = Consumer(consumer_config(bootstrap, group_id, client_id))
        self._stop = threading.Event()
        self._warmed = False
        self.last_error: str | None = None

    def stop(self) -> None:
        self._stop.set()

    def _on_assign(self, consumer: Any, partitions: Any) -> None:
        if not self._warmed and self.warm_loader is not None:
            n = self.store.warm(self.warm_loader())
            log.info("feature state warm start: %d events", n)
            self._warmed = True

    def _deliver(self, outputs: list[Output]) -> None:
        """Produce outputs and wait for acks; retry failed records; raise if they cannot be delivered."""
        pending = outputs
        for attempt in range(6):
            failed: list[Output] = []

            def cb(err: Any, _msg: Any, o: Output) -> None:
                if err is not None:
                    failed.append(o)
            for o in pending:
                while True:
                    try:
                        self.producer.produce(o.topic, key=o.key, value=o.value, headers=o.headers,
                                              on_delivery=lambda e, m, o=o: cb(e, m, o))
                        break
                    except BufferError:
                        self.producer.poll(0.2)
            remaining = self.producer.flush(30)
            if remaining:
                raise RuntimeError(f"{remaining} output records not acknowledged within 30 s")
            if not failed:
                return
            self.stats.delivery_retries += 1
            pending = failed
            time.sleep(min(5.0, 0.5 * 2 ** attempt))
        raise RuntimeError(f"{len(pending)} output records could not be delivered")

    def _heartbeat(self) -> None:
        if self.redis is None:
            return
        try:
            self.redis.set("fraudstream:processor:heartbeat", json.dumps({
                "ts": time.time(), "processor": "python-kafka", "stats": asdict(self.stats),
                "runtime_config_source": self.watcher.source, "model_error": self.watcher.model_error,
                "model": self.watcher.scorer.supervised.label if self.watcher.scorer.supervised else None,
                "last_error": self.last_error}), ex=120)
        except Exception:  # noqa: BLE001
            pass

    def run(self, *, idle_exit_seconds: float | None = None, max_messages: int | None = None) -> ProcessorStats:
        from confluent_kafka import KafkaError

        self.consumer.subscribe([self.topics.raw], on_assign=self._on_assign)
        idle_since = time.monotonic()
        last_hb = 0.0
        try:
            while not self._stop.is_set():
                self.watcher.refresh()
                msgs = self.consumer.consume(num_messages=self.batch_size, timeout=0.2)
                if time.monotonic() - last_hb > 5:
                    self._heartbeat()
                    last_hb = time.monotonic()
                if not msgs:
                    if idle_exit_seconds is not None and time.monotonic() - idle_since > idle_exit_seconds:
                        break
                    continue
                idle_since = time.monotonic()
                outputs: list[Output] = []
                last_good = []
                for m in msgs:
                    if m.error():
                        if m.error().code() != KafkaError._PARTITION_EOF:
                            log.warning("consumer error: %s", m.error())
                        continue
                    src = {"topic": m.topic(), "partition": m.partition(), "offset": m.offset()}
                    ts_type, ts = m.timestamp()
                    for attempt in range(1, MAX_PROCESSING_ATTEMPTS + 1):
                        try:
                            outputs += route_raw(m.value(), m.key(), ts if ts_type else None, src, self.topics,
                                                 self.store, self.watcher, self.stats)
                            break
                        except Exception as exc:  # noqa: BLE001 - retry, then dead-letter
                            self.last_error = f"{exc.__class__.__name__}: {exc}"
                            log.warning("processing attempt %d failed at %s: %s", attempt, src, exc)
                            if attempt == MAX_PROCESSING_ATTEMPTS:
                                self.stats.processing_failures += 1
                                outputs.append(dlq_record(self.topics, m.value() or b"", m.key(),
                                                          f"PROCESSING_ERROR: {self.last_error}", src))
                            else:
                                time.sleep(0.2 * attempt)
                    last_good.append(m)
                self._deliver(outputs)
                for m in last_good:
                    self.consumer.store_offsets(m)
                self.consumer.commit(asynchronous=False)
                self.stats.batches_committed += 1
                if max_messages is not None and self.stats.consumed >= max_messages:
                    break
        finally:
            self._heartbeat()
            self.consumer.close()
        return self.stats


def _db_warm_loader(database_url: str, hours: int):  # type: ignore[no-untyped-def]
    def load():  # type: ignore[no-untyped-def]
        from datetime import timedelta

        from sqlalchemy import create_engine, text

        eng = create_engine(database_url)
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        with eng.connect() as c:
            rows = c.execute(text(
                "SELECT event_id, customer_id, event_time, amount, status, device_id, merchant_id, merchant_category, "
                "channel, transaction_type, city, country, lat, lon FROM transactions WHERE processed_at >= :s "
                "ORDER BY event_time LIMIT 500000"), {"s": since}).fetchall()
        eng.dispose()
        for r in rows:
            et = r.event_time if isinstance(r.event_time, datetime) else datetime.fromisoformat(str(r.event_time))
            yield {"event_id": r.event_id, "customer_id": r.customer_id,
                   "event_time": et if et.tzinfo else et.replace(tzinfo=timezone.utc), "amount": float(r.amount),
                   "status": r.status, "device_id": r.device_id, "merchant_id": r.merchant_id,
                   "merchant_category": r.merchant_category, "channel": r.channel, "transaction_type": r.transaction_type,
                   "location": {"city": r.city, "country": r.country, "lat": r.lat, "lon": r.lon}}
    return load


def main() -> None:
    from app.core.logging import configure_logging

    configure_logging()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        raise SystemExit("KAFKA_BOOTSTRAP_SERVERS is required")
    root = Path(__file__).resolve().parents[3]
    artifacts = Path(os.environ.get("FRAUDSTREAM_ARTIFACTS_DIR", root / "ml" / "artifacts"))
    redis_client = None
    if os.environ.get("REDIS_URL"):
        from app.services.redis_guard import guarded_from_url

        redis_client = guarded_from_url(os.environ["REDIS_URL"], "redis (fallback processor)")
    if redis_client is not None and os.environ.get("ALLOW_CONCURRENT_PROCESSORS", "0") != "1":
        from app.services.streaming_status import spark_status

        if spark_status(redis_client, {}).get("status") == "RUNNING":
            raise SystemExit("Spark Structured Streaming job is running (fresh progress); refusing to start the "
                             "python-kafka processor on the same topics. Set ALLOW_CONCURRENT_PROCESSORS=1 to override.")
    db = os.environ.get("DATABASE_URL")
    proc = StreamProcessor(bootstrap, artifacts, redis_client=redis_client,
                           warm_events=_db_warm_loader(db, int(os.environ.get("FEATURE_WARM_START_HOURS", "24"))) if db else None)
    signal.signal(signal.SIGINT, lambda *_: proc.stop())
    signal.signal(signal.SIGTERM, lambda *_: proc.stop())
    log.info("python-kafka processor started (topics prefix=%r)", proc.topics.prefix)
    stats = proc.run()
    log.info("processor stopped: %s", asdict(stats))


if __name__ == "__main__":
    main()
