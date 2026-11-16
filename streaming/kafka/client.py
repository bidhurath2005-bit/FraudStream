"""Kafka topic definitions and client helpers (confluent-kafka / librdkafka).

Delivery semantics (see docs/ARCHITECTURE.md section 6, "Delivery guarantees"):
* Producers use ``acks=all`` + ``enable.idempotence=true``: a record acknowledged by the
  broker is durably written to all in-sync replicas (1 broker in the local compose) and
  producer retries cannot create broker-side duplicates within a producer session.
* Consumers disable auto-commit and commit offsets MANUALLY only after their side effects
  (produced outputs acknowledged / database transaction committed): AT-LEAST-ONCE delivery.
  Re-delivered messages are absorbed by event-id deduplication and unique constraints
  (idempotent persistence). The pipeline is *effectively-once at the database boundary*,
  NOT end-to-end exactly-once (Kafka transactions are not used).

Topic names may be prefixed with ``FRAUDSTREAM_TOPIC_PREFIX`` (used by integration tests
to run on isolated topics); the default prefix is empty.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("fraudstream.kafka")


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_ms: int
    description: str


TOPIC_SPECS: tuple[TopicSpec, ...] = (
    TopicSpec("transactions.raw", 6, 7 * 86_400_000, "Raw events from simulator/API; key = customer_id"),
    TopicSpec("transactions.validated", 6, 3 * 86_400_000, "Schema-valid, de-duplicated events"),
    TopicSpec("transactions.normal", 6, 3 * 86_400_000, "Scored events with LOW risk"),
    TopicSpec("transactions.suspicious", 6, 7 * 86_400_000, "Scored events with MEDIUM/HIGH risk"),
    TopicSpec("fraud.alerts", 3, 30 * 86_400_000, "Alert-created notifications (persistence consumer)"),
    TopicSpec("fraud.dlq", 1, 30 * 86_400_000, "Dead letters: malformed/late/failed messages with reason headers"),
    TopicSpec("features.windows", 3, 86_400_000, "Spark windowed per-customer aggregates (monitoring)"),
)


@dataclass(frozen=True)
class Topics:
    prefix: str = ""

    @classmethod
    def from_env(cls) -> Topics:
        return cls(os.environ.get("FRAUDSTREAM_TOPIC_PREFIX", ""))

    def name(self, base: str) -> str:
        return f"{self.prefix}{base}"

    raw = property(lambda self: self.name("transactions.raw"))
    validated = property(lambda self: self.name("transactions.validated"))
    normal = property(lambda self: self.name("transactions.normal"))
    suspicious = property(lambda self: self.name("transactions.suspicious"))
    alerts = property(lambda self: self.name("fraud.alerts"))
    dlq = property(lambda self: self.name("fraud.dlq"))
    windows = property(lambda self: self.name("features.windows"))

    def specs(self) -> list[TopicSpec]:
        return [TopicSpec(self.name(s.name), s.partitions, s.retention_ms, s.description) for s in TOPIC_SPECS]


DEFAULT_TOPICS = Topics.from_env()
# Backwards-compatible module constants (resolved with the env prefix at import time)
RAW, VALIDATED, NORMAL, SUSPICIOUS, ALERTS, DLQ, WINDOWS = (
    DEFAULT_TOPICS.raw, DEFAULT_TOPICS.validated, DEFAULT_TOPICS.normal, DEFAULT_TOPICS.suspicious,
    DEFAULT_TOPICS.alerts, DEFAULT_TOPICS.dlq, DEFAULT_TOPICS.windows)
TOPICS = tuple(DEFAULT_TOPICS.specs())
PROCESSOR_GROUP = "fraudstream-processor"
PERSISTENCE_GROUP = "fraudstream-persistence"


class KafkaUnavailableError(RuntimeError):
    pass


def producer_config(bootstrap: str, client_id: str) -> dict[str, Any]:
    return {"bootstrap.servers": bootstrap, "client.id": client_id, "acks": "all", "enable.idempotence": True,
            "linger.ms": 5, "compression.type": "lz4", "message.timeout.ms": 30000,
            "retries": 10, "retry.backoff.ms": 200}


def consumer_config(bootstrap: str, group_id: str, client_id: str) -> dict[str, Any]:
    return {"bootstrap.servers": bootstrap, "group.id": group_id, "client.id": client_id,
            "enable.auto.commit": False,          # commit only after side effects are durable
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest", "isolation.level": "read_committed",
            "session.timeout.ms": 30000, "max.poll.interval.ms": 300000,
            "partition.assignment.strategy": "cooperative-sticky"}


def make_producer(bootstrap: str, client_id: str = "fraudstream-producer"):  # type: ignore[no-untyped-def]
    from confluent_kafka import Producer

    return Producer(producer_config(bootstrap, client_id))


def check_kafka(bootstrap: str, timeout: float = 5.0) -> dict[str, Any]:
    """Return cluster metadata or raise KafkaUnavailableError (never a silent success)."""
    from confluent_kafka.admin import AdminClient

    try:
        md = AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": int(timeout * 1000)}).list_topics(timeout=timeout)
    except Exception as exc:  # noqa: BLE001
        raise KafkaUnavailableError(f"Kafka unreachable at {bootstrap}: {exc}") from exc
    return {"brokers": len(md.brokers), "topics": sorted(t for t in md.topics if not t.startswith("__"))}


def ensure_topics(bootstrap: str, topics: Topics | None = None, timeout: float = 15.0) -> list[str]:
    """Create any missing FraudStream topics. Returns the names created."""
    from confluent_kafka.admin import AdminClient, NewTopic

    topics = topics or DEFAULT_TOPICS
    admin = AdminClient({"bootstrap.servers": bootstrap})
    try:
        existing = set(admin.list_topics(timeout=timeout).topics)
    except Exception as exc:  # noqa: BLE001
        raise KafkaUnavailableError(f"Kafka unreachable at {bootstrap}: {exc}") from exc
    new = [NewTopic(t.name, num_partitions=t.partitions, replication_factor=1,
                    config={"retention.ms": str(t.retention_ms), "min.insync.replicas": "1"})
           for t in topics.specs() if t.name not in existing]
    created = []
    if new:
        for name, fut in admin.create_topics(new).items():
            try:
                fut.result(timeout)
                created.append(name)
            except Exception as exc:  # noqa: BLE001
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise
    return created


def delete_topics(bootstrap: str, names: list[str], timeout: float = 30.0) -> list[str]:
    """Used ONLY by integration tests to remove their own uniquely-prefixed test topics.
    Returns the names that could NOT be deleted (callers report them; nothing is hidden)."""
    from confluent_kafka.admin import AdminClient

    if not names:
        return []
    admin = AdminClient({"bootstrap.servers": bootstrap})   # must stay referenced until futures resolve
    failed = []
    for name, fut in admin.delete_topics(names, operation_timeout=timeout).items():
        try:
            fut.result(timeout)
        except Exception as exc:  # noqa: BLE001
            if "UNKNOWN_TOPIC_OR_PART" not in str(exc):
                failed.append(name)
                log.warning("could not delete test topic %s: %s", name, exc)
    return failed


def encode(value: dict[str, Any]) -> bytes:
    return json.dumps(value, separators=(",", ":"), default=str).encode()


def header_dict(msg: Any) -> dict[str, str]:
    return {k: (v.decode() if isinstance(v, bytes) else str(v)) for k, v in (msg.headers() or [])}


def main() -> None:
    """python -m streaming.kafka.client create-topics|check"""
    import sys

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    if cmd == "create-topics":
        created = ensure_topics(bootstrap)
        print(f"created: {created or 'none (all exist)'}")
    info = check_kafka(bootstrap)
    print(json.dumps(info, indent=2))


if __name__ == "__main__":
    main()
