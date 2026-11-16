"""Live Kafka / Spark status for the Streaming Infrastructure page (DISTRIBUTED profile).
Every value is read from the running system; failures produce explicit DOWN/UNKNOWN states."""
from __future__ import annotations

import json
import threading
import time
from typing import Any

from streaming.kafka.client import PERSISTENCE_GROUP, PROCESSOR_GROUP, Topics

_last: dict[str, tuple[float, int]] = {}
_lock = threading.Lock()


def kafka_status(bootstrap: str, timeout: float = 4.0) -> dict[str, Any]:
    from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient

    tp = Topics.from_env()
    RAW, DLQ = tp.raw, tp.dlq
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": int(timeout * 1000)})
        md = admin.list_topics(timeout=timeout)
    except Exception as exc:  # noqa: BLE001
        return {"status": "DOWN", "error": f"{exc.__class__.__name__}: {exc}"[:300]}
    probe = Consumer({"bootstrap.servers": bootstrap, "group.id": "fraudstream-status-probe",
                      "enable.auto.commit": False})
    topics: list[dict[str, Any]] = []
    ends: dict[tuple[str, int], int] = {}
    now = time.time()
    try:
        for spec in tp.specs():
            t = md.topics.get(spec.name)
            if t is None or t.error is not None:
                topics.append({"name": spec.name, "exists": False, "description": spec.description})
                continue
            total, low_total = 0, 0
            for pid in t.partitions:
                lo, hi = probe.get_watermark_offsets(TopicPartition(spec.name, pid), timeout=timeout)
                ends[(spec.name, pid)] = hi
                total += hi
                low_total += lo
            with _lock:
                prev = _last.get(spec.name)
                _last[spec.name] = (now, total)
            rate = (total - prev[1]) / (now - prev[0]) if prev and now > prev[0] else None
            topics.append({"name": spec.name, "exists": True, "partitions": len(t.partitions), "end_offset_total": total,
                           "messages_retained": total - low_total, "rate_per_sec_since_last_check": rate,
                           "description": spec.description})
        groups = []
        for group in (PROCESSOR_GROUP, PERSISTENCE_GROUP):
            try:
                fut = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])
                res = fut[group].result(timeout=timeout)
                lag_by_topic: dict[str, int] = {}
                for tpp in res.topic_partitions:
                    if tpp.offset is None or tpp.offset < 0:
                        continue
                    lag_by_topic[tpp.topic] = lag_by_topic.get(tpp.topic, 0) + max(0, ends.get((tpp.topic, tpp.partition), 0) - tpp.offset)
                state = "UNKNOWN"
                try:
                    desc = admin.describe_consumer_groups([group])[group].result(timeout=timeout)
                    state = desc.state.name if hasattr(desc.state, "name") else str(desc.state)
                    members = len(desc.members)
                except Exception:  # noqa: BLE001
                    members = None
                groups.append({"group": group, "state": state, "members": members,
                               "active": bool(members), "lag_by_topic": lag_by_topic,
                               "total_lag": sum(lag_by_topic.values()), "committed_partitions": len(res.topic_partitions)})
            except Exception as exc:  # noqa: BLE001
                groups.append({"group": group, "error": f"{exc.__class__.__name__}"})
    finally:
        probe.close()
    dlq = next((t for t in topics if t["name"] == DLQ), {})
    return {"status": "UP", "brokers": len(md.brokers), "topics": topics, "consumer_groups": groups,
            "dlq_messages": dlq.get("messages_retained"), "raw_end_offsets": {
                f"{t}:{p}": o for (t, p), o in ends.items() if t == RAW}}


def spark_status(redis_client: Any, kafka: dict[str, Any]) -> dict[str, Any]:
    if redis_client is None:
        return {"status": "UNKNOWN", "unavailable_reason": "Redis not configured"}
    try:
        raw = redis_client.get("fraudstream:spark:progress")
    except Exception as exc:  # noqa: BLE001
        return {"status": "UNKNOWN", "unavailable_reason": f"redis error {exc.__class__.__name__}"}
    if not raw:
        return {"status": "NOT_REPORTING", "unavailable_reason": "Spark job has not published progress (is it running?)"}
    prog = json.loads(raw)
    age = time.time() - prog.get("reported_at", 0)
    prog["age_seconds"] = round(age, 1)
    if prog.get("terminated"):
        prog["status"] = "STOPPED"
    elif any(q.get("exception") for q in prog.get("queries", [])):
        prog["status"] = "FAILED"
    else:
        prog["status"] = "RUNNING" if age < 30 else "STALE"
    # Spark's Kafka source tracks offsets in its checkpoint, not a consumer group; lag is
    # computed from the latest processed end offsets vs current topic end offsets.
    RAW = Topics.from_env().raw
    raw_ends = kafka.get("raw_end_offsets") or {}
    for q in prog.get("queries", []):
        end = q.get("source_end_offsets") or {}
        if raw_ends and isinstance(end, dict) and RAW in end:
            lag = sum(max(0, raw_ends.get(f"{RAW}:{p}", 0) - int(o)) for p, o in end[RAW].items())
            q["lag_vs_raw_topic"] = lag
    return prog


def heartbeat(redis_client: Any, key: str, max_age: float = 30.0) -> dict[str, Any]:
    if redis_client is None:
        return {"status": "UNKNOWN", "unavailable_reason": "Redis not configured"}
    try:
        raw = redis_client.get(key)
    except Exception as exc:  # noqa: BLE001
        return {"status": "UNKNOWN", "unavailable_reason": f"redis error {exc.__class__.__name__}"}
    if not raw:
        return {"status": "NO_HEARTBEAT", "unavailable_reason": "process has not reported"}
    hb = json.loads(raw)
    age = time.time() - hb.get("ts", 0)
    hb["age_seconds"] = round(age, 1)
    hb["status"] = "UP" if age < max_age else "STALE"
    return hb


def detect_stream_processor(redis_client: Any) -> str:
    """Name the stream processor that is ACTUALLY reporting (never assumed)."""
    spark = spark_status(redis_client, {})
    if spark.get("status") == "RUNNING":
        return "Spark Structured Streaming"
    if spark.get("status") == "UNKNOWN" and redis_client is not None:
        # Phase 12: heartbeats live in Redis. With Redis unreachable the processor may well be running (Spark keeps
        # scoring from Kafka) - reporting "NOT RUNNING" would be false, so the state is reported as unknown.
        return "UNKNOWN (Redis unavailable: processor heartbeats cannot be read)"
    proc = heartbeat(redis_client, "fraudstream:processor:heartbeat")
    if proc.get("status") == "UP":
        return "python-kafka processor (non-Spark)"
    return "NOT RUNNING (no processor heartbeat)"
