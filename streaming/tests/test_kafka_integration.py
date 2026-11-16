"""Kafka integration tests against REAL Kafka + PostgreSQL + Redis (docker compose).

Run:  python -m pytest -m integration streaming/tests -v
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import func, select

from app.database.session import SessionLocal, init_engine
from app.models.entities import Alert, Prediction, Transaction
from app.services.simulation import KafkaSink
from app.streaming.persistence_consumer import PersistenceConsumer
from app.streaming.processor import StreamProcessor
from ml.features.engineering import build_feature_frame
from simulator.generator import SimulationConfig, TransactionGenerator
from streaming.kafka.client import (
    PERSISTENCE_GROUP,
    PROCESSOR_GROUP,
    KafkaUnavailableError,
    check_kafka,
    encode,
    header_dict,
    make_producer,
)
from streaming.tests.conftest import ART, committed_lag, read_topic, run_in_thread, wait_until

pytestmark = pytest.mark.integration


def _counts() -> tuple[int, int, int]:
    db = SessionLocal()
    try:
        return tuple(db.scalar(select(func.count(m.id))) or 0 for m in (Transaction, Prediction, Alert))  # type: ignore[return-value]
    finally:
        db.close()


def _events(n: int, seed: int, scenarios: tuple[str, ...] = ()) -> list:
    gen = TransactionGenerator(SimulationConfig(mode="mixed", tps=50, max_events=n, duration_seconds=None,
                                                customer_count=500, suspicious_pct=8, seed=seed), datetime.now(timezone.utc))
    evs = list(gen.events())
    for s in scenarios:   # appended AFTER the stream in event time, so produce order == event-time order
        evs += gen.inject(s, when=evs[-1].event_time + timedelta(seconds=1), align="start")
    return evs


def _produce(bootstrap: str, topic: str, events: list) -> None:
    sink = KafkaSink(make_producer(bootstrap, "it-producer"), topic)
    for ev in events:
        sink.send(ev)
    assert sink.flush(30) == 0 and sink.failed == 0, sink.last_error
    assert sink.delivered == len(events)


def _process(bootstrap, topics, expect: int, group: str = PROCESSOR_GROUP, **kw) -> StreamProcessor:
    proc = StreamProcessor(bootstrap, ART, topics=topics, group_id=group, client_id=f"it-proc-{group}", **kw)
    t, box = run_in_thread(lambda: proc.run(idle_exit_seconds=8))
    assert wait_until(lambda: proc.stats.consumed >= expect, 90), f"processor consumed {proc.stats.consumed}/{expect}"
    proc.stop()
    t.join(30)
    assert "error" not in box, box.get("error")
    return proc


def _persist(bootstrap, topics, expect: int, group: str = PERSISTENCE_GROUP, redis_client=None, **kw) -> PersistenceConsumer:
    c = PersistenceConsumer(bootstrap, topics=topics, group_id=group, redis_client=redis_client,
                            client_id=f"it-persist-{group}", **kw)
    t, box = run_in_thread(lambda: c.run(idle_exit_seconds=8))
    assert wait_until(lambda: c.stats.consumed >= expect, 90), f"consumer consumed {c.stats.consumed}/{expect}"
    c.stop()
    t.join(30)
    assert "error" not in box, box.get("error")
    return c


# ---------------------------------------------------------------------------------------------
def test_topics_exist_with_expected_partitions(bootstrap, topics):
    from confluent_kafka.admin import AdminClient

    md = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10)
    for spec in topics.specs():
        assert spec.name in md.topics, spec.name
        assert len(md.topics[spec.name].partitions) == spec.partitions


def test_end_to_end_ingestion_processing_persistence(bootstrap, topics, database_url, redis_client):
    """simulator -> transactions.raw -> processor (features+ML+risk) -> scored topics -> persistence -> PostgreSQL."""
    evs = _events(400, seed=101, scenarios=("account_takeover", "velocity_burst", "large_amount"))
    n = len(evs)
    before = _counts()
    _produce(bootstrap, topics.raw, evs)
    proc = _process(bootstrap, topics, n, group="it-e2e-proc")
    assert proc.stats.valid == n and proc.stats.invalid == 0 and proc.stats.model_unavailable == 0
    scored_total = proc.stats.to_normal + proc.stats.to_suspicious
    assert scored_total == n and proc.stats.to_suspicious > 0

    pubsub = redis_client.pubsub()
    pubsub.subscribe("fraudstream:live")
    cons = _persist(bootstrap, topics, n, group="it-e2e-persist", redis_client=redis_client)
    after = _counts()
    assert after[0] - before[0] == n and after[1] - before[1] == n
    assert cons.stats.created == n and cons.stats.dead_lettered == 0
    assert after[2] - before[2] == cons.stats.alerts == proc.stats.to_suspicious

    # every scored event reached Kafka topics; alert notifications emitted once per alert
    assert len(read_topic(bootstrap, topics.validated, n)) >= n
    alerts_msgs = [json.loads(m.value()) for m in read_topic(bootstrap, topics.alerts, cons.stats.alerts)]
    assert len(alerts_msgs) == cons.stats.alerts and all(a["type"] == "alert_created" for a in alerts_msgs)
    # offsets committed -> no lag in either consumer group
    assert committed_lag(bootstrap, "it-e2e-proc", [topics.raw]) == 0
    assert committed_lag(bootstrap, "it-e2e-persist", [topics.normal, topics.suspicious]) == 0

    # integration with the shared feature engineering & model: stored features == offline recomputation
    offline = build_feature_frame(evs).set_index("meta_event_id")
    db = SessionLocal()
    try:
        rows = db.execute(select(Transaction.event_id, Prediction.features, Prediction.model_score, Transaction.profile,
                                 Transaction.pipeline_latency_ms)
                          .join(Prediction).where(Transaction.event_id.in_([e.event_id for e in evs]))).all()
    finally:
        db.close()
    assert len(rows) == n
    for event_id, feats, score, profile, latency in rows:
        assert profile == "distributed" and score is not None and latency is not None and latency >= 0
        ref = offline.loc[event_id]
        for k in ("tx_count_1m", "tx_count_1h", "amount", "is_new_device", "hist_tx_count", "merchant_category"):
            assert ref[k] == pytest.approx(feats[k]) if isinstance(feats[k], float) else ref[k] == feats[k], (event_id, k)

    # live feed published to Redis for the dashboard
    live = []
    for _ in range(200):
        m = pubsub.get_message(timeout=0.05)
        if m and m["type"] == "message":
            live.append(json.loads(m["data"]))
    pubsub.close()
    assert live and all(x["type"] == "transaction" for x in live)
    hb = json.loads(redis_client.get("fraudstream:consumer:heartbeat"))
    assert hb["stats"]["created"] == n
    redis_client.delete("fraudstream:consumer:heartbeat", "fraudstream:processor:heartbeat")


def test_invalid_payloads_are_dead_lettered_not_persisted(bootstrap, topics, database_url):
    good = _events(1, seed=202)[0].to_wire()
    bad = [b"{not json", b"[]", b"", json.dumps({**good, "amount": -5}).encode(),
           json.dumps({**good, "merchant_category": "weapons"}).encode(), json.dumps({"event_id": "only"}).encode()]
    dlq_before = len(read_topic(bootstrap, topics.dlq, timeout=5))
    p = make_producer(bootstrap, "it-bad")
    for b in bad:
        p.produce(topics.raw, value=b, key=b"bad")
    assert p.flush(20) == 0
    before = _counts()
    proc = _process(bootstrap, topics, len(bad), group="it-invalid-proc")
    assert proc.stats.invalid == len(bad) and proc.stats.valid == 0
    dlq = read_topic(bootstrap, topics.dlq, dlq_before + len(bad))
    new = dlq[dlq_before:]
    assert len(new) == len(bad)
    reasons = [header_dict(m)["dlq_reason"] for m in new]
    assert sum(r.startswith("MALFORMED_JSON") for r in reasons) == 2      # "{not json", "[]"
    assert sum(r.startswith("EMPTY_PAYLOAD") for r in reasons) == 1       # b""
    assert sum(r.startswith("SCHEMA_VIOLATION") for r in reasons) == 3    # negative amount, bad category, missing fields
    assert all(header_dict(m)["source_topic"] == topics.raw for m in new)
    assert _counts() == before
    assert committed_lag(bootstrap, "it-invalid-proc", [topics.raw]) == 0


def test_duplicate_events_produce_no_duplicate_records(bootstrap, topics, database_url):
    evs = _events(60, seed=303, scenarios=("velocity_burst",))
    n = len(evs)
    _produce(bootstrap, topics.raw, evs)
    _produce(bootstrap, topics.raw, evs)   # genuine broker-level duplicates (second producer session)
    proc = _process(bootstrap, topics, 2 * n, group="it-dup-proc")
    assert proc.stats.valid == n and proc.stats.duplicates == n
    before = _counts()
    _persist(bootstrap, topics, n, group="it-dup-persist")
    after = _counts()
    assert after[0] - before[0] == n
    # duplicate SCORED messages (e.g. a processor replay) are absorbed by the persistence layer
    dup_msgs = read_topic(bootstrap, topics.suspicious, timeout=8)
    assert dup_msgs
    p = make_producer(bootstrap, "it-dup-replay")
    for m in dup_msgs:
        p.produce(topics.suspicious, key=m.key(), value=m.value())
    assert p.flush(20) == 0
    cons = _persist(bootstrap, topics, len(dup_msgs), group="it-dup-persist")
    assert cons.stats.created == 0 and cons.stats.duplicates == len(dup_msgs)
    assert _counts() == after


def test_consumer_crash_before_commit_redelivers_idempotently(bootstrap, topics, database_url):
    evs = _events(80, seed=404, scenarios=("account_takeover", "failed_attempts"))
    n = len(evs)
    _produce(bootstrap, topics.raw, evs)
    _process(bootstrap, topics, n, group="it-crash-proc")
    before = _counts()
    # 1st consumer persists everything but "crashes" before committing offsets
    c1 = _persist(bootstrap, topics, n, group="it-crash-persist", commit_offsets=False)
    mid = _counts()
    assert mid[0] - before[0] == n and c1.stats.created == n
    assert committed_lag(bootstrap, "it-crash-persist", [topics.normal, topics.suspicious]) > 0
    alerts_topic_before = len(read_topic(bootstrap, topics.alerts, timeout=8))
    # restarted consumer (same group) receives the SAME messages again
    c2 = _persist(bootstrap, topics, n, group="it-crash-persist")
    assert c2.stats.consumed >= n and c2.stats.created == 0 and c2.stats.duplicates >= n
    assert _counts() == mid                                         # no duplicate tx / predictions / alerts
    assert len(read_topic(bootstrap, topics.alerts, timeout=8)) == alerts_topic_before   # no duplicate notifications
    assert committed_lag(bootstrap, "it-crash-persist", [topics.normal, topics.suspicious]) == 0


def test_processor_restart_resumes_from_committed_offsets(bootstrap, topics, database_url):
    evs = _events(120, seed=505)
    _produce(bootstrap, topics.raw, evs[:60])
    p1 = _process(bootstrap, topics, 60, group="it-restart-proc")
    assert p1.stats.valid == 60
    _produce(bootstrap, topics.raw, evs[60:])
    p2 = _process(bootstrap, topics, len(evs) - 60, group="it-restart-proc")
    assert p2.stats.consumed == len(evs) - 60          # only the new records, nothing re-processed
    assert committed_lag(bootstrap, "it-restart-proc", [topics.raw]) == 0


def test_database_outage_blocks_commit_and_loses_nothing(bootstrap, topics, database_url):
    evs = _events(30, seed=606)
    n = len(evs)
    _produce(bootstrap, topics.raw, evs)
    _process(bootstrap, topics, n, group="it-dbdown-proc")
    before = _counts()
    init_engine("postgresql+psycopg://nobody:nopass@127.0.0.1:1/none?connect_timeout=1")   # unreachable DB
    c = PersistenceConsumer(bootstrap, topics=topics, group_id="it-dbdown-persist", client_id="it-dbdown",
                            max_db_backoff=0.5)
    t, box = run_in_thread(lambda: c.run(idle_exit_seconds=30))
    assert wait_until(lambda: c.stats.db_retries >= 3, 30)
    c.stop()
    t.join(30)
    init_engine(database_url)
    assert c.stats.created == 0 and c.stats.dead_lettered == 0       # nothing skipped or dead-lettered
    assert committed_lag(bootstrap, "it-dbdown-persist", [topics.normal, topics.suspicious]) >= n
    c2 = _persist(bootstrap, topics, n, group="it-dbdown-persist")  # database back -> everything persisted
    assert c2.stats.created == n and _counts()[0] - before[0] == n


def test_persistence_dead_letters_poison_messages_and_continues(bootstrap, topics, database_url):
    dlq_before = len(read_topic(bootstrap, topics.dlq, timeout=5))
    p = make_producer(bootstrap, "it-poison")
    p.produce(topics.normal, value=b"not-json", key=b"x")
    p.produce(topics.normal, value=encode({"event": {"event_id": "x"}, "decision": {}, "features": {}}), key=b"x")
    assert p.flush(20) == 0
    c = _persist(bootstrap, topics, 2, group="it-poison-persist")
    assert c.stats.dead_lettered == 2
    new = read_topic(bootstrap, topics.dlq, dlq_before + 2)[dlq_before:]
    assert {header_dict(m)["dlq_stage"] for m in new} == {"persistence"}
    assert committed_lag(bootstrap, "it-poison-persist", [topics.normal]) == 0


def _redis_db15() -> str:
    import os

    base = os.environ["REDIS_URL"].rsplit("/", 1)[0]
    return f"{base}/15"


@pytest.fixture
def artifacts_copy(tmp_path):
    """The API writes runtime_config.json into its artifacts dir: give it a COPY, never the real one."""
    import shutil

    dst = tmp_path / "artifacts"
    shutil.copytree(ART, dst)
    return dst


def test_kafka_unavailable_is_reported_never_silent(database_url, monkeypatch, artifacts_copy):
    with pytest.raises(KafkaUnavailableError):
        check_kafka("127.0.0.1:1", timeout=2)
    # API in DISTRIBUTED profile with Kafka down: ingest and simulation return 503, nothing persisted
    from fastapi.testclient import TestClient

    from app.cli import _create
    from app.core.config import Settings, get_settings
    from app.main import create_app

    monkeypatch.setenv("FRAUDSTREAM_ENV", "test")
    monkeypatch.setenv("FRAUDSTREAM_PROFILE", "distributed")
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:1")
    monkeypatch.setenv("REDIS_URL", _redis_db15())
    monkeypatch.setenv("JWT_SECRET", "it-secret-" + "y" * 40)
    monkeypatch.setenv("FRAUDSTREAM_ARTIFACTS_DIR", str(artifacts_copy))
    get_settings.cache_clear()
    settings = Settings()
    assert settings.profile == "distributed"
    before = _counts()
    with TestClient(create_app(settings)) as client:
        _create("it_admin", "ADMIN", "It-Admin-Pass-123!")
        tok = client.post("/api/v1/auth/login", json={"username": "it_admin", "password": "It-Admin-Pass-123!"}).json()["access_token"]
        h = {"Authorization": f"Bearer {tok}"}
        ev = _events(1, seed=707)[0].to_wire()
        r = client.post("/api/v1/transactions/ingest", headers=h, json=ev)
        assert r.status_code == 503 and "Kafka" in r.json()["detail"]
        r = client.post("/api/v1/simulation/start", headers=h, json={"tps": 5, "duration_seconds": 5})
        assert r.status_code == 503
        infra = client.get("/api/v1/infrastructure/status", headers=h).json()
        assert infra["profile"] == "distributed" and infra["kafka"]["status"] == "DOWN"
        assert client.get("/api/v1/system/status", headers=h).json()["profile"] == "distributed"
    get_settings.cache_clear()
    init_engine(database_url)
    assert _counts() == before


def test_distributed_api_ingest_flows_through_kafka(bootstrap, topics, database_url, redis_client, monkeypatch, artifacts_copy):
    """API (distributed profile) -> Kafka -> processor -> persistence -> API reads it back."""
    from fastapi.testclient import TestClient

    from app.cli import _create
    from app.core.config import Settings, get_settings
    from app.main import create_app

    for k, v in {"FRAUDSTREAM_ENV": "test", "FRAUDSTREAM_PROFILE": "distributed", "DATABASE_URL": database_url,
                 "KAFKA_BOOTSTRAP_SERVERS": bootstrap, "REDIS_URL": _redis_db15(), "FRAUDSTREAM_ARTIFACTS_DIR": str(artifacts_copy),
                 "JWT_SECRET": "it-secret-" + "z" * 40, "FRAUDSTREAM_TOPIC_PREFIX": topics.prefix}.items():
        monkeypatch.setenv(k, v)
    get_settings.cache_clear()
    settings = Settings()
    evs = _events(5, seed=808, scenarios=("large_amount",))
    with TestClient(create_app(settings)) as client:
        _create("it_analyst", "ANALYST", "It-Analyst-Pass-123!")
        tok = client.post("/api/v1/auth/login", json={"username": "it_analyst", "password": "It-Analyst-Pass-123!"}).json()["access_token"]
        h = {"Authorization": f"Bearer {tok}"}
        for ev in evs:
            r = client.post("/api/v1/transactions/ingest", headers=h, json=ev.to_wire())
            assert r.status_code == 202 and r.json()["topic"] == topics.raw
        _process(bootstrap, topics, len(evs), group="it-api-proc")
        _persist(bootstrap, topics, len(evs), group="it-api-persist")
        found = client.get(f"/api/v1/transactions?q={evs[-1].transaction_id}", headers=h).json()
        assert found["total"] == 1 and found["items"][0]["profile"] == "distributed"
        infra = client.get("/api/v1/infrastructure/status", headers=h).json()
        assert infra["kafka"]["status"] == "UP"
        assert any(t["name"] == topics.raw and t["end_offset_total"] >= len(evs) for t in infra["kafka"]["topics"])
        groups = {g["group"]: g for g in infra["kafka"]["consumer_groups"]}
        assert set(groups) == {PROCESSOR_GROUP, PERSISTENCE_GROUP}
    get_settings.cache_clear()
    init_engine(database_url)


@pytest.mark.slow
def test_acknowledged_records_survive_broker_restart(bootstrap, topics):
    """Restart the Kafka container; every record acknowledged before the restart must still be there."""
    # Phase 12: the compose project is selectable (SPARK_IT_COMPOSE_PROJECT, default = the repository's project)
    project = os.environ.get("SPARK_IT_COMPOSE_PROJECT", "fraudstream")
    container = subprocess.run(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={project}",
                                "--filter", "label=com.docker.compose.service=kafka"], capture_output=True, text=True,
                               cwd=Path(__file__).resolve().parents[2]).stdout.strip()
    if not container:
        pytest.skip("NOT EXECUTED: kafka container not managed by this compose project")
    topic = topics.name("durability.check")
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})   # keep a reference until the future resolves
    admin.create_topics([NewTopic(topic, 3, 1)])[topic].result(15)
    p = make_producer(bootstrap, "it-durable")
    acked: list[bytes] = []
    for i in range(500):
        v = f"record-{i}".encode()
        p.produce(topic, value=v, key=str(i % 7).encode(), on_delivery=lambda e, m: acked.append(m.value()) if e is None else None)
    assert p.flush(30) == 0 and len(acked) == 500
    subprocess.run(["docker", "restart", container], check=True, capture_output=True, timeout=120)
    assert wait_until(lambda: _kafka_up(bootstrap), 120, 2), "kafka did not come back"
    got = read_topic(bootstrap, topic, 500, timeout=60)
    assert sorted(m.value() for m in got) == sorted(acked)


def _kafka_up(bootstrap: str) -> bool:
    try:
        check_kafka(bootstrap, timeout=3)
        return True
    except KafkaUnavailableError:
        return False


def test_processor_reports_itself_as_non_spark(bootstrap, topics, database_url):
    evs = _events(3, seed=909)
    _produce(bootstrap, topics.raw, evs)
    _process(bootstrap, topics, 3, group="it-label-proc")
    msgs = read_topic(bootstrap, topics.normal, timeout=8) + read_topic(bootstrap, topics.suspicious, timeout=4)
    assert msgs and all(json.loads(m.value())["processor"] == "python-kafka" for m in msgs)
    assert time.time() > 0
