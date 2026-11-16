"""Spark Structured Streaming integration tests against REAL Kafka + Spark (Docker) + PostgreSQL.

    python -m pytest -m "integration and spark" streaming/tests/test_spark_integration.py -v

The Spark job runs in the ``fraudstream-spark:local`` image on the compose network with an
isolated topic prefix and its own checkpoint volume (both removed by teardown). Source code
and model artifacts are mounted READ-ONLY.
"""
from __future__ import annotations

import json
import os
import secrets
import subprocess
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from app.services.simulation import KafkaSink
from app.streaming.persistence_consumer import PersistenceConsumer
from ml.features.engineering import build_feature_frame
from simulator.generator import SimulationConfig, TransactionGenerator
from streaming.kafka.client import Topics, delete_topics, ensure_topics, header_dict, make_producer
from streaming.tests.conftest import run_in_thread, wait_until

pytestmark = [pytest.mark.integration, pytest.mark.spark]
ROOT = Path(__file__).resolve().parents[2]
IMAGE = "fraudstream-spark:local"
# Phase 12: target selection (defaults = the live compose project, as before); e.g. the isolated stack:
#   SPARK_IT_NETWORK=fraudstream-p12 SPARK_IT_COMPOSE_PROJECT=fraudstream-p12
NETWORK = os.environ.get("SPARK_IT_NETWORK", "fraudstream")
COMPOSE_PROJECT = os.environ.get("SPARK_IT_COMPOSE_PROJECT", "fraudstream")
# only the source directories the job needs (not the whole repository, which contains .env)
SOURCE_MOUNTS = [a for d in ("backend", "ml", "streaming", "simulator")
                 for a in ("-v", f"{(ROOT / d).as_posix()}:/app/{d}:ro")]


def _docker(*args: str, check: bool = True, timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check, timeout=timeout, cwd=ROOT)


class SparkJob:
    def __init__(self, topics: Topics, redis_url: str, env: dict[str, str] | None = None) -> None:
        tag = topics.prefix.strip(".").replace(".", "-")
        self.name = f"fs-spark-{tag}"
        self.volume = f"fs-spark-ckpt-{tag}"
        self.topics = topics
        self.redis_url = redis_url
        self.env = {"TRIGGER_INTERVAL": "1 second", "WATERMARK_DELAY": "1 minute", "WINDOWS": "tumbling_1m,sliding_5m",
                    "SPARK_MASTER": "local[4]", **(env or {})}

    def start(self) -> None:
        _docker("volume", "create", self.volume)
        _docker("run", "-d", "--name", self.name, "--network", NETWORK, "--memory", "3g",
                "-e", "KAFKA_BOOTSTRAP_SERVERS=kafka:29092", "-e", f"FRAUDSTREAM_TOPIC_PREFIX={self.topics.prefix}",
                "-e", f"REDIS_URL={self.redis_url}", "-e", "CHECKPOINT_DIR=/checkpoints",
                *[a for k, v in self.env.items() for a in ("-e", f"{k}={v}")],
                "-e", "PROGRESS_FILE=/checkpoints/progress.json",
                *SOURCE_MOUNTS, "-v", f"{self.volume}:/checkpoints", IMAGE)

    def restart_container(self) -> None:
        _docker("start", self.name)

    def running(self) -> bool:
        r = _docker("inspect", "-f", "{{.State.Running}}", self.name, check=False)
        return r.stdout.strip() == "true"

    def progress(self) -> dict[str, Any] | None:
        r = _docker("exec", self.name, "cat", "/checkpoints/progress.json", check=False)
        try:
            return json.loads(r.stdout) if r.returncode == 0 and r.stdout else None
        except json.JSONDecodeError:
            return None

    def wait_ready(self, timeout: float = 240, not_app_id: str | None = None) -> dict[str, Any]:
        """Ready = a progress snapshot from a (new) Spark application in which every query ran a batch."""
        def ready() -> bool:
            p = self.progress()
            return (bool(p) and p.get("app_id") != not_app_id and not p.get("terminated")
                    and all(q.get("batch_id") is not None for q in p["queries"]))
        if not wait_until(ready, timeout, 3):
            raise AssertionError(f"spark job not ready; logs:\n{self.logs()[-4000:]}")
        return self.progress() or {}

    def logs(self) -> str:
        r = _docker("logs", self.name, check=False)
        return r.stdout + r.stderr

    def ls(self, path: str) -> list[str]:
        r = _docker("exec", self.name, "ls", path, check=False)
        return r.stdout.split()

    def remove(self) -> None:
        _docker("rm", "-f", self.name, check=False)
        _docker("volume", "rm", "-f", self.volume, check=False)   # only this test's checkpoint volume


@pytest.fixture(scope="module")
def spark_env(bootstrap: str) -> Iterator[tuple[Topics, SparkJob]]:
    if _docker("image", "inspect", IMAGE, check=False).returncode != 0:
        pytest.skip(f"NOT EXECUTED: image {IMAGE} not built")
    redis_url = os.environ.get("SPARK_IT_REDIS_URL")
    if not redis_url:
        pytest.skip("NOT EXECUTED: SPARK_IT_REDIS_URL (redis URL reachable from the compose network) not set")
    topics = Topics(f"it-{secrets.token_hex(4)}.")
    ensure_topics(bootstrap, topics)
    job = SparkJob(topics, redis_url)
    job.start()
    try:
        job.wait_ready()
        yield topics, job
    finally:
        log_path = Path(os.environ.get("SPARK_IT_LOG_DIR", ROOT / "streaming" / "tests")) / "spark-it-last.log"
        try:
            log_path.write_text(job.logs()[-200_000:], encoding="utf-8")
        except OSError:
            pass
        job.remove()
        from confluent_kafka.admin import AdminClient

        mine = [n for n in AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10).topics
                if n.startswith(topics.prefix)]
        assert not delete_topics(bootstrap, mine)


def _events(n: int, seed: int, customers: int = 300, start: datetime | None = None, scenarios: tuple[str, ...] = ()) -> list:
    gen = TransactionGenerator(SimulationConfig(mode="mixed", tps=40, max_events=n, duration_seconds=None,
                                                customer_count=customers, suspicious_pct=8, seed=seed,
                                                population_seed=seed * 7), start or datetime.now(timezone.utc))
    evs = list(gen.events())
    for s in scenarios:
        evs += gen.inject(s, when=evs[-1].event_time + timedelta(seconds=1), align="start")
    # tests share one Spark job (one state store): namespace customer ids per test seed
    return [e.model_copy(update={"customer_id": f"S{seed}-{e.customer_id}", "account_id": f"S{seed}-{e.account_id}"})
            for e in evs]


def _produce(bootstrap: str, topic: str, events: list) -> None:
    sink = KafkaSink(make_producer(bootstrap, "spark-it"), topic)
    for ev in events:
        sink.send(ev)
    assert sink.flush(30) == 0 and sink.failed == 0


class Collector:
    """One long-lived consumer that accumulates every record of the given topics (no re-reads)."""

    def __init__(self, bootstrap: str, topic_names: list[str]) -> None:
        from confluent_kafka import Consumer

        self.c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"spark-it-col-{secrets.token_hex(4)}",
                           "auto.offset.reset": "earliest", "enable.auto.commit": False})
        self.c.subscribe(topic_names)
        self.records: list[Any] = []

    def pump(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            for m in self.c.consume(500, 0.5):
                if not m.error():
                    self.records.append(m)

    def close(self) -> None:
        self.c.close()


def _scored(bootstrap: str, topics: Topics, ids: set[str], timeout: float = 150, settle: float = 6) -> dict[str, list[dict]]:
    """Collect scored output records for the given event ids (all copies, to detect duplicates)."""
    col = Collector(bootstrap, [topics.normal, topics.suspicious])
    found: dict[str, list[dict]] = {}
    seen = 0
    try:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            col.pump(2)
            for m in col.records[seen:]:
                v = json.loads(m.value())
                if v["event"]["event_id"] in ids:
                    found.setdefault(v["event"]["event_id"], []).append(v)
            seen = len(col.records)
            if set(found) >= ids:
                break
        col.pump(settle)                      # keep listening: any duplicate copies would show up here
        for m in col.records[seen:]:
            v = json.loads(m.value())
            if v["event"]["event_id"] in ids:
                found.setdefault(v["event"]["event_id"], []).append(v)
    finally:
        col.close()
    return found


def _collect(bootstrap: str, topic: str, until=None, timeout: float = 90, settle: float = 4) -> list[Any]:
    """Accumulate records of one topic with a long-lived consumer until `until(records)` is true."""
    col = Collector(bootstrap, [topic])
    try:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            col.pump(2)
            if until is not None and until(col.records):
                break
        col.pump(settle)
        return list(col.records)
    finally:
        col.close()


def _dlq(bootstrap: str, topics: Topics) -> list[Any]:
    return _collect(bootstrap, topics.dlq, timeout=8, settle=3)


# ------------------------------------------------------------------------------------------------
def test_spark_scores_events_and_feeds_persistence(bootstrap, spark_env, database_url):
    topics, job = spark_env
    # naturally generated, time-ordered suspicious traffic (no future-dated injections: they would
    # legitimately advance the event-time watermark of the shared job)
    evs = _events(250, seed=11)
    ids = {e.event_id for e in evs}
    _produce(bootstrap, topics.raw, evs)
    found = _scored(bootstrap, topics, ids)
    assert set(found) == ids, f"missing {len(ids - set(found))} scored events"
    assert all(len(v) == 1 for v in found.values()), "duplicate scored outputs"
    sample = next(iter(found.values()))[0]
    assert sample["processor"] == "spark-structured-streaming" and sample["decision"]["model_version"]
    # features computed inside Spark's stateful operator == offline shared feature module
    offline = build_feature_frame(evs).set_index("meta_event_id")
    for eid, (v,) in found.items():
        for k in ("hist_tx_count", "tx_count_1m", "tx_count_1h", "is_new_device", "amount_zscore", "merchant_category"):
            ref, got = offline.loc[eid][k], v["features"][k]
            assert (ref == pytest.approx(got)) if isinstance(got, float) else ref == got, (eid, k)
    assert sum(1 for v in found.values() if v[0]["decision"]["risk_category"] != "LOW") > 0
    # validated topic (watermark dedup) carries every valid event exactly once
    def has_all(recs: list[Any]) -> bool:
        return ids <= {header_dict(m).get("event_id") for m in recs}
    validated = [m for m in _collect(bootstrap, topics.validated, has_all) if header_dict(m).get("event_id") in ids]
    assert len(validated) == len(ids)
    # window aggregates are produced
    wins = [json.loads(m.value()) for m in _collect(bootstrap, topics.windows, lambda r: len(r) > 0)]
    assert {"tumbling_1m", "sliding_5m"} <= {w["window_type"] for w in wins}
    assert all(w["tx_count"] >= 1 for w in wins)
    # the EXISTING persistence consumer works unchanged on Spark output
    from sqlalchemy import func, select

    from app.database.session import SessionLocal
    from app.models.entities import Prediction, Transaction
    c = PersistenceConsumer(bootstrap, topics=topics, group_id="spark-it-persist", client_id="spark-it-persist")
    t, box = run_in_thread(lambda: c.run(idle_exit_seconds=8))
    assert wait_until(lambda: c.stats.consumed >= len(ids), 120)
    c.stop()
    t.join(30)
    assert "error" not in box and c.stats.created == len(ids) and c.stats.dead_lettered == 0
    db = SessionLocal()
    try:
        n = db.scalar(select(func.count(Transaction.id)).where(Transaction.event_id.in_(ids)))
        procs = db.execute(select(Prediction.model_version).join(Transaction).where(Transaction.event_id.in_(ids))).all()
    finally:
        db.close()
    assert n == len(ids) and all(p[0] for p in procs)


def test_spark_dead_letters_malformed_events(bootstrap, spark_env):
    topics, _ = spark_env
    before = len(_dlq(bootstrap, topics))
    good = _events(1, seed=21)[0].to_wire()
    bad = [b"{not json", b"", json.dumps({**good, "amount": -3}).encode(), json.dumps({**good, "currency": "usd"}).encode(),
           json.dumps({**good, "unexpected_field": 1}).encode()]
    p = make_producer(bootstrap, "spark-it-bad")
    for b in bad:
        p.produce(topics.raw, value=b, key=b"bad")
    assert p.flush(20) == 0
    assert wait_until(lambda: len(_dlq(bootstrap, topics)) >= before + len(bad), 90, 3)
    new = _dlq(bootstrap, topics)[before:]
    reasons = sorted(header_dict(m)["dlq_reason"].split(":")[0] for m in new)
    assert reasons == ["EMPTY_PAYLOAD", "MALFORMED_JSON", "SCHEMA_VIOLATION", "SCHEMA_VIOLATION", "SCHEMA_VIOLATION"]
    assert {header_dict(m)["dlq_stage"] for m in new} == {"spark-validation"}
    assert all(header_dict(m)["source_topic"] == topics.raw for m in new)
    # the JVM-native validator (validated topic + windows) must reject exactly what the shared Python schema rejects
    assert not [m for m in _collect(bootstrap, topics.validated, timeout=12, settle=4)
                if header_dict(m).get("event_id") == good["event_id"]]
    assert not [m for m in _collect(bootstrap, topics.windows, timeout=10, settle=4)
                if json.loads(m.value())["customer_id"] == good["customer_id"]]


def test_spark_deduplicates_redelivered_events(bootstrap, spark_env):
    topics, _ = spark_env
    evs = _events(80, seed=31)
    ids = {e.event_id for e in evs}
    _produce(bootstrap, topics.raw, evs)
    _produce(bootstrap, topics.raw, evs)       # second producer session -> genuine broker duplicates
    found = _scored(bootstrap, topics, ids, settle=10)   # listen 10 s more for any (incorrect) second copies
    missing = ids - set(found)
    assert not missing, f"{len(missing)} events never scored"
    assert all(len(v) == 1 for v in found.values())
    validated = [m for m in _collect(bootstrap, topics.validated, lambda r: ids <= {header_dict(m).get("event_id") for m in r},
                                     settle=10) if header_dict(m).get("event_id") in ids]
    assert len(validated) == len(ids)          # dropDuplicatesWithinWatermark: exactly one copy each


def test_spark_handles_late_events(bootstrap, spark_env):
    topics, job = spark_env
    now = datetime.now(timezone.utc)
    evs = _events(40, seed=41, customers=5, start=now)
    _produce(bootstrap, topics.raw, evs)
    _scored(bootstrap, topics, {e.event_id for e in evs})
    # (a) customer-level lateness: older than the customer's latest event - 10 min -> DLQ via the scoring query
    late = evs[-1].model_copy(update={"event_id": f"late-{secrets.token_hex(6)}", "transaction_id": f"TXL{secrets.token_hex(5)}",
                                      "event_time": evs[-1].event_time - timedelta(minutes=45)})
    # (b) watermark lateness for the window queries (WATERMARK_DELAY=1 minute): a NEW customer's event that is
    #     far older than the current watermark is dropped from window aggregates
    other = _events(1, seed=43, customers=1, start=now - timedelta(minutes=30))[0]
    other = other.model_copy(update={"customer_id": f"CLATE{secrets.token_hex(3)}"})
    before = len(_dlq(bootstrap, topics))
    _produce(bootstrap, topics.raw, [late, other])

    def late_in_dlq() -> bool:
        return any(header_dict(m).get("event_id") == late.event_id and
                   header_dict(m).get("dlq_reason") == "LATE_EVENT_BEYOND_TOLERANCE" for m in _dlq(bootstrap, topics)[before:])
    assert wait_until(late_in_dlq, 90, 3)

    def dropped() -> bool:
        p = job.progress() or {}
        return any(q["name"].startswith("window-") and (q.get("rows_dropped_by_watermark") or 0) > 0 for q in p.get("queries", []))
    assert wait_until(dropped, 90, 3), job.progress()
    # the window topic never receives an aggregate for the too-late customer
    assert not [m for m in _collect(bootstrap, topics.windows, timeout=10, settle=4)
                if json.loads(m.value())["customer_id"] == other.customer_id]


def test_spark_restart_recovers_offsets_and_state_from_checkpoint(bootstrap, spark_env):
    topics, job = spark_env
    start = datetime.now(timezone.utc)
    evs = _events(120, seed=51, customers=15, start=start)
    part_a, part_b = evs[:60], evs[60:]
    _produce(bootstrap, topics.raw, part_a)
    found_a = _scored(bootstrap, topics, {e.event_id for e in part_a})
    assert len(found_a) == len(part_a)
    commits_before = sorted(int(x) for x in job.ls("/checkpoints/score/commits") if x.isdigit())
    assert commits_before and job.ls("/checkpoints/score/state")
    old_app = (job.progress() or {}).get("app_id")
    _docker("kill", job.name)                     # hard crash (SIGKILL), no graceful shutdown
    assert not job.running()
    _produce(bootstrap, topics.raw, part_b)       # arrives while Spark is down
    job.restart_container()
    new = job.wait_ready(not_app_id=old_app)
    assert new["app_id"] != old_app
    found = _scored(bootstrap, topics, {e.event_id for e in evs})
    assert set(found) == {e.event_id for e in evs}
    # part B processed exactly once
    assert all(len(found[e.event_id]) == 1 for e in part_b)
    # part A: at-least-once means a replay of the last uncommitted batch is possible; never lost
    replayed = [e.event_id for e in part_a if len(found[e.event_id]) > 1]
    # restored state store: part-B features continue the customers' history from part A
    offline = build_feature_frame(evs).set_index("meta_event_id")
    for e in part_b:
        got = found[e.event_id][0]["features"]
        assert got["hist_tx_count"] == offline.loc[e.event_id]["hist_tx_count"], e.event_id
        assert got["tx_count_1h"] == offline.loc[e.event_id]["tx_count_1h"], e.event_id
    commits_after = sorted(int(x) for x in job.ls("/checkpoints/score/commits") if x.isdigit())
    assert commits_after[-1] > commits_before[-1]                 # resumed from checkpoint, new batches committed

    # checkpointed source offsets == Kafka end offsets (everything consumed, nothing skipped)
    from confluent_kafka import Consumer, TopicPartition

    def ckpt_offsets() -> tuple[int, int, dict[str, int]]:
        offs = sorted(int(x) for x in job.ls("/checkpoints/score/offsets") if x.isdigit())
        commits = sorted(int(x) for x in job.ls("/checkpoints/score/commits") if x.isdigit())
        last = _docker("exec", job.name, "cat", f"/checkpoints/score/offsets/{offs[-1]}").stdout.strip().splitlines()[-1]
        return offs[-1], commits[-1], json.loads(last)[topics.raw]

    probe = Consumer({"bootstrap.servers": bootstrap, "group.id": f"probe-{secrets.token_hex(3)}"})
    ends = {str(p): probe.get_watermark_offsets(TopicPartition(topics.raw, p), timeout=10)[1] for p in range(6)}
    probe.close()

    def consistent() -> bool:
        last_off, last_commit, offsets = ckpt_offsets()
        return last_off == last_commit and {k: int(v) for k, v in offsets.items()} == ends
    assert wait_until(consistent, 60, 3), (ckpt_offsets(), ends)
    print(f"replayed-after-crash (at-least-once) scored records from part A: {len(replayed)}")


def test_spark_survives_kafka_outage(bootstrap, spark_env):
    topics, job = spark_env
    kafka = _docker("ps", "-q", "--filter", f"label=com.docker.compose.project={COMPOSE_PROJECT}",
                    "--filter", "label=com.docker.compose.service=kafka", check=False).stdout.strip()
    if not kafka:
        pytest.skip("NOT EXECUTED: kafka container not managed by compose project")
    _docker("stop", kafka, timeout=120)
    time.sleep(20)                                 # Spark keeps running/retrying (or fails and restarts)
    _docker("start", kafka, timeout=120)
    from streaming.kafka.client import KafkaUnavailableError, check_kafka

    def up() -> bool:
        try:
            check_kafka(bootstrap, timeout=3)
            return True
        except KafkaUnavailableError:
            return False
    assert wait_until(up, 120, 3)
    restarted = False
    if not job.running():                          # a query may fail on prolonged outage: restart from checkpoint
        job.restart_container()
        restarted = True
    job.wait_ready()
    print(f"spark container restarted after kafka outage: {restarted}")
    evs = _events(50, seed=61)
    _produce(bootstrap, topics.raw, evs)
    found = _scored(bootstrap, topics, {e.event_id for e in evs}, timeout=180)
    assert set(found) == {e.event_id for e in evs}
