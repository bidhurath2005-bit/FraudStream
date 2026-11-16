"""Fixtures for Kafka integration tests against REAL infrastructure (docker compose).

Required environment (tests are SKIPPED — i.e. NOT EXECUTED — when unreachable):
  KAFKA_BOOTSTRAP_SERVERS   e.g. localhost:9092
  IT_POSTGRES_ADMIN_URL     e.g. postgresql+psycopg://fraudstream:<pw>@localhost:5433/postgres
  REDIS_URL                 e.g. redis://:<pw>@localhost:6380/0   (tests use DB index 15)

Isolation: every session uses topics prefixed ``it-<random>.`` and a temporary database
``fraudstream_it_<random>``. Teardown removes ONLY those test-created topics/database.
"""
from __future__ import annotations

import os
import secrets
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
ART = ROOT / "ml" / "artifacts"


def _require(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        pytest.skip(f"NOT EXECUTED: {name} not set (start infrastructure: docker compose up -d kafka postgres redis)")
    return v


@pytest.fixture(scope="session")
def bootstrap() -> str:
    from streaming.kafka.client import KafkaUnavailableError, check_kafka

    b = _require("KAFKA_BOOTSTRAP_SERVERS")
    try:
        check_kafka(b, timeout=5)
    except KafkaUnavailableError as exc:
        pytest.skip(f"NOT EXECUTED: Kafka unreachable ({exc})")
    return b


@pytest.fixture
def topics(bootstrap: str) -> Iterator[Any]:
    """Fresh, uniquely-prefixed topics for EACH test (consumer groups start at 'earliest')."""
    from streaming.kafka.client import Topics, delete_topics, ensure_topics

    t = Topics(f"it-{secrets.token_hex(4)}.")
    ensure_topics(bootstrap, t)
    yield t
    from confluent_kafka.admin import AdminClient

    mine = [n for n in AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10).topics if n.startswith(t.prefix)]
    failed = delete_topics(bootstrap, mine)   # only topics carrying THIS test's unique prefix
    assert not failed, f"test topic cleanup failed: {failed}"


@pytest.fixture(scope="session")
def redis_client() -> Any:
    import redis

    url = _require("REDIS_URL")
    from urllib.parse import urlsplit, urlunsplit

    # Force DB 15: a db number in the URL path (e.g. /0) would otherwise override db=15 and the
    # tests would share keys (heartbeats) with a running stack's services on DB 0.
    client = redis.Redis.from_url(urlunsplit(urlsplit(url)._replace(path="/15")), socket_timeout=3)
    try:
        client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"NOT EXECUTED: Redis unreachable ({exc})")
    return client


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    from sqlalchemy import create_engine, text

    admin_url = _require("IT_POSTGRES_ADMIN_URL")
    name = f"fraudstream_it_{secrets.token_hex(4)}"
    admin = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as c:
            c.execute(text(f'CREATE DATABASE "{name}"'))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"NOT EXECUTED: PostgreSQL unreachable ({exc.__class__.__name__})")
    url = admin_url.rsplit("/", 1)[0] + f"/{name}"
    from app.database.migrate import upgrade
    from app.database.session import init_engine

    upgrade(url)
    init_engine(url)
    yield url
    from app.database.session import get_engine

    get_engine().dispose()
    with admin.connect() as c:   # drop ONLY the temporary database created above
        c.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    admin.dispose()


def run_in_thread(target: Callable[[], Any]) -> tuple[threading.Thread, dict[str, Any]]:
    box: dict[str, Any] = {}

    def wrap() -> None:
        try:
            box["result"] = target()
        except BaseException as exc:  # noqa: BLE001
            box["error"] = exc
    t = threading.Thread(target=wrap, daemon=True)
    t.start()
    return t, box


def wait_until(pred: Callable[[], bool], timeout: float = 60.0, interval: float = 0.5) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(interval)
    return pred()


def read_topic(bootstrap: str, topic: str, expect: int | None = None, timeout: float = 20.0) -> list[Any]:
    """Read a topic from the beginning with a throwaway group (no commits)."""
    from confluent_kafka import Consumer

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"it-reader-{secrets.token_hex(4)}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    out: list[Any] = []
    end = time.monotonic() + timeout
    idle = 0   # CONSECUTIVE empty polls (reset whenever a record arrives)
    while time.monotonic() < end:
        m = c.poll(0.5)
        if m is None:
            idle += 1
            if expect is not None and len(out) >= expect and idle > 3:
                break
            if expect is None and out and idle > 4:
                break
            continue
        idle = 0
        if m.error():
            continue
        out.append(m)
    c.close()
    return out


def committed_lag(bootstrap: str, group: str, topic_names: list[str]) -> int:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
    lag = 0
    for t in topic_names:
        md = c.list_topics(t, timeout=10).topics[t]
        tps = [TopicPartition(t, p) for p in md.partitions]
        for tp in c.committed(tps, timeout=10):
            _lo, hi = c.get_watermark_offsets(tp, timeout=10)
            lag += hi - (tp.offset if tp.offset >= 0 else 0)
    c.close()
    return lag
