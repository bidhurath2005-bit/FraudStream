"""Unit tests for Kafka routing / decoding logic (no broker needed)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.streaming.persistence_consumer import PoisonMessage, decode_scored
from app.streaming.processor import ProcessorStats, route_raw
from app.streaming.runtime_watch import RuntimeWatcher
from ml.features.engineering import FeatureStore
from simulator.generator import SimulationConfig, TransactionGenerator
from streaming.kafka.client import Topics, consumer_config, producer_config

ART = Path(__file__).resolve().parents[2] / "ml" / "artifacts"
T = Topics("unit.")
SRC = {"topic": "unit.transactions.raw", "partition": 0, "offset": 7}


@pytest.fixture(scope="module")
def watcher():
    w = RuntimeWatcher(ART, None)
    w.refresh(force=True)
    return w


def _events(n=5, scenario=None):
    gen = TransactionGenerator(SimulationConfig(mode="normal", max_events=n, duration_seconds=None, customer_count=20, seed=9),
                               datetime.now(timezone.utc))
    evs = list(gen.events())
    if scenario:
        evs += gen.inject(scenario, evs[-1].customer_id)
    return evs


def test_delivery_settings_are_durable_and_manual_commit():
    p = producer_config("x:1", "c")
    assert p["acks"] == "all" and p["enable.idempotence"] is True
    c = consumer_config("x:1", "g", "c")
    assert c["enable.auto.commit"] is False and c["enable.auto.offset.store"] is False


def test_topic_prefix():
    assert T.raw == "unit.transactions.raw" and T.dlq == "unit.fraud.dlq"
    assert [s.name for s in T.specs()][0] == "unit.transactions.raw" and len(T.specs()) == 7


def test_valid_event_routes_to_validated_and_scored_topic(watcher):
    store, st = FeatureStore(), ProcessorStats()
    ev = _events(1)[0]
    out = route_raw(json.dumps(ev.to_wire()).encode(), ev.customer_id.encode(), 1_700_000_000_000, SRC, T, store, watcher, st)
    assert [o.topic for o in out][0] == T.validated
    assert out[1].topic in (T.normal, T.suspicious) and out[1].key == ev.customer_id.encode()
    scored = json.loads(out[1].value)
    assert scored["processor"] == "python-kafka" and scored["decision"]["model_version"]
    assert scored["ingested_at"].startswith("2023-11-14")       # Kafka record timestamp used as ingest time
    assert "ground_truth" not in scored["features"] and "scenario" not in scored["features"]
    assert st.valid == 1


@pytest.mark.parametrize("payload,reason", [
    (b"{not json", "MALFORMED_JSON"), (b"[1,2]", "MALFORMED_JSON"), (None, "EMPTY_PAYLOAD"), (b"", "EMPTY_PAYLOAD"),
    (json.dumps({"event_id": "x"}).encode(), "SCHEMA_VIOLATION"),
])
def test_invalid_payloads_route_to_dlq_with_reason_and_source(watcher, payload, reason):
    st = ProcessorStats()
    out = route_raw(payload, b"k", None, SRC, T, FeatureStore(), watcher, st)
    assert len(out) == 1 and out[0].topic == T.dlq
    assert out[0].headers["dlq_reason"].startswith(reason)
    assert out[0].headers["source_offset"] == "7" and st.invalid == 1


def test_duplicate_raw_event_produces_nothing(watcher):
    store, st = FeatureStore(), ProcessorStats()
    ev = _events(1)[0]
    raw = json.dumps(ev.to_wire()).encode()
    assert route_raw(raw, None, None, SRC, T, store, watcher, st)
    assert route_raw(raw, None, None, SRC, T, store, watcher, st) == []
    assert st.duplicates == 1


def test_suspicious_scenario_reaches_suspicious_topic(watcher):
    store, st = FeatureStore(), ProcessorStats()
    topics = []
    for ev in _events(10, "velocity_burst"):
        topics += [o.topic for o in route_raw(json.dumps(ev.to_wire()).encode(), None, None, SRC, T, store, watcher, st)]
    assert T.suspicious in topics and st.to_suspicious >= 1


def test_decode_scored_rejects_poison_messages(watcher):
    ev = _events(1)[0]
    out = route_raw(json.dumps(ev.to_wire()).encode(), None, None, SRC, T, FeatureStore(), watcher, ProcessorStats())
    scored, event = decode_scored(out[1].value)
    assert event.event_id == ev.event_id
    bad = json.loads(out[1].value)
    del bad["decision"]["risk_score"]
    for value in (b"", b"{x", json.dumps({"event": {}}).encode(), json.dumps(bad).encode()):
        with pytest.raises(PoisonMessage):
            decode_scored(value)


def test_runtime_watcher_keeps_the_last_redis_config_during_a_redis_outage():
    """Phase 12: an unreachable Redis must not switch the risk engine to the (possibly stale) file or to the
    built-in defaults; the last configuration Redis provided stays active and the source says so."""
    import json as _json

    class FlakyRedis:
        def __init__(self, cfg):
            self.cfg, self.down = cfg, False

        def get(self, _key):
            if self.down:
                raise ConnectionError("redis down")
            return _json.dumps(self.cfg)

    from app.risk_engine.engine import DEFAULT_RULES

    rules = [dict(r.__dict__) for r in DEFAULT_RULES]
    rules[0]["enabled"] = not rules[0]["enabled"]          # distinguishable from the built-in defaults
    cfg = {"rules": rules}
    r = FlakyRedis(cfg)
    w = RuntimeWatcher(ART, r)
    w.refresh(force=True)
    assert w.source == "redis"
    engine = w.engine
    r.down = True
    assert w.refresh(force=True) is False
    assert w.engine is engine and w.source == "redis (last known; Redis unavailable)" and w.redis_error
    r.down = False
    w.refresh(force=True)
    assert w.source == "redis" and w.redis_error is None
