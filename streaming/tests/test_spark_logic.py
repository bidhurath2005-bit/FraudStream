"""Unit tests for the Spark UDF logic (runs on the host, no JVM needed)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from app.streaming.runtime_watch import RuntimeWatcher
from ml.features.engineering import build_feature_frame
from simulator.generator import SimulationConfig, TransactionGenerator
from streaming.kafka.client import Topics
from streaming.spark import logic

ART = Path(__file__).resolve().parents[2] / "ml" / "artifacts"
T = Topics("unit.")
T0 = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def watcher():
    w = RuntimeWatcher(ART, None)
    w.refresh(force=True)
    return w


def _events(n=40, seed=3, scenario=None):
    gen = TransactionGenerator(SimulationConfig(mode="normal", tps=5, max_events=n, duration_seconds=None,
                                                customer_count=8, seed=seed), T0)
    evs = list(gen.events())
    if scenario:
        evs += gen.inject(scenario, evs[-1].customer_id, when=evs[-1].event_time + timedelta(seconds=1), align="start")
    return evs


def _raw_frame(values):
    return pd.DataFrame({"value": values, "key": [b"k"] * len(values),
                         "kafka_ts": [pd.Timestamp("2026-05-01 12:00:00")] * len(values),
                         "src_partition": [0] * len(values), "src_offset": list(range(len(values)))})


def test_validate_frame_flags_every_invalid_kind():
    good = _events(1)[0].to_wire()
    vals = [json.dumps(good).encode(), b"{x", b"", None, json.dumps({**good, "amount": -1}).encode()]
    out = logic.validate_frame(_raw_frame(vals))
    assert list(out["valid"]) == [True, False, False, False, False]
    assert out["reason"][1].startswith("MALFORMED_JSON") and out["reason"][2] == "EMPTY_PAYLOAD"
    assert out["reason"][3] == "EMPTY_PAYLOAD" and out["reason"][4].startswith("SCHEMA_VIOLATION")
    assert out["event_id"][0] == good["event_id"] and out["event_time"][0].tzinfo is None   # UTC-naive for Spark
    assert list(out.columns) == logic.VALIDATION_COLUMNS


def _rows(evs, start_offset=0):
    return pd.DataFrame({"value": [json.dumps(e.to_wire()).encode() for e in evs],
                         "kafka_ts": [pd.Timestamp(e.event_time).tz_convert("UTC").tz_localize(None) for e in evs],
                         "src_partition": [0] * len(evs), "src_offset": range(start_offset, start_offset + len(evs)),
                         "event_time": [pd.Timestamp(e.event_time).tz_convert("UTC").tz_localize(None) for e in evs]})


def test_stateful_batches_match_offline_features_and_survive_state_roundtrip(watcher):
    """Splitting a customer's stream across micro-batches (state serialised in between, as Spark's
    state store does) must give exactly the same features as one offline pass."""
    evs = _events(60)
    cust = evs[0].customer_id
    mine = [e for e in evs if e.customer_id == cust]
    assert len(mine) >= 4
    state = None
    outputs = []
    for chunk in (mine[: len(mine) // 2], mine[len(mine) // 2:]):
        out, state, counters = logic.process_customer_batch(cust, _rows(chunk), state, T, watcher)
        outputs += out
        assert counters["duplicates"] == 0 and counters["late"] == 0
    offline = build_feature_frame(mine).set_index("meta_event_id")
    assert len(outputs) == len(mine)
    for o in outputs:
        scored = json.loads(o["value"])
        assert o["topic"] in (T.normal, T.suspicious) and scored["processor"] == "spark-structured-streaming"
        ref = offline.loc[scored["event"]["event_id"]]
        for k in ("hist_tx_count", "tx_count_1h", "seconds_since_prev", "amount_zscore", "is_new_device"):
            assert ref[k] == pytest.approx(scored["features"][k]), k
        assert scored["decision"]["model_version"] and scored["model_error"] is None
        assert "ground_truth" not in scored["features"]


def test_duplicates_within_and_across_batches_are_suppressed(watcher):
    evs = [e for e in _events(30, seed=5) if e.customer_id == _events(30, seed=5)[0].customer_id]
    cust = evs[0].customer_id
    out1, state, c1 = logic.process_customer_batch(cust, _rows(evs + evs[:2]), None, T, watcher)
    assert c1["duplicates"] == 2 and len(out1) == len(evs)
    out2, _, c2 = logic.process_customer_batch(cust, _rows(evs), state, T, watcher)   # replayed batch
    assert out2 == [] and c2["duplicates"] == len(evs)


def test_late_event_goes_to_dlq_with_reason(watcher):
    evs = _events(20, seed=7)
    cust = evs[0].customer_id
    mine = [e for e in evs if e.customer_id == cust]
    _, state, _ = logic.process_customer_batch(cust, _rows(mine), None, T, watcher)
    late = mine[0].model_copy(update={"event_id": "late-evt-0001", "transaction_id": "TXLATE1",
                                      "event_time": mine[-1].event_time - timedelta(hours=1)})
    out, _, c = logic.process_customer_batch(cust, _rows([late]), state, T, watcher)
    assert c["late"] == 1 and out[0]["topic"] == T.dlq and out[0]["h_reason"] == "LATE_EVENT_BEYOND_TOLERANCE"


def test_suspicious_scenario_routes_to_suspicious_topic(watcher):
    evs = _events(30, seed=11, scenario="velocity_burst")
    cust = evs[-1].customer_id
    mine = [e for e in evs if e.customer_id == cust]
    out, _, c = logic.process_customer_batch(cust, _rows(mine), None, T, watcher)
    assert c["to_suspicious"] >= 1 and any(o["topic"] == T.suspicious for o in out)


def test_model_unavailable_is_explicit(watcher):
    class NoModel:
        class scorer:  # noqa: N801
            ready = False
        engine = watcher.engine
        source = "test"
        model_error = "no active supervised model"
    evs = _events(5, seed=13)
    out, _, c = logic.process_customer_batch(evs[0].customer_id, _rows(evs[:1]), None, T, NoModel())
    s = json.loads(out[0]["value"])
    assert c["model_unavailable"] == 1 and s["model_error"] and s["decision"]["model_version"] is None


def test_invalid_group_is_dead_lettered_without_state(watcher):
    vals = [b"{x", b"", json.dumps({"event_id": "only"}).encode()]
    v = logic.validate_frame(_raw_frame(vals))
    assert set(v["customer_id"]) == {logic.INVALID_KEY}

    class State:
        exists, updated = False, None

        def update(self, s):  # noqa: ANN001
            self.updated = s
    st = State()
    out = list(logic.score_group_iter((logic.INVALID_KEY,), [v], st, T))[0]
    assert list(out["topic"]) == [T.dlq] * 3 and set(out["h_stage"]) == {"spark-validation"}
    assert out["h_reason"].tolist()[0].startswith("MALFORMED_JSON") and st.updated is None
