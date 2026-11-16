"""Pure-Python logic executed INSIDE Spark Python workers (pandas UDFs).

Kept free of pyspark imports so it can be unit-tested on the host without a JVM. It reuses
the project's shared modules unchanged:
  * streaming.schemas.events.parse_event        - schema validation (same as API/processor)
  * ml.features.engineering                      - point-in-time features, dedup, lateness
  * app.risk_engine.detection.detect            - ML inference + risk scoring
"""
from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ml.features.engineering import CustomerState, classify_arrival, compute_features, update_state, validate_features
from streaming.schemas.events import EventValidationError, parse_event

PROCESSOR_NAME = "spark-structured-streaming"
INVALID_KEY = "__invalid__"   # group key for schema-invalid records (routed to the DLQ by the score query)

# Output columns of the validation mapInPandas step
VALIDATION_COLUMNS = ["key", "value", "kafka_ts", "src_partition", "src_offset", "valid", "reason",
                      "event_id", "customer_id", "event_time", "amount", "status"]
# Output columns of the stateful scoring step (headers are assembled in Spark SQL afterwards)
SCORE_COLUMNS = ["topic", "key", "value", "h_event_id", "h_reason", "h_stage", "h_src_partition", "h_src_offset"]


def validate_frame(pdf: pd.DataFrame) -> pd.DataFrame:
    """Validate raw Kafka records with the shared Pydantic schema. One output row per input row."""
    out: dict[str, list[Any]] = {c: [] for c in VALIDATION_COLUMNS}
    for value, key, ts, part, off in zip(pdf["value"], pdf["key"], pdf["kafka_ts"], pdf["src_partition"],
                                         pdf["src_offset"], strict=True):
        raw = bytes(value) if value is not None else b""
        ev = None
        if not raw:
            reason = "EMPTY_PAYLOAD"
        else:
            try:
                ev = parse_event(raw)
                reason = None
            except EventValidationError as exc:
                reason = exc.reason
        out["key"].append(bytes(key) if key is not None else None)
        out["value"].append(raw)
        out["kafka_ts"].append(ts)
        out["src_partition"].append(int(part))
        out["src_offset"].append(int(off))
        out["valid"].append(ev is not None)
        out["reason"].append(reason)
        out["event_id"].append(ev.event_id if ev else None)
        out["customer_id"].append(ev.customer_id if ev else INVALID_KEY)
        out["event_time"].append(pd.Timestamp(ev.event_time).tz_convert("UTC").tz_localize(None) if ev else pd.NaT)
        out["amount"].append(float(ev.amount) if ev else None)
        out["status"].append(str(ev.status) if ev else None)
    return pd.DataFrame(out, columns=VALIDATION_COLUMNS)


# ------------------------------------------------------------------ model/rules (per Python worker)
_WATCHER: Any = None
_WATCHER_LOCK = threading.Lock()


def get_watcher() -> Any:
    """Lazily create one RuntimeWatcher per Python worker process (models are loaded once)."""
    global _WATCHER
    with _WATCHER_LOCK:
        if _WATCHER is None:
            from app.streaming.runtime_watch import RuntimeWatcher

            redis_client = None
            if os.environ.get("REDIS_URL"):
                from app.services.redis_guard import guarded_from_url

                redis_client = guarded_from_url(os.environ["REDIS_URL"], "redis (spark worker)")
            artifacts = Path(os.environ.get("FRAUDSTREAM_ARTIFACTS_DIR", "/app/ml/artifacts"))
            _WATCHER = RuntimeWatcher(artifacts, redis_client)
            _WATCHER.refresh(force=True)
        else:
            _WATCHER.refresh()
        return _WATCHER


def _to_utc(ts: Any) -> datetime | None:
    if ts is None or (isinstance(ts, float) and pd.isna(ts)) or ts is pd.NaT:
        return None
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return t.to_pydatetime().astimezone(timezone.utc)


def process_customer_batch(customer_id: str, rows: pd.DataFrame, state_json: str | None, topics: Any,
                           watcher: Any) -> tuple[list[dict[str, Any]], str, dict[str, int]]:
    """Process one customer's records of a micro-batch against its persisted state.

    Returns (output rows, new state JSON, counters). Rows are handled in event-time order
    (ties by Kafka offset). Duplicates (event id already in state) produce no output; events
    older than the customer's latest event minus the lateness tolerance go to the DLQ."""
    from app.risk_engine.detection import detect, is_suspicious
    from ml.inference.scorer import ModelUnavailableError

    state = CustomerState.from_dict(json.loads(state_json)) if state_json else CustomerState()
    counters = {"valid": 0, "duplicates": 0, "late": 0, "to_normal": 0, "to_suspicious": 0, "model_unavailable": 0}
    out: list[dict[str, Any]] = []
    pending: list[tuple[Any, dict[str, Any], Any, Any]] = []
    rows = rows.sort_values(["event_time", "src_offset"], kind="mergesort")
    for value, ts, part, off in zip(rows["value"], rows["kafka_ts"], rows["src_partition"], rows["src_offset"], strict=True):
        ev = parse_event(bytes(value))           # already validated upstream; parse to an object
        arrival = classify_arrival(ev, state)
        if arrival == "duplicate":
            counters["duplicates"] += 1
            continue
        if arrival == "late":
            counters["late"] += 1
            out.append({"topic": topics.dlq, "key": customer_id, "value": bytes(value).decode(),
                        "h_event_id": ev.event_id, "h_reason": "LATE_EVENT_BEYOND_TOLERANCE", "h_stage": "spark-scoring",
                        "h_src_partition": str(part), "h_src_offset": str(off)})
            continue
        feats = compute_features(ev, state)
        validate_features(feats)
        update_state(state, ev)
        pending.append((ev, feats, ts, (part, off)))
    if pending:
        scores: list[Any] | None = None
        if watcher.scorer.ready:
            try:
                scores = watcher.scorer.score_many([p[1] for p in pending])
            except ModelUnavailableError:
                scores = None
        for i, (ev, feats, ts, (part, off)) in enumerate(pending):
            scored = detect(ev.to_wire(), feats, None, watcher.engine, ingested_at=_to_utc(ts),
                            precomputed=scores[i] if scores else None)
            if scores is None:
                scored["model_error"] = watcher.model_error or "no active supervised model loaded"
                counters["model_unavailable"] += 1
            scored["processor"] = PROCESSOR_NAME
            scored["runtime_config_source"] = watcher.source
            suspicious = is_suspicious(scored)
            counters["valid"] += 1
            counters["to_suspicious" if suspicious else "to_normal"] += 1
            out.append({"topic": topics.suspicious if suspicious else topics.normal, "key": customer_id,
                        "value": json.dumps(scored, separators=(",", ":"), default=str), "h_event_id": ev.event_id,
                        "h_reason": None, "h_stage": None, "h_src_partition": str(part), "h_src_offset": str(off)})
    return out, json.dumps(state.to_dict(), separators=(",", ":")), counters


def dead_letter_rows(rows: pd.DataFrame, topics: Any) -> list[dict[str, Any]]:
    out = []
    for value, reason, part, off in zip(rows["value"], rows["reason"], rows["src_partition"], rows["src_offset"], strict=True):
        raw = bytes(value) if value is not None else b""
        out.append({"topic": topics.dlq, "key": None, "value": raw.decode("utf-8", errors="replace"),
                    "h_event_id": None, "h_reason": reason or "INVALID", "h_stage": "spark-validation",
                    "h_src_partition": str(part), "h_src_offset": str(off)})
    return out


def score_group_iter(key: tuple[str], pdf_iter: Iterable[pd.DataFrame], state: Any, topics: Any) -> Iterable[pd.DataFrame]:
    """Adapter for DataFrame.groupBy(customer_id).applyInPandasWithState."""
    (customer_id,) = key
    frames = [f for f in pdf_iter if len(f)]
    if not frames:
        return
    rows = pd.concat(frames, ignore_index=True)
    if customer_id == INVALID_KEY:          # schema-invalid records: dead-letter, never touch state
        yield pd.DataFrame(dead_letter_rows(rows, topics), columns=SCORE_COLUMNS)
        return
    prev = state.get[0] if state.exists else None
    out, new_state, _ = process_customer_batch(customer_id, rows, prev, topics, get_watcher())
    state.update((new_state,))
    yield pd.DataFrame(out, columns=SCORE_COLUMNS)
