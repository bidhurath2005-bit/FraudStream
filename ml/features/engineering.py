"""Point-in-time-correct behavioural feature engineering.

The same ``compute_features`` / ``update_state`` pair is used by:
  * offline training (``build_feature_frame``) — events replayed in event-time order,
  * local-profile online inference (``FeatureStore``),
  * the Spark job (``applyInPandasWithState`` keeps ``CustomerState`` per customer).

Features for an event are computed from the customer's state *before* the event is
applied, so no future information can leak into the feature vector.
"""
from __future__ import annotations

import math
import threading
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from ml.features.definitions import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    FEATURE_RANGES,
    FORBIDDEN_FEATURE_FIELDS,
    LATENESS_TOLERANCE_SECONDS,
    MAX_RECENT_EVENTS,
    MAX_SEEN_EVENT_IDS,
    MAX_TRACKED_DEVICES,
    MAX_TRACKED_LOCATIONS,
    MAX_TRACKED_MERCHANTS,
    NO_HISTORY_GAP_SECONDS,
    NUMERIC_FEATURES,
    RECENT_WINDOW_SECONDS,
)
from simulator.geography import haversine_km
from streaming.schemas.events import HIGH_RISK_MERCHANT_CATEGORIES

FAILED_STATUSES = frozenset({"FAILED", "DECLINED"})


class FeatureValidationError(ValueError):
    pass


@dataclass
class CustomerState:
    """Serializable per-customer behavioural state (JSON-safe via to_dict/from_dict)."""
    # recent events: [ts, amount, failed(0/1), lat, lon]
    recent: list[list[float]] = field(default_factory=list)
    n_amount: int = 0
    mean_amount: float = 0.0
    m2_amount: float = 0.0
    total_count: int = 0
    failed_total: int = 0
    last_ts: float | None = None
    last_lat: float | None = None
    last_lon: float | None = None
    devices: dict[str, int] = field(default_factory=dict)
    locations: dict[str, int] = field(default_factory=dict)
    merchants: dict[str, int] = field(default_factory=dict)
    seen_event_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Mapping[str, Any] | None) -> CustomerState:
        return cls(**dict(d)) if d else cls()


def _get(event: Any, key: str) -> Any:
    if isinstance(event, Mapping):
        return event[key]
    return getattr(event, key)


def _event_view(event: Any) -> dict[str, Any]:
    """Extract only the model-relevant raw fields. ``ground_truth`` is deliberately dropped."""
    loc = _get(event, "location")
    loc = loc if isinstance(loc, Mapping) else loc.model_dump()
    ts = _get(event, "event_time")
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        raise FeatureValidationError("event_time must be timezone-aware")
    ts = ts.astimezone(timezone.utc)
    return {
        "event_id": str(_get(event, "event_id")),
        "ts": ts, "epoch": ts.timestamp(),
        "amount": float(_get(event, "amount")),
        "status": str(_get(event, "status")),
        "device_id": str(_get(event, "device_id")),
        "merchant_id": str(_get(event, "merchant_id")),
        "merchant_category": str(_get(event, "merchant_category")),
        "channel": str(_get(event, "channel")),
        "transaction_type": str(_get(event, "transaction_type")),
        "loc_key": f"{loc['city']}|{loc['country']}",
        "lat": float(loc["lat"]), "lon": float(loc["lon"]),
    }


def compute_features(event: Any, state: CustomerState) -> dict[str, Any]:
    """Compute the feature vector for ``event`` from the state *prior* to it."""
    e = _event_view(event)
    t, amount = e["epoch"], e["amount"]
    prior = [r for r in state.recent if r[0] <= t]

    def count(window: float) -> int:
        return sum(1 for r in prior if r[0] >= t - window)

    def spend(window: float) -> float:
        return float(sum(r[1] for r in prior if r[0] >= t - window and not r[2]))

    has_hist = state.total_count > 0
    mean = state.mean_amount if state.n_amount > 0 else 0.0
    std = math.sqrt(state.m2_amount / (state.n_amount - 1)) if state.n_amount >= 2 else 0.0
    z = (amount - mean) / max(std, 1.0) if state.n_amount >= 2 else 0.0
    ratio = amount / mean if mean > 0 else 1.0

    prev = max(prior, key=lambda r: r[0]) if prior else None
    if prev is not None:
        gap, plat, plon = t - prev[0], prev[3], prev[4]
    elif state.last_ts is not None and state.last_ts <= t:
        gap, plat, plon = t - state.last_ts, state.last_lat, state.last_lon
    else:
        gap, plat, plon = float(NO_HISTORY_GAP_SECONDS), None, None
    gap = min(max(gap, 0.0), float(NO_HISTORY_GAP_SECONDS))
    geo_v = 0.0
    if plat is not None and plon is not None:
        km = haversine_km(plat, plon, e["lat"], e["lon"])
        geo_v = min(km / max(gap / 3600.0, 1 / 60), 20000.0)

    hour = e["ts"].hour
    feats: dict[str, Any] = {
        "amount": round(amount, 2),
        "log_amount": math.log1p(amount),
        "hour_of_day": hour,
        "day_of_week": e["ts"].weekday(),
        "is_night": int(hour <= 5),
        "tx_count_1m": count(60), "tx_count_5m": count(300), "tx_count_1h": count(3600),
        "spend_sum_15m": spend(900), "spend_sum_1h": spend(3600),
        "hist_tx_count": state.total_count,
        "hist_mean_amount": mean, "hist_std_amount": std,
        "amount_zscore": max(-50.0, min(50.0, z)),
        "amount_to_mean_ratio": max(0.0, min(1000.0, ratio)),
        "failed_count_1h": sum(1 for r in prior if r[0] >= t - 3600 and r[2]),
        "failed_ratio_hist": state.failed_total / state.total_count if has_hist else 0.0,
        "seconds_since_prev": gap,
        "is_new_device": int(has_hist and e["device_id"] not in state.devices),
        "is_new_location": int(has_hist and e["loc_key"] not in state.locations),
        "is_new_merchant": int(has_hist and e["merchant_id"] not in state.merchants),
        "merchant_frequency": state.merchants.get(e["merchant_id"], 0) / state.total_count if has_hist else 0.0,
        "geo_velocity_kmh": geo_v,
        "has_history": int(has_hist),
        "is_high_risk_category": int(e["merchant_category"] in HIGH_RISK_MERCHANT_CATEGORIES),
        "merchant_category": e["merchant_category"],
        "channel": e["channel"],
        "transaction_type": e["transaction_type"],
        "status": e["status"],
    }
    return feats


def _bounded_inc(d: dict[str, int], key: str, limit: int) -> None:
    d[key] = d.get(key, 0) + 1
    if len(d) > limit:
        victim = min((k for k in d if k != key), key=lambda k: d[k])
        del d[victim]


def update_state(state: CustomerState, event: Any) -> CustomerState:
    """Apply ``event`` to ``state`` (mutates and returns it)."""
    e = _event_view(event)
    t = e["epoch"]
    failed = e["status"] in FAILED_STATUSES
    state.recent.append([t, e["amount"], int(failed), e["lat"], e["lon"]])
    newest = max(t, state.last_ts or t)
    state.recent = [r for r in state.recent if r[0] >= newest - RECENT_WINDOW_SECONDS][-MAX_RECENT_EVENTS:]
    state.total_count += 1
    if failed:
        state.failed_total += 1
    else:
        state.n_amount += 1
        delta = e["amount"] - state.mean_amount
        state.mean_amount += delta / state.n_amount
        state.m2_amount += delta * (e["amount"] - state.mean_amount)
    if state.last_ts is None or t >= state.last_ts:
        state.last_ts, state.last_lat, state.last_lon = t, e["lat"], e["lon"]
    _bounded_inc(state.devices, e["device_id"], MAX_TRACKED_DEVICES)
    _bounded_inc(state.locations, e["loc_key"], MAX_TRACKED_LOCATIONS)
    _bounded_inc(state.merchants, e["merchant_id"], MAX_TRACKED_MERCHANTS)
    state.seen_event_ids.append(e["event_id"])
    if len(state.seen_event_ids) > MAX_SEEN_EVENT_IDS:
        state.seen_event_ids = state.seen_event_ids[-MAX_SEEN_EVENT_IDS:]
    return state


def classify_arrival(event: Any, state: CustomerState) -> str:
    """Return 'duplicate', 'late' or 'ok' for an incoming event against current state."""
    e = _event_view(event)
    if e["event_id"] in state.seen_event_ids:
        return "duplicate"
    if state.last_ts is not None and e["epoch"] < state.last_ts - LATENESS_TOLERANCE_SECONDS:
        return "late"
    return "ok"


def validate_features(feats: Mapping[str, Any]) -> None:
    missing = [f for f in ALL_FEATURES if f not in feats]
    if missing:
        raise FeatureValidationError(f"missing features: {missing}")
    leaked = FORBIDDEN_FEATURE_FIELDS.intersection(feats)
    if leaked:
        raise FeatureValidationError(f"forbidden fields in feature vector: {sorted(leaked)}")
    for name in NUMERIC_FEATURES:
        v = feats[name]
        if isinstance(v, bool) or not isinstance(v, (int, float)) or math.isnan(v) or math.isinf(v):
            raise FeatureValidationError(f"feature {name} must be a finite number, got {v!r}")
        lo, hi = FEATURE_RANGES[name]
        if not lo <= v <= hi:
            raise FeatureValidationError(f"feature {name}={v} outside [{lo}, {hi}]")
    for name in CATEGORICAL_FEATURES:
        if not isinstance(feats[name], str) or not feats[name]:
            raise FeatureValidationError(f"feature {name} must be a non-empty string")


@dataclass
class ProcessResult:
    status: str                          # ok | duplicate | late
    features: dict[str, Any] | None


class FeatureStore:
    """Thread-safe in-memory per-customer state for the LOCAL profile.

    In the DISTRIBUTED profile the equivalent state lives in Spark's checkpointed
    state store (see streaming/spark/fraud_stream_job.py)."""

    def __init__(self) -> None:
        self._states: dict[str, CustomerState] = {}
        self._lock = threading.Lock()

    def __len__(self) -> int:
        return len(self._states)

    def process(self, event: Any, *, apply: bool = True) -> ProcessResult:
        cid = str(_get(event, "customer_id"))
        with self._lock:
            st = self._states.setdefault(cid, CustomerState())
            arrival = classify_arrival(event, st)
            if arrival != "ok":
                return ProcessResult(arrival, None)
            feats = compute_features(event, st)
            validate_features(feats)
            if apply:
                update_state(st, event)
            return ProcessResult("ok", feats)

    def apply(self, event: Any) -> None:
        """Advance state with an event that has been durably processed."""
        cid = str(_get(event, "customer_id"))
        with self._lock:
            st = self._states.setdefault(cid, CustomerState())
            if classify_arrival(event, st) != "duplicate":
                update_state(st, event)

    def warm(self, events: Iterable[Any]) -> int:
        """Rebuild state from already-persisted events (restart recovery)."""
        n = 0
        with self._lock:
            for ev in sorted(events, key=lambda x: _event_view(x)["epoch"]):
                st = self._states.setdefault(str(_get(ev, "customer_id")), CustomerState())
                if classify_arrival(ev, st) == "duplicate":
                    continue
                update_state(st, ev)
                n += 1
        return n

    def snapshot(self, customer_id: str) -> dict[str, Any] | None:
        with self._lock:
            st = self._states.get(customer_id)
            if st is None:
                return None
            d = st.to_dict()
            d["seen_event_ids"] = len(d["seen_event_ids"])
            d["recent"] = len(d["recent"])
            return d

    def clear(self) -> None:
        with self._lock:
            self._states.clear()


def build_feature_frame(events: Iterable[Any]):  # -> pandas.DataFrame
    """Replay events in event-time order and compute point-in-time features.

    Returns a DataFrame with ALL_FEATURES plus metadata columns prefixed ``meta_``.
    Labels come from ``ground_truth`` only into ``meta_label`` (never into features)."""
    import pandas as pd

    store = FeatureStore()
    rows: list[dict[str, Any]] = []
    evs = sorted(events, key=lambda x: _event_view(x)["epoch"])
    for ev in evs:
        res = store.process(ev)
        if res.status != "ok" or res.features is None:
            continue
        gt = _get(ev, "ground_truth") if (isinstance(ev, Mapping) and "ground_truth" in ev) or hasattr(ev, "ground_truth") else None
        if gt is not None and not isinstance(gt, Mapping):
            gt = gt.model_dump()
        row = dict(res.features)
        row["meta_event_id"] = str(_get(ev, "event_id"))
        row["meta_customer_id"] = str(_get(ev, "customer_id"))
        row["meta_event_time"] = _event_view(ev)["ts"]
        row["meta_label"] = int(bool(gt["is_suspicious"])) if gt else None
        row["meta_scenario"] = gt["scenario"] if gt else None
        rows.append(row)
    return pd.DataFrame(rows)
