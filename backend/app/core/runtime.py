"""Process-wide runtime objects (feature store, scorer, risk engine, broker, managers)."""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.database.session import SessionLocal
from app.models.entities import Transaction
from app.risk_engine.engine import RiskEngine
from app.security.rate_limit import RateLimiter
from app.services.broker import InProcessBroker, RedisBroker
from app.services.model_registry import TrainingJobManager, active_models
from app.services.pipeline import LocalPipeline
from app.services.runtime_config import build_engine, build_runtime_config, publish_runtime_config
from app.services.simulation import SimulationManager
from ml.features.engineering import FeatureStore
from ml.inference.scorer import ModelUnavailableError, Scorer

log = logging.getLogger("fraudstream.runtime")


class Runtime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.started_at = datetime.now(timezone.utc)
        self.feature_store = FeatureStore()
        self.scorer = Scorer(settings.artifacts_dir)
        self.engine = RiskEngine()
        self.model_error: str | None = None
        self.redis = self._redis()
        self.broker: Any = InProcessBroker() if settings.profile == "local" else RedisBroker(settings.redis_url or "")
        self.limiter = RateLimiter(self.redis)
        self.pipeline = LocalPipeline(self) if settings.profile == "local" else None
        self.simulation = SimulationManager(self)
        self.training = TrainingJobManager(settings.artifacts_dir, SessionLocal)
        self._producer: Any = None
        self._producer_lock = threading.Lock()
        self.config_publish_status: dict[str, str] = {}
        self.warm_started_events = 0

    def _redis(self) -> Any:
        if not self.settings.redis_url:
            return None
        import redis

        if "@localhost:" in self.settings.redis_url or "//localhost:" in self.settings.redis_url:
            log.warning("REDIS_URL uses 'localhost'; on Windows this resolves to ::1 first and the async live-feed "
                        "client may time out. Prefer 127.0.0.1.")
        from app.services.redis_guard import GuardedRedis

        # Phase 12: circuit breaker - a Redis outage must not add seconds to every API request
        client = GuardedRedis(redis.Redis.from_url(self.settings.redis_url, socket_timeout=2, socket_connect_timeout=2),
                              "redis (api)")
        try:
            client.ping()
        except Exception as exc:  # noqa: BLE001
            log.warning("redis not reachable at startup (%s); features depending on it report UNAVAILABLE",
                        exc.__class__.__name__)
        return client

    def redis_ok(self) -> bool:
        if self.redis is None:
            return False
        try:
            return bool(self.redis.ping())
        except Exception:  # noqa: BLE001
            return False

    def kafka_producer(self) -> Any:
        from streaming.kafka.client import make_producer

        with self._producer_lock:
            if self._producer is None:
                self._producer = make_producer(self.settings.kafka_bootstrap_servers or "", "fraudstream-api")
            return self._producer

    def reload(self, db: Session) -> None:
        """Rebuild the risk engine and reload active models from the DB (source of truth)."""
        self.engine = build_engine(db)
        act = active_models(db)
        sup, an = act.get("supervised"), act.get("anomaly")
        try:
            self.scorer.load((sup.model_name, sup.version) if sup else None, (an.model_name, an.version) if an else None)
            self.model_error = None if sup else "no active supervised model (train models first)"
        except ModelUnavailableError as exc:
            self.model_error = str(exc)
            log.error("model load failed: %s", exc)
        self.config_publish_status = publish_runtime_config(build_runtime_config(db),
                                                            self.settings.runtime_config_path_dir, self.redis)

    def warm_feature_store(self, db: Session) -> int:
        """Restart recovery for the LOCAL profile: rebuild per-customer state from persisted events."""
        since = datetime.now(timezone.utc) - timedelta(hours=self.settings.warm_start_hours)
        rows = db.execute(select(Transaction).where(Transaction.processed_at >= since)
                          .order_by(Transaction.event_time).limit(500_000)).scalars()
        events = ({
            "event_id": t.event_id, "customer_id": t.customer_id, "event_time": t.event_time, "amount": float(t.amount),
            "status": t.status, "device_id": t.device_id, "merchant_id": t.merchant_id,
            "merchant_category": t.merchant_category, "channel": t.channel, "transaction_type": t.transaction_type,
            "location": {"city": t.city, "country": t.country, "lat": t.lat, "lon": t.lon},
        } for t in rows)
        self.warm_started_events = self.feature_store.warm(events)
        return self.warm_started_events

    def shutdown(self) -> None:
        if self.simulation.running:
            try:
                self.simulation.stop()
            except RuntimeError:
                pass
        if self._producer is not None:
            self._producer.flush(10)


_runtime: Runtime | None = None


def set_runtime(rt: Runtime | None) -> None:
    global _runtime
    _runtime = rt


def get_runtime() -> Runtime:
    if _runtime is None:
        raise RuntimeError("runtime not initialised")
    return _runtime
