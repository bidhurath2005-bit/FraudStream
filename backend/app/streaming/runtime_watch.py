"""Runtime configuration for out-of-process workers (Kafka processor, Spark job).

The API publishes the active models + risk rules/settings to Redis key
``fraudstream:runtime_config`` and to ``<artifacts>/runtime_config.json``. Workers poll it,
rebuilding the risk engine and reloading models when it changes.

Resolution order: Redis -> file -> built-in defaults. The source actually used is exposed
as ``source`` so it can be reported (never silently assumed).

Phase 12: a Redis ERROR (outage) is not "no config". Once a configuration has been loaded from Redis, an
unreachable Redis keeps that configuration (source ``redis (last known; Redis unavailable)``) instead of
switching to the file - in containers a possibly stale copy - or to the built-in default rules, which silently
changed the scoring rules for the duration of an outage. The file/defaults are used only when Redis has never
provided a configuration (or the key is absent).
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

from app.risk_engine.engine import RiskEngine, engine_from_config
from ml.inference.scorer import ModelUnavailableError, Scorer

log = logging.getLogger("fraudstream.runtime_watch")
REDIS_KEY = "fraudstream:runtime_config"


class RuntimeWatcher:
    def __init__(self, artifacts_dir: Path, redis_client: Any | None = None, refresh_seconds: float = 15.0) -> None:
        self.artifacts_dir = artifacts_dir
        self.redis = redis_client
        self.refresh_seconds = refresh_seconds
        self.scorer = Scorer(artifacts_dir)
        self.engine: RiskEngine = RiskEngine()
        self.source = "defaults"
        self.model_error: str | None = None
        self._fingerprint: str | None = None
        self._models: dict[str, Any] | None = None
        self._last = 0.0
        self._lock = threading.Lock()
        self.redis_error: str | None = None

    def _read(self) -> tuple[dict[str, Any] | None, str]:
        if self.redis is not None:
            try:
                raw = self.redis.get(REDIS_KEY)
                self.redis_error = None
                if raw:
                    return json.loads(raw), "redis"
            except Exception as exc:  # noqa: BLE001
                self.redis_error = exc.__class__.__name__
                log.warning("redis runtime config unavailable: %s", self.redis_error)
                if self.source.startswith("redis"):
                    return None, "keep"
        p = self.artifacts_dir / "runtime_config.json"
        if p.exists():
            try:
                return json.loads(p.read_text()), "file"
            except (OSError, json.JSONDecodeError) as exc:
                log.warning("runtime_config.json unreadable: %s", exc)
        return None, "defaults"

    def refresh(self, force: bool = False) -> bool:
        """Reload if the config changed. Returns True if anything was (re)loaded."""
        now = time.monotonic()
        if not force and now - self._last < self.refresh_seconds:
            return False
        self._last = now
        cfg, source = self._read()
        if source == "keep":                    # Redis outage: keep the configuration Redis last provided
            if self.source == "redis":
                log.error("Redis unavailable - keeping the last runtime configuration loaded from Redis")
            self.source = "redis (last known; Redis unavailable)"
            return False
        fp = json.dumps({k: v for k, v in (cfg or {}).items() if k != "generated_at"}, sort_keys=True)
        with self._lock:
            if fp == self._fingerprint and not force:
                return False
            self._fingerprint, self.source = fp, source
            self.engine = engine_from_config(cfg)
            models = (cfg or {}).get("active_models")
            try:
                if models:
                    s, a = models.get("supervised"), models.get("anomaly")
                    if models != self._models or not self.scorer.ready:
                        self.scorer.load((s["model_name"], s["version"]) if s else None,
                                         (a["model_name"], a["version"]) if a else None)
                else:
                    self.scorer.load_from_registry()   # no published config: registry.json "active"
                self._models = models
                self.model_error = None if self.scorer.ready else "no active supervised model"
            except ModelUnavailableError as exc:
                self.model_error = str(exc)
                log.error("model load failed: %s", exc)
        log.info("runtime config loaded from %s (model=%s)", source,
                 self.scorer.supervised.label if self.scorer.supervised else None)
        return True
