"""Redis circuit breaker (Phase 12).

Measured on the isolated stack: with the Redis container stopped, every Redis call failed only after ~3.9 s (the
service name no longer resolves; DNS is not covered by socket_connect_timeout). Because the rate limiter, the
processor-status checks, the consumer's live-feed publish and heartbeat all call Redis synchronously, a Redis outage
made EVERY API request ~4 s slower (/system/status 12 s) and cut persistence to ~0.25 events/s - although Redis
holds no system-of-record data.

``GuardedRedis`` wraps a redis-py client: after a connection/timeout error the circuit OPENS and every call fails
immediately with ``RedisCircuitOpen`` (a ``redis.exceptions.ConnectionError``, so existing fallbacks apply); one
background thread PINGs Redis every ``probe_interval`` seconds and closes the circuit when it answers. An outage then
costs the calls in flight when it starts, not one slow call per request or per event. Pub/sub objects are passed
through unguarded (the SSE broker has its own reconnect handling).
"""
from __future__ import annotations

import functools
import logging
import threading
import time
from typing import Any

import redis

log = logging.getLogger("fraudstream.redis")
_TRIP = (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError)


class RedisCircuitOpen(redis.exceptions.ConnectionError):
    """Redis is known to be unreachable; the call was not attempted."""


class _GuardedPipeline:
    def __init__(self, guard: GuardedRedis, pipe: Any):
        self._g, self._p = guard, pipe

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._p, name)
        if name == "execute":
            return functools.partial(self._g._call, attr)
        if callable(attr):
            def queued(*a: Any, **k: Any) -> Any:
                attr(*a, **k)
                return self
            return queued
        return attr


class GuardedRedis:
    def __init__(self, client: Any, name: str = "redis", probe_interval: float = 5.0):
        self._c = client
        self.name = name
        self.probe_interval = probe_interval
        self._open = False
        self._lock = threading.Lock()
        self.trips = 0
        self.opened_at: float | None = None
        self.last_error: str | None = None

    @property
    def circuit_open(self) -> bool:
        return self._open

    def _call(self, fn: Any, *a: Any, **k: Any) -> Any:
        if self._open:
            raise RedisCircuitOpen(f"{self.name}: circuit open since {self.opened_at:.0f} ({self.last_error})")
        try:
            return fn(*a, **k)
        except _TRIP as exc:
            self._trip(exc)
            raise

    def _trip(self, exc: BaseException) -> None:
        with self._lock:
            if self._open:
                return
            self._open, self.opened_at = True, time.time()
            self.trips += 1
            self.last_error = exc.__class__.__name__
        log.error("%s unreachable (%s): circuit OPEN - calls fail fast until it answers again", self.name,
                  self.last_error)
        threading.Thread(target=self._probe, name=f"{self.name}-probe", daemon=True).start()

    def _probe(self) -> None:
        while self._open:
            time.sleep(self.probe_interval)
            try:
                self._c.ping()
            except Exception:  # noqa: BLE001 - still down
                continue
            with self._lock:
                self._open = False
            log.warning("%s reachable again after %.0f s: circuit CLOSED", self.name, time.time() - (self.opened_at or 0))

    def pipeline(self, *a: Any, **k: Any) -> _GuardedPipeline:
        return _GuardedPipeline(self, self._c.pipeline(*a, **k))

    def pubsub(self, *a: Any, **k: Any) -> Any:
        return self._c.pubsub(*a, **k)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._c, name)
        return functools.partial(self._call, attr) if callable(attr) else attr


def guarded_from_url(url: str, name: str = "redis", **kw: Any) -> GuardedRedis:
    kw.setdefault("socket_timeout", 2)
    kw.setdefault("socket_connect_timeout", 2)
    return GuardedRedis(redis.Redis.from_url(url, **kw), name)
