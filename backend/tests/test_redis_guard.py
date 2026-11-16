"""Phase 12: Redis circuit breaker - an outage costs one failing call, not one per request/event."""
from __future__ import annotations

import time

import pytest
import redis

from app.security.rate_limit import RateLimiter
from app.services.redis_guard import GuardedRedis, RedisCircuitOpen


class FlakyClient:
    def __init__(self) -> None:
        self.down, self.calls, self.store = False, 0, {}

    def _check(self) -> None:
        self.calls += 1
        if self.down:
            time.sleep(0.05)                       # a slow failure, like the ~3.9 s DNS failure measured
            raise redis.exceptions.ConnectionError("Name or service not known")

    def get(self, k):  # noqa: ANN001, ANN201
        self._check()
        return self.store.get(k)

    def ping(self) -> bool:
        self._check()
        return True

    def pipeline(self):  # noqa: ANN201
        client = self

        class P:
            def __init__(self) -> None:
                self.ops: list = []

            def incr(self, k):  # noqa: ANN001, ANN201
                self.ops.append(k)
                return self

            def expire(self, *_a):  # noqa: ANN002, ANN201
                return self

            def execute(self):  # noqa: ANN201
                client._check()
                return [len(self.ops)]
        return P()


def test_circuit_opens_on_failure_fails_fast_and_closes_when_redis_answers():
    c = FlakyClient()
    g = GuardedRedis(c, "test", probe_interval=0.05)
    assert g.ping() is True and not g.circuit_open
    c.down = True
    with pytest.raises(redis.exceptions.ConnectionError):
        g.get("k")                                  # the one slow failing call
    assert g.circuit_open and g.trips == 1
    calls = c.calls
    t0 = time.perf_counter()
    for _ in range(50):
        with pytest.raises(RedisCircuitOpen):
            g.get("k")
    assert time.perf_counter() - t0 < 0.05         # fail fast: nothing reached the client
    assert c.calls - calls <= 3                     # only background probes
    c.down = False
    deadline = time.time() + 2
    while g.circuit_open and time.time() < deadline:
        time.sleep(0.02)
    assert not g.circuit_open and g.get("k") is None


def test_rate_limiter_falls_back_fast_and_reports_recovery():
    c = FlakyClient()
    g = GuardedRedis(c, "test", probe_interval=0.05)
    rl = RateLimiter(g)
    assert rl.hit("k", 10)[0] and rl.backend == "redis"
    c.down = True
    rl.hit("k", 10)
    t0 = time.perf_counter()
    for _ in range(100):
        assert rl.hit("k2", 1000)[0]
    assert time.perf_counter() - t0 < 0.1 and rl.backend.startswith("in-process")
    c.down = False
    deadline = time.time() + 2
    while g.circuit_open and time.time() < deadline:
        time.sleep(0.02)
    rl.hit("k", 10)
    assert rl.backend == "redis"
