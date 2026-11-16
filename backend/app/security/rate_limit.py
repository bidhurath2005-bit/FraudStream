"""Fixed-window rate limiting. Uses Redis when configured (shared across API replicas);
otherwise an in-process limiter. The active backend is reported by /health."""
from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict

log = logging.getLogger("fraudstream.ratelimit")


class RateLimiter:
    def __init__(self, redis_client=None) -> None:  # type: ignore[no-untyped-def]
        self.redis = redis_client
        self._local: dict[str, tuple[int, int]] = defaultdict(lambda: (0, 0))
        self._lock = threading.Lock()
        self.backend = "redis" if redis_client is not None else "in-process"
        self.redis_errors = 0

    def hit(self, key: str, limit: int, window_seconds: int = 60) -> tuple[bool, int]:
        """Register a hit. Returns (allowed, retry_after_seconds)."""
        window = int(time.time() // window_seconds)
        retry = window_seconds - int(time.time() % window_seconds)
        if self.redis is not None:
            try:
                rk = f"fraudstream:rl:{key}:{window}"
                pipe = self.redis.pipeline()
                pipe.incr(rk)
                pipe.expire(rk, window_seconds + 1)
                count = int(pipe.execute()[0])
                self.backend = "redis"           # (Phase 12) report recovery after an outage, not only the failure
                return count <= limit, retry
            except Exception as exc:  # noqa: BLE001 - Redis outage: fall back, but record it
                self.redis_errors += 1
                log.warning("redis rate limiter unavailable (%s); using in-process limiter", exc.__class__.__name__)
                self.backend = "in-process (redis unavailable)"
        with self._lock:
            w, c = self._local[key]
            c = c + 1 if w == window else 1
            self._local[key] = (window, c)
            if len(self._local) > 50_000:
                self._local = defaultdict(lambda: (0, 0), {k: v for k, v in self._local.items() if v[0] == window})
            return c <= limit, retry
