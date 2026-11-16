"""Live event fan-out for SSE.

* LOCAL profile: in-process broker (events produced by the in-process pipeline).
* DISTRIBUTED profile: Redis pub/sub channel ``fraudstream:live`` (events published by
  the Kafka persistence consumer, which runs in a different process/container).
If Redis is unreachable in the distributed profile, subscribers receive an explicit
``unavailable`` status event — the API never pretends the live feed is working.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import AsyncIterator
from typing import Any

log = logging.getLogger("fraudstream.broker")
CHANNEL = "fraudstream:live"
# subscribe() yields ``None`` once the subscription is active and then at least every
# IDLE_TICK_SECONDS while idle, so callers can send keep-alives / check for disconnects
# WITHOUT cancelling the generator (cancelling it mid-await closes it and ended the SSE stream).
IDLE_TICK_SECONDS = 5.0


class InProcessBroker:
    kind = "in-process"

    def __init__(self) -> None:
        self._subs: set[tuple[asyncio.AbstractEventLoop, asyncio.Queue[dict[str, Any]]]] = set()
        self._lock = threading.Lock()

    def publish(self, message: dict[str, Any]) -> None:
        with self._lock:
            subs = list(self._subs)
        for loop, q in subs:
            def _put(q: asyncio.Queue[dict[str, Any]] = q) -> None:
                if q.qsize() < 1000:
                    q.put_nowait(message)
            try:
                loop.call_soon_threadsafe(_put)
            except RuntimeError:
                pass

    async def subscribe(self) -> AsyncIterator[dict[str, Any] | None]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        entry = (asyncio.get_running_loop(), q)
        with self._lock:
            self._subs.add(entry)
        try:
            yield None                       # subscribed: nothing published from now on is missed
            while True:
                try:
                    yield await asyncio.wait_for(q.get(), timeout=IDLE_TICK_SECONDS)
                except TimeoutError:  # cancelling a Queue.get is safe (unlike cancelling this generator)
                    yield None
        finally:
            with self._lock:
                self._subs.discard(entry)


class RedisBroker:
    kind = "redis"

    def __init__(self, url: str) -> None:
        import redis

        self.url = url
        from app.services.redis_guard import GuardedRedis

        self._sync = GuardedRedis(redis.Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2), "redis (live publish)")

    def publish(self, message: dict[str, Any]) -> None:
        try:
            self._sync.publish(CHANNEL, json.dumps(message, default=str))
        except Exception as exc:  # noqa: BLE001
            log.warning("redis publish failed: %s", exc.__class__.__name__)

    async def subscribe(self) -> AsyncIterator[dict[str, Any] | None]:
        import redis.asyncio as aioredis

        client = aioredis.Redis.from_url(self.url, socket_connect_timeout=2)
        pubsub = client.pubsub()
        try:
            await pubsub.subscribe(CHANNEL)
        except Exception as exc:  # noqa: BLE001
            yield {"type": "status", "live": "unavailable", "reason": f"redis unreachable: {exc.__class__.__name__}"}
            await client.aclose()
            return
        try:
            yield None                       # SUBSCRIBE acknowledged by Redis
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=IDLE_TICK_SECONDS)
                if msg and msg.get("type") == "message":
                    yield json.loads(msg["data"])
                else:
                    yield None
        finally:
            await pubsub.aclose()
            await client.aclose()
