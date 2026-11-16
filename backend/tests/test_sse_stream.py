"""SSE live-feed generator (regression for the intermittent live-feed failure).

Before the fix, 15 s without traffic made ``asyncio.wait_for`` cancel the broker generator,
which closed the subscription and ENDED the stream after the first keep-alive (reproduced
against the container stack: stream ended at 15.1 s). Real broker, no mocks; only the timing
constants are shortened so the test runs in about a second."""
from __future__ import annotations

import asyncio
import json

import pytest
from app.api.routes import transactions as tx_routes
from app.services import broker as broker_mod


@pytest.fixture
def fast_ticks(monkeypatch):
    monkeypatch.setattr(broker_mod, "IDLE_TICK_SECONDS", 0.05)


def _data(frame: str) -> dict:
    return json.loads(frame.split("data: ", 1)[1])


def test_stream_survives_idle_periods_and_delivers_later_messages(fast_ticks):
    async def scenario() -> list[str]:
        b = broker_mod.InProcessBroker()
        disconnected = asyncio.Event()

        async def is_disconnected() -> bool:
            return disconnected.is_set()
        frames: list[str] = []
        gen = tx_routes.sse_events(b, is_disconnected, "local", keepalive_seconds=0.2)

        async def consume() -> None:
            async for f in gen:
                frames.append(f)
        task = asyncio.create_task(consume())
        await asyncio.sleep(1.0)                    # idle for 5 keep-alive intervals
        assert not task.done(), "stream ended while idle"
        b.publish({"type": "transaction", "transaction_id": "TX-late"})
        await asyncio.sleep(0.2)
        disconnected.set()
        await asyncio.wait_for(task, 2)
        return frames
    frames = asyncio.run(scenario())
    assert _data(frames[0])["live"] == "connected"
    keepalives = [f for f in frames if f.startswith(": keep-alive")]
    assert len(keepalives) >= 3, frames
    assert any(f.startswith("event: transaction") and _data(f)["transaction_id"] == "TX-late" for f in frames)


def test_connected_is_sent_only_after_subscription(fast_ticks):
    """A message published immediately after the client sees 'connected' must be delivered."""
    async def scenario() -> list[str]:
        b = broker_mod.InProcessBroker()

        async def never() -> bool:
            return False
        gen = tx_routes.sse_events(b, never, "local", keepalive_seconds=10)
        first = await gen.__anext__()
        b.publish({"type": "transaction", "transaction_id": "TX-immediate"})
        second = await asyncio.wait_for(gen.__anext__(), 2)
        await gen.aclose()
        assert not b._subs, "subscription not released on close"
        return [first, second]
    first, second = asyncio.run(scenario())
    assert _data(first)["live"] == "connected"
    assert _data(second)["transaction_id"] == "TX-immediate"


def test_unavailable_broker_status_is_forwarded():
    class DownBroker:
        kind = "redis"

        async def subscribe(self):
            yield {"type": "status", "live": "unavailable", "reason": "redis unreachable: ConnectionError"}

    async def scenario() -> list[str]:
        async def never() -> bool:
            return False
        return [f async for f in tx_routes.sse_events(DownBroker(), never, "distributed")]
    frames = asyncio.run(scenario())
    assert [_data(f)["live"] for f in frames] == ["connected", "unavailable"]
