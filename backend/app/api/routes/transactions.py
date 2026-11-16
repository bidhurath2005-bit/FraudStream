from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Body, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy import func, or_, select
from sqlalchemy.orm import joinedload

from app.api.params import IdPath, Page
from app.core.runtime import get_runtime
from app.models.entities import Alert, Prediction, Transaction
from app.schemas.api import transaction_out
from app.security.auth import DB, CurrentUser, decode_access_token
from app.services.persistence import record_dead_letter
from streaming.kafka.client import KafkaUnavailableError, Topics, check_kafka, encode
from streaming.schemas.events import EventValidationError, parse_event

router = APIRouter(tags=["transactions"])

SORTS = {"processed_at": Transaction.processed_at, "event_time": Transaction.event_time,
         "amount": Transaction.amount, "risk_score": Prediction.risk_score}


@router.get("/transactions")
def list_transactions(
    user: CurrentUser, db: DB,
    page: Page = 1, size: Annotated[int, Query(ge=1, le=200)] = 25,
    customer_id: str | None = None, merchant_category: str | None = None,
    risk_category: Literal["LOW", "MEDIUM", "HIGH"] | None = None,
    min_risk: Annotated[float | None, Query(ge=0, le=100)] = None,
    status_: Annotated[str | None, Query(alias="status")] = None,
    q: Annotated[str | None, Query(max_length=64)] = None,
    since: datetime | None = None, until: datetime | None = None,
    sort: Literal["processed_at", "event_time", "amount", "risk_score"] = "processed_at",
    order: Literal["asc", "desc"] = "desc",
) -> dict[str, Any]:
    stmt = select(Transaction).join(Prediction, Prediction.transaction_id == Transaction.id, isouter=True)
    if customer_id:
        stmt = stmt.where(Transaction.customer_id == customer_id)
    if merchant_category:
        stmt = stmt.where(Transaction.merchant_category == merchant_category)
    if risk_category:
        stmt = stmt.where(Prediction.risk_category == risk_category)
    if min_risk is not None:
        stmt = stmt.where(Prediction.risk_score >= min_risk)
    if status_:
        stmt = stmt.where(Transaction.status == status_)
    if q:
        like = f"%{q}%"
        stmt = stmt.where(or_(Transaction.transaction_id.ilike(like), Transaction.customer_id.ilike(like),
                              Transaction.merchant_id.ilike(like), Transaction.city.ilike(like)))
    if since:
        stmt = stmt.where(Transaction.processed_at >= since)
    if until:
        stmt = stmt.where(Transaction.processed_at <= until)
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    col = SORTS[sort]
    stmt = stmt.order_by(col.desc() if order == "desc" else col.asc(), Transaction.id.desc())
    rows = db.scalars(stmt.options(joinedload(Transaction.prediction)).offset((page - 1) * size).limit(size)).unique()
    return {"items": [transaction_out(t) for t in rows], "total": total, "page": page, "size": size}


@router.get("/transactions/{tx_pk}")
def get_transaction(tx_pk: IdPath, user: CurrentUser, db: DB) -> dict[str, Any]:
    t = db.get(Transaction, tx_pk)
    if t is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "transaction not found")
    out = transaction_out(t, with_prediction=True)
    a = db.scalar(select(Alert).where(Alert.transaction_id == t.id))
    out["alert_uid"] = a.alert_uid if a else None
    return out


@router.post("/transactions/ingest", status_code=202)
def ingest(user: CurrentUser, db: DB, payload: Annotated[dict[str, Any], Body()]) -> dict[str, Any]:
    """Ingest one event. LOCAL: processed synchronously. DISTRIBUTED: produced to Kafka."""
    rt = get_runtime()
    received = datetime.now(timezone.utc)
    try:
        event = parse_event(payload)
    except EventValidationError as exc:
        record_dead_letter(db, "api-ingest", exc.reason, payload, payload.get("event_id") if isinstance(payload, dict) else None)
        db.commit()
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, exc.reason) from exc
    if event.source == "simulator":
        event = event.model_copy(update={"source": "api"})
    if rt.settings.profile == "local":
        assert rt.pipeline is not None
        result = rt.pipeline.process(event, ingested_at=received)
        return {"profile": "local", **{k: v for k, v in result.items() if k != "decision"},
                "decision": result.get("decision")}
    try:
        check_kafka(rt.settings.kafka_bootstrap_servers or "", timeout=3)
        prod = rt.kafka_producer()
        errors: list[str] = []
        raw_topic = Topics.from_env().raw
        prod.produce(raw_topic, key=event.customer_id.encode(), value=encode(event.to_wire()),
                     headers={"event_id": event.event_id, "producer": "fraudstream-api"},
                     on_delivery=lambda err, _m: errors.append(str(err)) if err else None)
        remaining = prod.flush(10)
        if remaining or errors:
            raise KafkaUnavailableError(f"delivery not acknowledged: {errors or 'timeout'}")
    except KafkaUnavailableError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, f"Kafka unavailable: {exc}") from exc
    return {"profile": "distributed", "status": "accepted", "topic": Topics.from_env().raw, "event_id": event.event_id}


@router.get("/stream/transactions")
async def stream_transactions(request: Request, token: Annotated[str | None, Query()] = None) -> StreamingResponse:
    """Server-Sent Events. Authenticate with ``Authorization: Bearer`` (preferred; the
    frontend uses fetch-streaming) — ``token`` query param is accepted for EventSource
    clients but is discouraged because URLs may be logged."""
    auth = request.headers.get("authorization", "")
    raw = auth[7:] if auth.lower().startswith("bearer ") else token
    if not raw:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "not authenticated")
    decode_access_token(raw)
    rt = get_runtime()
    return StreamingResponse(sse_events(rt.broker, request.is_disconnected, rt.settings.profile),
                             media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


SSE_KEEPALIVE_SECONDS = 15.0


def _frame(msg: dict[str, Any]) -> str:
    return f"event: {msg.get('type', 'transaction')}\ndata: {json.dumps(msg, default=str)}\n\n"


async def sse_events(broker: Any, is_disconnected: Callable[[], Awaitable[bool]], profile: str,
                     keepalive_seconds: float | None = None) -> AsyncIterator[str]:
    """SSE frames for the live feed.

    * "connected" is sent only once the broker subscription is active, so nothing published
      after a client sees it can be missed.
    * Idle periods produce keep-alive comments and never end the stream. (The previous
      implementation wrapped the broker generator's ``__anext__`` in ``asyncio.wait_for``; the
      timeout CANCELLED the generator, which closed the subscription and ended the stream after
      the first 15 s without traffic — the cause of the intermittent live-feed test failure.)
    """
    keepalive = SSE_KEEPALIVE_SECONDS if keepalive_seconds is None else keepalive_seconds
    agen = broker.subscribe()
    try:
        try:
            first = await agen.__anext__()   # None once subscribed, or an 'unavailable' status message
        except StopAsyncIteration:
            first = None
        yield _frame({"type": "status", "live": "connected", "broker": broker.kind, "profile": profile})
        if first is not None:
            yield _frame(first)
        last_sent = time.monotonic()
        async for msg in agen:
            if await is_disconnected():
                break
            if msg is None:                  # broker idle tick (at least every IDLE_TICK_SECONDS)
                if time.monotonic() - last_sent >= keepalive:
                    yield ": keep-alive\n\n"
                    last_sent = time.monotonic()
                continue
            yield _frame(msg)
            last_sent = time.monotonic()
    finally:
        await agen.aclose()
