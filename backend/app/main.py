"""FraudStream API entry point."""
from __future__ import annotations

import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.routes import alerts, analytics, auth, models, reports, simulation, transactions
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging, log_event
from app.core.runtime import Runtime, get_runtime, set_runtime
from app.database.session import SessionLocal, get_engine, init_engine
from app.models.entities import Role
from app.security.auth import CurrentUser, decode_access_token
from app.services.model_registry import sync_registry
from app.services.runtime_config import seed_defaults
from app.services.streaming_status import detect_stream_processor

log = logging.getLogger("fraudstream.api")
API_PREFIX = "/api/v1"
VERSION = "1.0.0"


def bootstrap(settings: Settings, *, migrate: bool = True) -> Runtime:
    """Initialise DB, seed reference data, load models, warm the feature store."""
    init_engine(settings.database_url)
    if migrate:
        from app.database.migrate import upgrade

        upgrade(settings.database_url)
    db = SessionLocal()
    try:
        for name, desc in (("ADMIN", "Full access incl. configuration, models and users"),
                           ("ANALYST", "Alert triage and investigations")):
            if db.query(Role).filter_by(name=name).first() is None:
                db.add(Role(name=name, description=desc))
        db.commit()
        seed_defaults(db)
        settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
        added = sync_registry(db, settings.artifacts_dir)
        rt = Runtime(settings)
        set_runtime(rt)
        rt.reload(db)
        if settings.profile == "local":
            n = rt.warm_feature_store(db)
            log_event(log, "feature store warm start", events=n)
        log_event(log, "startup complete", profile=settings.profile, models_synced=added,
                  model_loaded=rt.scorer.supervised.label if rt.scorer.supervised else None, model_error=rt.model_error)
        return rt
    finally:
        db.close()


def _rate_limit_key(request: Request) -> str:
    """Per-token bucket for a VALID access token (as before); otherwise per client address. Phase 12: an invalid
    or random bearer value used to get a fresh bucket of its own, so garbage tokens bypassed the limit."""
    auth = request.headers.get("authorization", "")
    if auth[:7].lower() == "bearer ":
        try:
            decode_access_token(auth[7:])
            return "t:" + auth[-24:]
        except HTTPException:
            pass
    return "ip:" + (request.client.host if request.client else "?")


def create_app(settings: Settings | None = None, *, migrate: bool = True) -> FastAPI:
    settings = settings or get_settings()
    configure_logging()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        rt = bootstrap(settings, migrate=migrate)
        yield
        rt.shutdown()
        set_runtime(None)

    app = FastAPI(title="FraudStream API", version=VERSION, lifespan=lifespan,
                  description="Financial Intelligence & Risk Operations Center — educational fraud-detection "
                              "platform operating on SYNTHETIC transactions only.",
                  docs_url="/api/docs", redoc_url="/api/redoc", openapi_url="/api/openapi.json")
    app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origin_list, allow_credentials=True,
                       allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
                       allow_headers=["Authorization", "Content-Type"])

    @app.middleware("http")
    async def request_context(request: Request, call_next: Any) -> Any:
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        t0 = time.perf_counter()
        path = request.url.path
        if path.startswith(API_PREFIX) and not path.endswith("/auth/login") and not path.endswith("/health"):
            try:
                rt = get_runtime()
                key = _rate_limit_key(request)
                ok, retry = rt.limiter.hit(f"api:{key}", rt.settings.api_rate_limit_per_minute)
                if not ok:
                    return JSONResponse({"detail": "rate limit exceeded"}, status_code=429,
                                        headers={"Retry-After": str(retry), "X-Request-ID": rid})
            except RuntimeError:
                pass
        response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        if response.status_code >= 500:
            log_event(log, "request failed", logging.ERROR, path=path, status=response.status_code, rid=rid)
        elif not path.endswith("/stream/transactions"):
            log_event(log, "request", logging.DEBUG, path=path, status=response.status_code,
                      ms=round((time.perf_counter() - t0) * 1000, 1))
        return response

    @app.exception_handler(StarletteHTTPException)
    async def http_exc(_r: Request, exc: StarletteHTTPException) -> JSONResponse:
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code, headers=getattr(exc, "headers", None))

    @app.exception_handler(RequestValidationError)
    async def validation_exc(_r: Request, exc: RequestValidationError) -> JSONResponse:
        errs = [{"loc": list(e.get("loc", ())), "msg": e.get("msg"), "type": e.get("type")} for e in exc.errors()]
        return JSONResponse({"detail": "validation error", "errors": errs}, status_code=422)

    @app.exception_handler(Exception)
    async def unhandled(_r: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error")
        return JSONResponse({"detail": "internal server error"}, status_code=500)

    api = APIRouter(prefix=API_PREFIX)

    @api.get("/health", tags=["health"])
    def health() -> JSONResponse:
        """Liveness/readiness. Public; reveals no configuration details."""
        try:
            rt = get_runtime()
        except RuntimeError:
            return JSONResponse({"status": "starting"}, status_code=503)
        db_ok = True
        try:
            with get_engine().connect() as c:
                c.execute(text("SELECT 1"))
        except Exception:  # noqa: BLE001
            db_ok = False
        ok = db_ok
        processor = "in-process" if rt.settings.profile == "local" else detect_stream_processor(rt.redis)
        body = {"status": "ok" if ok and rt.scorer.ready else ("degraded" if ok else "down"),
                "database": "up" if db_ok else "down", "model_loaded": rt.scorer.ready, "version": VERSION,
                "profile": rt.settings.profile, "stream_processor": processor,
                "time": datetime.now(timezone.utc).isoformat()}
        return JSONResponse(body, status_code=200 if ok else 503)

    @api.get("/system/status", tags=["health"])
    def system_status(user: CurrentUser) -> dict[str, Any]:
        rt = get_runtime()
        s = rt.settings
        if s.profile == "local":
            desc = "LOCAL DEVELOPMENT MODE: in-process pipeline (no Kafka/Spark), " + ("SQLite" if s.is_sqlite else "PostgreSQL")
            processor = "in-process"
        else:
            processor = detect_stream_processor(rt.redis)
            desc = f"FULL DISTRIBUTED MODE: Kafka + PostgreSQL + Redis; stream processor: {processor}"
        return {"profile": s.profile, "profile_description": desc, "stream_processor": processor,
                "environment": s.env, "database": "sqlite" if s.is_sqlite else "postgresql",
                "redis": ("up" if rt.redis_ok() else "down") if s.redis_url else "not configured",
                "live_broker": rt.broker.kind, "rate_limiter": rt.limiter.backend,
                "model": {"supervised": rt.scorer.supervised.label if rt.scorer.supervised else None,
                          "anomaly": rt.scorer.anomaly.label if rt.scorer.anomaly else None, "error": rt.model_error},
                "runtime_config_publish": rt.config_publish_status, "started_at": rt.started_at.isoformat(),
                "version": VERSION}

    for r in (auth.router, transactions.router, simulation.router, alerts.router, models.router, analytics.router, reports.router):
        api.include_router(r)
    app.include_router(api)

    @app.exception_handler(HTTPException)
    async def fastapi_http_exc(_r: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers)

    return app


