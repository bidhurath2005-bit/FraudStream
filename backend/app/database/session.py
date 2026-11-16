"""SQLAlchemy engine/session. Synchronous sessions are used consistently; FastAPI runs
sync endpoints in its threadpool so the event loop is not blocked."""
from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TypeVar

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

T = TypeVar("T")


class Base(DeclarativeBase):
    pass


_engine: Engine | None = None
_SessionLocal: sessionmaker[Session] | None = None


def init_engine(url: str) -> Engine:
    global _engine, _SessionLocal
    if url.startswith("sqlite"):
        from pathlib import Path

        path = url.split("///", 1)[-1]
        if path and path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        eng = create_engine(url, connect_args={"check_same_thread": False, "timeout": 30})

        @event.listens_for(eng, "connect")
        def _pragmas(dbapi_conn, _rec):  # type: ignore[no-untyped-def]
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.close()
    else:
        eng = create_engine(url, pool_size=10, max_overflow=10, pool_pre_ping=True, pool_recycle=1800)
    _engine = eng
    _SessionLocal = sessionmaker(bind=eng, expire_on_commit=False, autoflush=False)
    return eng


def get_engine() -> Engine:
    if _engine is None:
        raise RuntimeError("database engine not initialised")
    return _engine


def SessionLocal() -> Session:  # noqa: N802 - factory
    if _SessionLocal is None:
        raise RuntimeError("database engine not initialised")
    return _SessionLocal()


def get_db() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def is_transient(exc: BaseException) -> bool:
    if isinstance(exc, OperationalError):
        return True
    if isinstance(exc, DBAPIError) and exc.connection_invalidated:
        return True
    msg = str(exc).lower()
    return "database is locked" in msg or "could not connect" in msg


def with_retry(fn: Callable[[], T], attempts: int = 4, base_delay: float = 0.1) -> T:
    """Retry ``fn`` on transient DB errors with exponential backoff; re-raise otherwise."""
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if i == attempts - 1 or not is_transient(exc):
                raise
            time.sleep(base_delay * (2 ** i))
    raise RuntimeError("unreachable")
