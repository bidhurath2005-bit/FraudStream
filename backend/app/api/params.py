"""Shared bounds for numeric request parameters (Phase 12 input-validation hardening).

Without an upper bound, a huge page number or id reached the database driver and failed there
(SQLite: OverflowError; PostgreSQL: integer out of range) -> HTTP 500. Bounded parameters are rejected
with 422 by FastAPI before any query runs. Primary keys are PostgreSQL `integer` columns.
"""
from __future__ import annotations

from typing import Annotated

from fastapi import Path, Query

MAX_DB_ID = 2_147_483_647          # PostgreSQL integer
MAX_PAGE = 100_000                 # x page size (<= 200) keeps OFFSET far below any integer limit

Page = Annotated[int, Query(ge=1, le=MAX_PAGE)]
IdPath = Annotated[int, Path(ge=1, le=MAX_DB_ID)]


def parse_id_list(raw: str) -> list[int]:
    """Comma-separated ids; raises ValueError for non-integers or out-of-range values."""
    ids = [int(i) for i in raw.split(",")]
    if not all(1 <= i <= MAX_DB_ID for i in ids):
        raise ValueError("id out of range")
    return ids
