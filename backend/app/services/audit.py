"""Audit trail helper. Callers add audit rows inside the same DB transaction as the change."""
from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models.entities import AuditLog, User


def _s(v: Any) -> str | None:
    return None if v is None else str(v)


def record(db: Session, actor: User | None, action: str, entity_type: str, entity_id: Any, *,
           field: str | None = None, old: Any = None, new: Any = None, details: dict[str, Any] | None = None) -> AuditLog:
    row = AuditLog(actor_id=actor.id if actor else None, actor_name=actor.username if actor else "system",
                   action=action, entity_type=entity_type, entity_id=str(entity_id), field=field,
                   old_value=_s(old), new_value=_s(new), details=details or {})
    db.add(row)
    return row
