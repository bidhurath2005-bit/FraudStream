"""Alert triage and investigation workflow. Every change writes an audit row in the SAME
database transaction as the change itself (callers commit once)."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.entities import Alert, Investigation, InvestigationNote, User
from app.services import audit

ALERT_TRANSITIONS: dict[str, set[str]] = {
    "NEW": {"UNDER_REVIEW", "DISMISSED"},
    "UNDER_REVIEW": {"CONFIRMED_SUSPICIOUS", "DISMISSED", "NEW"},
    "CONFIRMED_SUSPICIOUS": {"CLOSED", "UNDER_REVIEW"},
    "DISMISSED": {"CLOSED", "UNDER_REVIEW"},
    "CLOSED": {"UNDER_REVIEW"},             # re-open: ADMIN only
}
INVESTIGATION_TRANSITIONS: dict[str, set[str]] = {
    "OPEN": {"IN_PROGRESS", "CLOSED"},
    "IN_PROGRESS": {"RESOLVED", "OPEN"},
    "RESOLVED": {"CLOSED", "IN_PROGRESS"},
    "CLOSED": {"IN_PROGRESS"},              # re-open: ADMIN only
}
FINAL_CLASSIFICATIONS = ("CONFIRMED_SUSPICIOUS", "NOT_SUSPICIOUS", "INCONCLUSIVE")


def _is_admin(u: User) -> bool:
    return u.role.name == "ADMIN"


def _forbid(msg: str) -> HTTPException:
    return HTTPException(status.HTTP_403_FORBIDDEN, msg)


def _check_alert_actor(alert: Alert, actor: User) -> None:
    if not _is_admin(actor) and alert.assigned_to_id not in (None, actor.id):
        raise _forbid("alert is assigned to another analyst")


def change_alert_status(db: Session, alert: Alert, new: str, actor: User, note: str | None = None) -> Alert:
    old = alert.status
    if new == old:
        return alert
    if new not in ALERT_TRANSITIONS.get(old, set()):
        raise HTTPException(status.HTTP_409_CONFLICT, f"illegal alert transition {old} -> {new}")
    if old == "CLOSED" and not _is_admin(actor):
        raise _forbid("only administrators can re-open closed alerts")
    _check_alert_actor(alert, actor)
    alert.status = new
    if new == "UNDER_REVIEW" and alert.assigned_to_id is None:
        alert.assigned_to_id = actor.id
        audit.record(db, actor, "ALERT_ASSIGNED", "alert", alert.alert_uid, field="assigned_to", old=None,
                     new=actor.username, details={"implicit": True})
    audit.record(db, actor, "ALERT_STATUS_CHANGED", "alert", alert.alert_uid, field="status", old=old, new=new)
    if note:
        add_note(db, actor, body=note, alert=alert)
    return alert


def assign_alert(db: Session, alert: Alert, assignee: User | None, actor: User) -> Alert:
    if assignee is not None and assignee.id != actor.id and not _is_admin(actor):
        raise _forbid("only administrators can assign alerts to other analysts")
    if assignee is None and not _is_admin(actor) and alert.assigned_to_id != actor.id:
        raise _forbid("cannot unassign an alert assigned to someone else")
    old = alert.assigned_to.username if alert.assigned_to else None
    alert.assigned_to_id = assignee.id if assignee else None
    audit.record(db, actor, "ALERT_ASSIGNED", "alert", alert.alert_uid, field="assigned_to", old=old,
                 new=assignee.username if assignee else None)
    return alert


def add_note(db: Session, actor: User, body: str, *, alert: Alert | None = None,
             investigation: Investigation | None = None) -> InvestigationNote:
    body = body.strip()
    if not body:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "note body is empty")
    note = InvestigationNote(alert_id=alert.id if alert else None,
                             investigation_id=investigation.id if investigation else None,
                             author_id=actor.id, body=body)
    db.add(note)
    db.flush()
    target = ("alert", alert.alert_uid) if alert else ("investigation", investigation.case_uid if investigation else "?")
    audit.record(db, actor, "NOTE_ADDED", target[0], target[1], details={"note_id": note.id, "length": len(body)})
    return note


def open_investigation(db: Session, actor: User, alerts: list[Alert], title: str, priority: str,
                       assignee: User | None) -> Investigation:
    if not alerts:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "at least one alert is required")
    customers = {a.customer_id for a in alerts}
    if len(customers) != 1:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "all alerts must belong to the same customer")
    if any(a.investigation_id is not None for a in alerts):
        raise HTTPException(status.HTTP_409_CONFLICT, "an alert is already linked to an investigation")
    if assignee is not None and assignee.id != actor.id and not _is_admin(actor):
        raise _forbid("only administrators can assign investigations to other analysts")
    inv = Investigation(case_uid=str(uuid.uuid4()), title=title.strip()[:200], customer_id=customers.pop(),
                        status="OPEN", priority=priority, opened_by_id=actor.id,
                        assigned_to_id=(assignee or actor).id)
    db.add(inv)
    db.flush()
    audit.record(db, actor, "INVESTIGATION_OPENED", "investigation", inv.case_uid, field="status", new="OPEN",
                 details={"alerts": [a.alert_uid for a in alerts], "priority": priority})
    for a in alerts:
        a.investigation_id = inv.id
        audit.record(db, actor, "ALERT_LINKED", "alert", a.alert_uid, field="investigation", new=inv.case_uid)
        if a.status == "NEW":
            change_alert_status(db, a, "UNDER_REVIEW", actor)
    return inv


def _check_inv_actor(inv: Investigation, actor: User) -> None:
    if not _is_admin(actor) and inv.assigned_to_id not in (None, actor.id):
        raise _forbid("investigation is assigned to another analyst")


def change_investigation_status(db: Session, inv: Investigation, new: str, actor: User,
                                final_classification: str | None = None, findings: str | None = None) -> Investigation:
    old = inv.status
    if new == old:
        return inv
    if new not in INVESTIGATION_TRANSITIONS.get(old, set()):
        raise HTTPException(status.HTTP_409_CONFLICT, f"illegal investigation transition {old} -> {new}")
    if old == "CLOSED" and not _is_admin(actor):
        raise _forbid("only administrators can re-open closed investigations")
    _check_inv_actor(inv, actor)
    if new == "RESOLVED":
        if final_classification not in FINAL_CLASSIFICATIONS:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT,
                                f"final_classification must be one of {FINAL_CLASSIFICATIONS}")
        if not findings or len(findings.strip()) < 10:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "findings (>= 10 characters) are required")
        audit.record(db, actor, "INVESTIGATION_FINDINGS", "investigation", inv.case_uid,
                     field="final_classification", old=inv.final_classification, new=final_classification)
        inv.final_classification, inv.findings = final_classification, findings.strip()
        # propagate the analyst's decision to linked alerts (never automatic from model scores)
        target = {"CONFIRMED_SUSPICIOUS": "CONFIRMED_SUSPICIOUS", "NOT_SUSPICIOUS": "DISMISSED"}.get(final_classification)
        if target:
            for a in inv.alerts:
                if a.status == "NEW":
                    change_alert_status(db, a, "UNDER_REVIEW", actor)
                if a.status == "UNDER_REVIEW":
                    change_alert_status(db, a, target, actor)
    inv.status = new
    inv.closed_at = datetime.now(timezone.utc) if new == "CLOSED" else None
    audit.record(db, actor, "INVESTIGATION_STATUS_CHANGED", "investigation", inv.case_uid, field="status",
                 old=old, new=new)
    return inv


def assign_investigation(db: Session, inv: Investigation, assignee: User, actor: User) -> Investigation:
    if assignee.id != actor.id and not _is_admin(actor):
        raise _forbid("only administrators can assign investigations to other analysts")
    old = inv.assigned_to.username if inv.assigned_to else None
    inv.assigned_to_id = assignee.id
    audit.record(db, actor, "INVESTIGATION_ASSIGNED", "investigation", inv.case_uid, field="assigned_to",
                 old=old, new=assignee.username)
    return inv


def link_alert(db: Session, inv: Investigation, alert: Alert, actor: User) -> None:
    _check_inv_actor(inv, actor)
    if alert.customer_id != inv.customer_id:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "alert belongs to a different customer")
    if alert.investigation_id not in (None, inv.id):
        raise HTTPException(status.HTTP_409_CONFLICT, "alert already linked to another investigation")
    alert.investigation_id = inv.id
    audit.record(db, actor, "ALERT_LINKED", "alert", alert.alert_uid, field="investigation", new=inv.case_uid)


def get_alert(db: Session, alert_uid: str) -> Alert:
    a = db.scalar(select(Alert).where(Alert.alert_uid == alert_uid))
    if a is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "alert not found")
    return a


def get_investigation(db: Session, case_uid: str) -> Investigation:
    inv = db.scalar(select(Investigation).where(Investigation.case_uid == case_uid))
    if inv is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "investigation not found")
    return inv
