from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import func, or_, select
from sqlalchemy.orm import joinedload

from app.api.params import Page
from app.core.runtime import get_runtime
from app.investigations import service as inv_svc
from app.models.entities import (
    Alert,
    AuditLog,
    Investigation,
    InvestigationNote,
    Prediction,
    Transaction,
    User,
)
from app.schemas.api import (
    AlertStatusUpdate,
    AssignRequest,
    InvestigationCreate,
    InvestigationStatusUpdate,
    LinkAlertRequest,
    NoteCreate,
    alert_out,
    audit_out,
    investigation_out,
    note_out,
    prediction_out,
    transaction_out,
)
from app.security.auth import DB, CurrentUser

router = APIRouter(tags=["alerts & investigations"])
ALERT_SORT = {"created_at": Alert.created_at, "risk_score": Alert.risk_score, "updated_at": Alert.updated_at}


def _user(db: DB, user_id: int | None) -> User | None:
    if user_id is None:
        return None
    u = db.get(User, user_id)
    if u is None or not u.is_active:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "assignee not found or inactive")
    return u


def _audit_for(db: DB, entity_type: str, entity_id: str) -> list[dict[str, Any]]:
    rows = db.scalars(select(AuditLog).where(AuditLog.entity_type == entity_type, AuditLog.entity_id == entity_id)
                      .order_by(AuditLog.created_at, AuditLog.id))
    return [audit_out(a) for a in rows]


# ------------------------------------------------------------------ alerts
@router.get("/alerts")
def list_alerts(
    user: CurrentUser, db: DB,
    page: Page = 1, size: Annotated[int, Query(ge=1, le=200)] = 25,
    status_: Annotated[str | None, Query(alias="status")] = None,
    risk_category: Literal["MEDIUM", "HIGH"] | None = None,
    detection_method: str | None = None, customer_id: str | None = None,
    assigned: Literal["me", "unassigned", "any"] = "any",
    q: Annotated[str | None, Query(max_length=64)] = None,
    rule: str | None = None, since: datetime | None = None,
    sort: Literal["created_at", "risk_score", "updated_at"] = "created_at",
    order: Literal["asc", "desc"] = "desc",
) -> dict[str, Any]:
    stmt = select(Alert).join(Transaction, Transaction.id == Alert.transaction_id)
    if status_:
        stmt = stmt.where(Alert.status.in_(status_.split(",")))
    if risk_category:
        stmt = stmt.where(Alert.risk_category == risk_category)
    if detection_method:
        stmt = stmt.where(Alert.detection_method == detection_method)
    if customer_id:
        stmt = stmt.where(Alert.customer_id == customer_id)
    if assigned == "me":
        stmt = stmt.where(Alert.assigned_to_id == user.id)
    elif assigned == "unassigned":
        stmt = stmt.where(Alert.assigned_to_id.is_(None))
    if q:
        like = f"%{q}%"
        stmt = stmt.where(or_(Alert.alert_uid.ilike(like), Alert.customer_id.ilike(like),
                              Transaction.transaction_id.ilike(like), Alert.explanation.ilike(like)))
    if rule:
        stmt = stmt.where(Alert.explanation.ilike(f"%{rule}%"))
    if since:
        stmt = stmt.where(Alert.created_at >= since)
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    col = ALERT_SORT[sort]
    stmt = stmt.order_by(col.desc() if order == "desc" else col.asc(), Alert.id.desc())
    rows = db.scalars(stmt.offset((page - 1) * size).limit(size)).unique()
    return {"items": [alert_out(a) for a in rows], "total": total, "page": page, "size": size}


@router.get("/alerts/summary")
def alerts_summary(user: CurrentUser, db: DB) -> dict[str, Any]:
    by_status = dict(db.execute(select(Alert.status, func.count()).group_by(Alert.status)).all())
    by_cat = dict(db.execute(select(Alert.risk_category, func.count()).group_by(Alert.risk_category)).all())
    mine = db.scalar(select(func.count()).where(Alert.assigned_to_id == user.id,
                                                Alert.status.in_(["NEW", "UNDER_REVIEW"]))) or 0
    return {"by_status": by_status, "by_category": by_cat, "open_assigned_to_me": mine}


@router.get("/alerts/{alert_uid}")
def get_alert(alert_uid: str, user: CurrentUser, db: DB) -> dict[str, Any]:
    a = inv_svc.get_alert(db, alert_uid)
    out = alert_out(a)
    t = a.transaction
    out["transaction"] = transaction_out(t)
    out["prediction"] = prediction_out(db.scalar(select(Prediction).where(Prediction.transaction_id == t.id)))
    out["notes"] = [note_out(n) for n in db.scalars(select(InvestigationNote).where(InvestigationNote.alert_id == a.id)
                                                    .order_by(InvestigationNote.created_at))]
    out["audit"] = _audit_for(db, "alert", a.alert_uid)
    out["investigation"] = investigation_out(db.get(Investigation, a.investigation_id)) if a.investigation_id else None
    return out


@router.patch("/alerts/{alert_uid}/status")
def update_alert_status(alert_uid: str, body: AlertStatusUpdate, user: CurrentUser, db: DB) -> dict[str, Any]:
    a = inv_svc.get_alert(db, alert_uid)
    inv_svc.change_alert_status(db, a, body.status, user, body.note)
    db.commit()
    db.refresh(a)  # reload relationships (e.g. implicit assignee) before serialising
    return alert_out(a)


@router.post("/alerts/{alert_uid}/assign")
def assign_alert(alert_uid: str, body: AssignRequest, user: CurrentUser, db: DB) -> dict[str, Any]:
    a = inv_svc.get_alert(db, alert_uid)
    inv_svc.assign_alert(db, a, _user(db, body.user_id), user)
    db.commit()
    db.refresh(a)
    return alert_out(a)


@router.post("/alerts/{alert_uid}/notes", status_code=201)
def add_alert_note(alert_uid: str, body: NoteCreate, user: CurrentUser, db: DB) -> dict[str, Any]:
    a = inv_svc.get_alert(db, alert_uid)
    n = inv_svc.add_note(db, user, body.body, alert=a)
    db.commit()
    return note_out(n)


@router.get("/alerts/{alert_uid}/audit")
def alert_audit(alert_uid: str, user: CurrentUser, db: DB) -> list[dict[str, Any]]:
    a = inv_svc.get_alert(db, alert_uid)
    return _audit_for(db, "alert", a.alert_uid)


# ------------------------------------------------------------------ investigations
@router.get("/investigations")
def list_investigations(user: CurrentUser, db: DB, page: Page = 1,
                        size: Annotated[int, Query(ge=1, le=100)] = 25,
                        status_: Annotated[str | None, Query(alias="status")] = None,
                        assigned: Literal["me", "any"] = "any", customer_id: str | None = None) -> dict[str, Any]:
    stmt = select(Investigation)
    if status_:
        stmt = stmt.where(Investigation.status.in_(status_.split(",")))
    if assigned == "me":
        stmt = stmt.where(Investigation.assigned_to_id == user.id)
    if customer_id:
        stmt = stmt.where(Investigation.customer_id == customer_id)
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = db.scalars(stmt.order_by(Investigation.updated_at.desc()).offset((page - 1) * size).limit(size)).unique()
    return {"items": [investigation_out(i) for i in rows], "total": total, "page": page, "size": size}


@router.post("/investigations", status_code=201)
def create_investigation(body: InvestigationCreate, user: CurrentUser, db: DB) -> dict[str, Any]:
    alerts = [inv_svc.get_alert(db, uid) for uid in dict.fromkeys(body.alert_uids)]
    inv = inv_svc.open_investigation(db, user, alerts, body.title, body.priority, _user(db, body.assigned_to_id))
    db.commit()
    db.refresh(inv)
    return investigation_out(inv, full=True)


@router.get("/investigations/{case_uid}")
def get_investigation(case_uid: str, user: CurrentUser, db: DB) -> dict[str, Any]:
    inv = inv_svc.get_investigation(db, case_uid)
    out = investigation_out(inv, full=True)
    out["notes"] = [note_out(n) for n in db.scalars(
        select(InvestigationNote).where(InvestigationNote.investigation_id == inv.id).order_by(InvestigationNote.created_at))]
    out["audit"] = _audit_for(db, "investigation", inv.case_uid)
    txs = db.scalars(select(Transaction).where(Transaction.customer_id == inv.customer_id)
                     .options(joinedload(Transaction.prediction)).order_by(Transaction.event_time.desc()).limit(50)).unique()
    out["customer_transactions"] = [transaction_out(t) for t in txs]
    other = db.scalars(select(Alert).where(Alert.customer_id == inv.customer_id,
                                           or_(Alert.investigation_id.is_(None), Alert.investigation_id != inv.id))
                       .order_by(Alert.created_at.desc()).limit(25))
    out["related_alerts"] = [alert_out(a) for a in other]
    return out


@router.patch("/investigations/{case_uid}/status")
def update_investigation_status(case_uid: str, body: InvestigationStatusUpdate, user: CurrentUser, db: DB) -> dict[str, Any]:
    inv = inv_svc.get_investigation(db, case_uid)
    inv_svc.change_investigation_status(db, inv, body.status, user, body.final_classification, body.findings)
    db.commit()
    db.refresh(inv)
    return investigation_out(inv, full=True)


@router.post("/investigations/{case_uid}/assign")
def assign_investigation(case_uid: str, body: AssignRequest, user: CurrentUser, db: DB) -> dict[str, Any]:
    inv = inv_svc.get_investigation(db, case_uid)
    assignee = _user(db, body.user_id)
    if assignee is None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "user_id is required")
    inv_svc.assign_investigation(db, inv, assignee, user)
    db.commit()
    db.refresh(inv)
    return investigation_out(inv)


@router.post("/investigations/{case_uid}/notes", status_code=201)
def add_investigation_note(case_uid: str, body: NoteCreate, user: CurrentUser, db: DB) -> dict[str, Any]:
    inv = inv_svc.get_investigation(db, case_uid)
    n = inv_svc.add_note(db, user, body.body, investigation=inv)
    db.commit()
    return note_out(n)


@router.post("/investigations/{case_uid}/alerts")
def link_alert(case_uid: str, body: LinkAlertRequest, user: CurrentUser, db: DB) -> dict[str, Any]:
    inv = inv_svc.get_investigation(db, case_uid)
    inv_svc.link_alert(db, inv, inv_svc.get_alert(db, body.alert_uid), user)
    db.commit()
    db.refresh(inv)
    return investigation_out(inv, full=True)


# ------------------------------------------------------------------ customers
@router.get("/customers/{customer_id}", tags=["customers"])
def customer_profile(customer_id: str, user: CurrentUser, db: DB) -> dict[str, Any]:
    n = db.scalar(select(func.count()).where(Transaction.customer_id == customer_id)) or 0
    if n == 0:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "customer has no transactions")
    agg = db.execute(select(func.avg(Transaction.amount), func.max(Transaction.amount), func.min(Transaction.event_time),
                            func.max(Transaction.event_time), func.count(func.distinct(Transaction.device_id)),
                            func.count(func.distinct(Transaction.city)))
                     .where(Transaction.customer_id == customer_id)).one()
    cats = db.execute(select(Transaction.merchant_category, func.count()).where(Transaction.customer_id == customer_id)
                      .group_by(Transaction.merchant_category).order_by(func.count().desc())).all()
    risk = db.execute(select(Prediction.risk_category, func.count()).join(Transaction)
                      .where(Transaction.customer_id == customer_id).group_by(Prediction.risk_category)).all()
    txs = db.scalars(select(Transaction).where(Transaction.customer_id == customer_id)
                     .options(joinedload(Transaction.prediction)).order_by(Transaction.event_time.desc()).limit(100)).unique()
    alerts = db.scalars(select(Alert).where(Alert.customer_id == customer_id).order_by(Alert.created_at.desc()).limit(50))
    rt = get_runtime()
    return {
        "customer_id": customer_id, "synthetic": True, "transaction_count": n,
        "avg_amount": float(agg[0] or 0), "max_amount": float(agg[1] or 0), "first_seen": agg[2], "last_seen": agg[3],
        "distinct_devices": agg[4], "distinct_cities": agg[5],
        "categories": [{"category": c, "count": k} for c, k in cats],
        "risk_distribution": {c: k for c, k in risk},
        "transactions": [transaction_out(t) for t in txs], "alerts": [alert_out(a) for a in alerts],
        "feature_state": (rt.feature_store.snapshot(customer_id) if rt.settings.profile == "local" else None),
        "feature_state_note": ("in-process feature store snapshot" if rt.settings.profile == "local"
                               else "feature state lives in the Spark state store (not queryable from the API)"),
    }
