"""Generated reports (Phase 11C): catalogue, generation, history, preview and CSV/PDF download.

The older download endpoints (/reports/model-evaluation, /reports/alerts.csv, /reports/performance,
/reports/benchmarks) live in analytics.py and are unchanged.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.orm import defer

from app.api.params import Page
from app.core.runtime import get_runtime
from app.models.entities import GeneratedReport, User
from app.security.auth import DB, CurrentUser
from app.services import reports as rep

router = APIRouter(tags=["reports"])


class ReportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    report_type: Literal[
        "fraud_activity", "risk_analytics", "alert_summary", "investigation", "model_performance", "system_activity"
    ]
    start: datetime
    end: datetime
    risk_level: Literal["LOW", "MEDIUM", "HIGH"] | None = None
    alert_status: Literal["NEW", "UNDER_REVIEW", "CONFIRMED_SUSPICIOUS", "DISMISSED", "CLOSED"] | None = None
    investigation_status: Literal["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"] | None = None
    # free-text fields are deliberately absent: a report title is derived from its type


def _is_admin(u: User) -> bool:
    return u.role.name == "ADMIN"


def _allowed(u: User, report_type: str) -> bool:
    spec = rep.REPORT_TYPES.get(report_type)
    return spec is not None and (not spec.admin_only or _is_admin(u))


@router.get("/reports/types")
def report_types(user: CurrentUser) -> dict[str, Any]:
    return {
        "types": [
            {
                "key": t.key,
                "title": t.title,
                "description": t.description,
                "period_basis": t.basis,
                "filters": [{"key": f, "options": list(rep.FILTER_OPTIONS[f])} for f in t.filters],
                "admin_only": t.admin_only,
                "available": _allowed(user, t.key),
            }
            for t in rep.REPORT_TYPES.values()
        ],
        "max_period_days": rep.MAX_PERIOD_DAYS,
        "detail_row_cap": rep.DETAIL_ROW_CAP,
        "pdf_row_cap": rep.PDF_ROW_CAP,
        "preview_rows": rep.PREVIEW_ROWS,
        "persisted": True,
    }


@router.post("/reports", status_code=201)
def create_report(body: ReportRequest, user: CurrentUser, db: DB) -> dict[str, Any]:
    spec = rep.REPORT_TYPES[body.report_type]
    if spec.admin_only and not _is_admin(user):
        raise HTTPException(status.HTTP_403_FORBIDDEN, f"{spec.title} requires the administrator role")
    if body.start.tzinfo is None or body.end.tzinfo is None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "start and end must include a timezone (e.g. Z for UTC)")
    if body.start >= body.end:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "start must be before end")
    if body.end - body.start > timedelta(days=rep.MAX_PERIOD_DAYS):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, f"period must not exceed {rep.MAX_PERIOD_DAYS} days")
    filters = {k: getattr(body, k) for k in rep.FILTER_OPTIONS}
    unsupported = sorted(k for k, v in filters.items() if v and k not in spec.filters)
    if unsupported:  # never pretend to filter: reject filters this report type cannot apply
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"{spec.title} does not support filter(s) {unsupported}; supported: {list(spec.filters) or 'none'}",
        )
    rec = rep.generate(db, user, body.report_type, body.start, body.end, filters, get_runtime().settings.profile)
    return rep.preview(rec)


@router.get("/reports/generated")
def list_reports(
    user: CurrentUser,
    db: DB,
    page: Page = 1,
    size: Annotated[int, Query(ge=1, le=100)] = 20,
    report_type: Annotated[str | None, Query(max_length=32)] = None,
) -> dict[str, Any]:
    stmt = select(GeneratedReport)
    if report_type:
        if report_type not in rep.REPORT_TYPES:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unknown report type")
        stmt = stmt.where(GeneratedReport.report_type == report_type)
    if not _is_admin(user):
        stmt = stmt.where(GeneratedReport.report_type.not_in([t.key for t in rep.REPORT_TYPES.values() if t.admin_only]))
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = db.scalars(
        stmt.options(defer(GeneratedReport.content))
        .order_by(GeneratedReport.created_at.desc(), GeneratedReport.id.desc())
        .offset((page - 1) * size)
        .limit(size)
    )
    return {"items": [rep.report_meta(r) for r in rows], "total": total, "page": page, "size": size}


def _get(db: DB, user: User, uid: str) -> GeneratedReport:
    r = db.scalar(select(GeneratedReport).where(GeneratedReport.report_uid == uid))
    if r is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "report not found")
    if not _allowed(user, r.report_type):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "this report type requires the administrator role")
    return r


@router.get("/reports/generated/{uid}")
def get_report(uid: str, user: CurrentUser, db: DB) -> dict[str, Any]:
    return rep.preview(_get(db, user, uid))


@router.get("/reports/generated/{uid}/download")
def download_report(
    uid: str,
    user: CurrentUser,
    db: DB,
    fmt: Annotated[Literal["csv", "pdf"], Query(alias="format")] = "csv",
    section: Annotated[str | None, Query(max_length=64)] = None,
) -> Response:
    r = _get(db, user, uid)
    if r.status != "COMPLETED" or not r.content:
        raise HTTPException(status.HTTP_409_CONFLICT, "this report failed to generate and has no content to download")
    if section is not None:
        if fmt != "csv":
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "single-section export is available as CSV only")
        if section not in {s["id"] for s in r.content["sections"]}:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "section not found in this report")
    if fmt == "csv":
        data, media = rep.render_csv(r.content, section), "text/csv; charset=utf-8"
    else:
        data, media = rep.render_pdf(r.content), "application/pdf"
    name = rep.safe_filename(r, fmt, section)
    return Response(
        data,
        media_type=media,
        headers={"Content-Disposition": f'attachment; filename="{name}"', "Cache-Control": "no-store"},
    )
