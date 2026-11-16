"""Report generation from persisted data (Phase 11C).

Every value in a report is computed by a query on the database at generation time; nothing is
estimated or filled in. The generated content is stored (``generated_reports.content``) so that a
later CSV/PDF download reproduces exactly what was generated, instead of silently re-querying data
that may have changed since.

Content layout (shared by the API preview, the CSV and the PDF renderers)::

    {report_uid, report_type, title, generated_at, generated_by, profile,
     period: {start, end, basis, label}, filters: {...},
     summary: [{key, label, value, format, hint?}],
     sections: [{id, title, description, columns: [{key, label, format}], rows: [...],
                 total_rows, truncated, note?}],
     limitations: [...], data_sources: [...]}
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from xml.sax.saxutils import escape

from sqlalchemy import Integer, case, cast, distinct, func, select
from sqlalchemy.orm import Session, aliased

from app.models.entities import (
    Alert,
    AuditLog,
    DriftReport,
    GeneratedReport,
    Investigation,
    InvestigationNote,
    ModelVersion,
    Prediction,
    Transaction,
    User,
)
from app.services import audit
from app.services.runtime_config import get_setting

log = logging.getLogger("fraudstream.reports")

DETAIL_ROW_CAP = 10_000  # rows stored per detail section (alerts, cases, audit events)
PDF_ROW_CAP = 500  # rows printed per section in the PDF (the CSV always has every stored row)
PDF_CELL_CHARS = 400  # free text per PDF cell (a cell taller than a page cannot be laid out)
PREVIEW_ROWS = 100  # rows per section returned to the browser preview
RULE_SCAN_CAP = 20_000  # predictions/alerts examined for rule-trigger counts
LATENCY_SCAN_CAP = 100_000  # inference_ms samples examined per report
MAX_PERIOD_DAYS = 366

ALERT_STATUSES = ("NEW", "UNDER_REVIEW", "CONFIRMED_SUSPICIOUS", "DISMISSED", "CLOSED")
INVESTIGATION_STATUSES = ("OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED")
RISK_LEVELS = ("LOW", "MEDIUM", "HIGH")
SYNTHETIC = (
    "All transactions are SYNTHETIC (simulator output); labels used for detection metrics are simulator ground "
    "truth, not real fraud outcomes."
)
FLAGGED_DEF = "flagged = risk category MEDIUM or HIGH (a detection, not confirmed fraud)"


@dataclass(frozen=True)
class ReportType:
    key: str
    title: str
    description: str
    basis: str
    filters: tuple[str, ...] = ()
    admin_only: bool = False


REPORT_TYPES: dict[str, ReportType] = {
    t.key: t
    for t in (
        ReportType(
            "fraud_activity",
            "Fraud activity report",
            "Transactions persisted in the period: totals, flagged volume, daily activity, merchants and injected patterns.",
            "transactions.processed_at",
            ("risk_level",),
        ),
        ReportType(
            "risk_analytics",
            "Risk analytics report",
            "Risk-score distribution, categories, detection methods, rule triggers and detection quality against simulator labels.",
            "transactions.processed_at",
        ),
        ReportType(
            "alert_summary",
            "Alert summary report",
            "Alerts raised in the period by status, risk, detection method, rule and assignee, with the alert list.",
            "alerts.created_at",
            ("risk_level", "alert_status"),
        ),
        ReportType(
            "investigation",
            "Investigation report",
            "Cases opened in the period: confirmed and unresolved cases, classifications, time to close and the case list.",
            "investigations.created_at",
            ("investigation_status",),
        ),
        ReportType(
            "model_performance",
            "Model performance report",
            "Registered models with offline test metrics, live inference latency and live labelled performance in the period, "
            "and drift checks run in the period.",
            "transactions.processed_at (live) / model registry (offline) / drift_reports.computed_at",
        ),
        ReportType(
            "system_activity",
            "System activity report",
            "Audit trail events in the period: logins, configuration changes, case actions, simulations and reports.",
            "audit_logs.created_at",
            admin_only=True,
        ),
    )
}
FILTER_OPTIONS = {"risk_level": RISK_LEVELS, "alert_status": ALERT_STATUSES, "investigation_status": INVESTIGATION_STATUSES}


# ------------------------------------------------------------------ helpers
def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).astimezone(timezone.utc).isoformat()


def _rate(n: int | float | None, d: int | float | None) -> float | None:
    return (n or 0) / d if d else None


def _pctl(sorted_vals: list[float], p: float) -> float | None:
    if not sorted_vals:
        return None
    return sorted_vals[min(len(sorted_vals) - 1, int(p * len(sorted_vals)))]


def metric(key: str, label: str, value: Any, fmt: str = "int", hint: str | None = None) -> dict[str, Any]:
    return {"key": key, "label": label, "value": value, "format": fmt, **({"hint": hint} if hint else {})}


def section(
    sid: str,
    title: str,
    columns: list[tuple[str, str, str]],
    rows: list[dict[str, Any]],
    *,
    description: str | None = None,
    total_rows: int | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    total = len(rows) if total_rows is None else total_rows
    return {
        "id": sid,
        "title": title,
        "description": description,
        "columns": [{"key": k, "label": lbl, "format": f} for k, lbl, f in columns],
        "rows": rows,
        "total_rows": total,
        "truncated": total > len(rows),
        "note": note,
    }


def _day(db: Session, col):  # type: ignore[no-untyped-def]
    """UTC calendar day of a timestamp column, as text (portable across PostgreSQL / SQLite)."""
    if db.get_bind().dialect.name == "postgresql":
        return func.to_char(func.timezone("UTC", col), "YYYY-MM-DD")
    return func.strftime("%Y-%m-%d", col)


def _hour(db: Session, col):  # type: ignore[no-untyped-def]
    if db.get_bind().dialect.name == "postgresql":
        return cast(func.extract("hour", func.timezone("UTC", col)), Integer)
    return cast(func.strftime("%H", col), Integer)


FLAGGED = case((Prediction.risk_category != "LOW", 1), else_=0)
HIGH = case((Prediction.risk_category == "HIGH", 1), else_=0)


def _tx_window(stmt, start: datetime, end: datetime, risk_level: str | None = None):  # type: ignore[no-untyped-def]
    stmt = stmt.where(Transaction.processed_at >= start, Transaction.processed_at < end)
    if risk_level:
        stmt = stmt.where(Prediction.risk_category == risk_level)
    return stmt


# ------------------------------------------------------------------ builders
def _fraud_activity(db: Session, start: datetime, end: datetime, f: dict[str, Any]) -> dict[str, Any]:
    rl = f.get("risk_level")
    scored = select().select_from(Transaction).join(Prediction)
    tot = db.execute(
        _tx_window(
            scored.add_columns(
                func.count(),
                func.sum(FLAGGED),
                func.sum(HIGH),
                func.count(distinct(Transaction.customer_id)),
                func.count(distinct(Transaction.merchant_id)),
                func.avg(Prediction.risk_score),
            ),
            start,
            end,
            rl,
        )
    ).one()
    n, flagged, high, customers, merchants, avg_risk = (
        int(tot[0] or 0),
        int(tot[1] or 0),
        int(tot[2] or 0),
        int(tot[3] or 0),
        int(tot[4] or 0),
        tot[5],
    )
    persisted = (
        db.scalar(
            select(func.count(Transaction.id)).where(Transaction.processed_at >= start, Transaction.processed_at < end)
        )
        or 0
    )
    al = db.execute(
        _tx_window(
            select(Alert.status, func.count())
            .join(Transaction, Alert.transaction_id == Transaction.id)
            .join(Prediction, Prediction.transaction_id == Transaction.id),
            start,
            end,
            rl,
        )
        .group_by(Alert.status)
        .order_by(Alert.status)
    ).all()
    alerts_total = sum(int(c) for _, c in al)
    confirmed = sum(int(c) for s, c in al if s == "CONFIRMED_SUSPICIOUS")

    cur = db.execute(
        _tx_window(
            scored.add_columns(
                Transaction.currency,
                func.count(),
                func.sum(Transaction.amount),
                func.sum(case((Prediction.risk_category != "LOW", Transaction.amount), else_=0)),
            ),
            start,
            end,
            rl,
        )
        .group_by(Transaction.currency)
        .order_by(Transaction.currency)
    ).all()
    cats = db.execute(
        _tx_window(
            scored.add_columns(Prediction.risk_category, func.count(), func.avg(Prediction.risk_score)), start, end, rl
        ).group_by(Prediction.risk_category)
    ).all()
    order = {c: i for i, c in enumerate(RISK_LEVELS)}
    day = _day(db, Transaction.processed_at).label("d")
    daily = db.execute(
        _tx_window(scored.add_columns(day, func.count(), func.sum(FLAGGED), func.sum(HIGH)), start, end, rl)
        .group_by(day)
        .order_by(day)
    ).all()
    mcat = db.execute(
        _tx_window(
            scored.add_columns(
                Transaction.merchant_category, func.count(), func.sum(FLAGGED), func.avg(Prediction.risk_score)
            ),
            start,
            end,
            rl,
        )
        .group_by(Transaction.merchant_category)
        .order_by(func.sum(FLAGGED).desc(), func.count().desc(), Transaction.merchant_category)
    ).all()
    top = db.execute(
        _tx_window(
            scored.add_columns(
                Transaction.merchant_id,
                func.max(Transaction.merchant_category),
                func.count(),
                func.sum(FLAGGED),
                func.max(Prediction.risk_score),
            ),
            start,
            end,
            rl,
        )
        .group_by(Transaction.merchant_id)
        .order_by(func.sum(FLAGGED).desc(), func.count().desc(), Transaction.merchant_id)
        .limit(25)
    ).all()
    pats = db.execute(
        _tx_window(
            scored.add_columns(
                Transaction.sim_scenario,
                func.count(),
                func.sum(case((Transaction.sim_is_suspicious.is_(True), 1), else_=0)),
                func.sum(FLAGGED),
            ),
            start,
            end,
            rl,
        )
        .where(Transaction.sim_scenario.is_not(None))
        .group_by(Transaction.sim_scenario)
        .order_by(func.count().desc(), Transaction.sim_scenario)
    ).all()
    scope = f" with risk level {rl}" if rl else ""
    return {
        "summary": [
            metric(
                "transactions_persisted",
                "Transactions persisted",
                persisted,
                hint="all transactions in the period" + (" (before the risk-level filter)" if rl else ""),
            ),
            metric("transactions_scored", f"Scored transactions{scope}", n),
            metric("flagged", "Flagged (MEDIUM/HIGH)", flagged),
            metric("flag_rate", "Flag rate", _rate(flagged, n), "pct"),
            metric("high", "HIGH risk", high),
            metric("alerts", "Alerts on these transactions", alerts_total),
            metric("alerts_confirmed", "Alerts confirmed suspicious by analysts", confirmed),
            metric("customers", "Distinct customers", customers),
            metric("merchants", "Distinct merchants", merchants),
            metric("avg_risk", "Average risk score", avg_risk, "score"),
        ],
        "sections": [
            section(
                "amounts",
                "Amounts by currency",
                [
                    ("currency", "Currency", "text"),
                    ("transactions", "Transactions", "int"),
                    ("amount", "Total amount", "money"),
                    ("flagged_amount", "Flagged amount", "money"),
                ],
                [
                    {"currency": c, "transactions": int(k), "amount": float(a or 0), "flagged_amount": float(fa or 0)}
                    for c, k, a, fa in cur
                ],
                description="Amounts are summed per currency, never across currencies.",
            ),
            section(
                "risk_categories",
                "Risk categories",
                [
                    ("category", "Category", "text"),
                    ("transactions", "Transactions", "int"),
                    ("share", "Share", "pct"),
                    ("avg_risk", "Avg risk", "score"),
                ],
                [
                    {"category": c, "transactions": int(k), "share": _rate(k, n), "avg_risk": a}
                    for c, k, a in sorted(cats, key=lambda r: order.get(r[0], 9))
                ],
            ),
            section(
                "daily",
                "Daily activity (UTC)",
                [
                    ("day", "Day (UTC)", "text"),
                    ("transactions", "Transactions", "int"),
                    ("flagged", "Flagged", "int"),
                    ("high", "HIGH", "int"),
                    ("flag_rate", "Flag rate", "pct"),
                ],
                [
                    {
                        "day": d,
                        "transactions": int(k),
                        "flagged": int(fl or 0),
                        "high": int(h or 0),
                        "flag_rate": _rate(fl, k),
                    }
                    for d, k, fl, h in daily
                ],
                description="Days without persisted transactions are omitted.",
            ),
            section(
                "alert_status",
                "Alerts on these transactions by status",
                [("status", "Status", "text"), ("alerts", "Alerts", "int")],
                [{"status": s, "alerts": int(c)} for s, c in al],
            ),
            section(
                "merchant_categories",
                "Merchant categories",
                [
                    ("category", "Category", "text"),
                    ("transactions", "Transactions", "int"),
                    ("flagged", "Flagged", "int"),
                    ("flag_rate", "Flag rate", "pct"),
                    ("avg_risk", "Avg risk", "score"),
                ],
                [
                    {
                        "category": c,
                        "transactions": int(k),
                        "flagged": int(fl or 0),
                        "flag_rate": _rate(fl, k),
                        "avg_risk": a,
                    }
                    for c, k, fl, a in mcat
                ],
            ),
            section(
                "top_merchants",
                "Top 25 merchants by flagged transactions",
                [
                    ("merchant_id", "Merchant id", "text"),
                    ("category", "Category", "text"),
                    ("transactions", "Transactions", "int"),
                    ("flagged", "Flagged", "int"),
                    ("flag_rate", "Flag rate", "pct"),
                    ("max_risk", "Max risk", "score"),
                ],
                [
                    {
                        "merchant_id": m,
                        "category": c,
                        "transactions": int(k),
                        "flagged": int(fl or 0),
                        "flag_rate": _rate(fl, k),
                        "max_risk": mx,
                    }
                    for m, c, k, fl, mx in top
                ],
                description="Ranked by flagged count, then volume, then id. Merchant names are not stored.",
            ),
            section(
                "patterns",
                "Simulated patterns (scenarios)",
                [
                    ("scenario", "Scenario", "text"),
                    ("transactions", "Transactions", "int"),
                    ("labelled_suspicious", "Labelled suspicious", "int"),
                    ("flagged", "Flagged", "int"),
                    ("flag_rate", "Flag rate", "pct"),
                ],
                [
                    {
                        "scenario": s,
                        "transactions": int(k),
                        "labelled_suspicious": int(ls or 0),
                        "flagged": int(fl or 0),
                        "flag_rate": _rate(fl, k),
                    }
                    for s, k, ls, fl in pats
                ],
                description="Scenario and label are simulator metadata (synthetic ground truth).",
            ),
        ],
        "limitations": [
            SYNTHETIC,
            FLAGGED_DEF + ".",
            "Alerts are counted for the transactions in the period (by transaction processing time), whatever "
            "the alert's own creation time.",
            "Risk categories are those assigned when each transaction was scored, under the thresholds active then.",
        ],
    }


def _risk_analytics(db: Session, start: datetime, end: datetime, f: dict[str, Any]) -> dict[str, Any]:
    scored = select().select_from(Transaction).join(Prediction)
    tot = db.execute(
        _tx_window(
            scored.add_columns(
                func.count(), func.sum(FLAGGED), func.avg(Prediction.risk_score), func.max(Prediction.risk_score)
            ),
            start,
            end,
        )
    ).one()
    n, flagged, avg_risk, max_risk = int(tot[0] or 0), int(tot[1] or 0), tot[2], tot[3]
    b = (
        case((Prediction.risk_score >= 90, 9), else_=cast(func.floor(Prediction.risk_score / 10), Integer))
        if db.get_bind().dialect.name == "postgresql"
        else case((Prediction.risk_score >= 90, 9), else_=cast(Prediction.risk_score / 10, Integer))
    )
    b = b.label("b")
    bins = dict(
        (int(k), int(c))
        for k, c in db.execute(_tx_window(scored.add_columns(b, func.count()), start, end).group_by(b)).all()
    )
    cats = dict(
        db.execute(
            _tx_window(scored.add_columns(Prediction.risk_category, func.count()), start, end).group_by(
                Prediction.risk_category
            )
        ).all()
    )
    methods = db.execute(
        _tx_window(scored.add_columns(Prediction.detection_method, func.count(), func.sum(FLAGGED)), start, end)
        .group_by(Prediction.detection_method)
        .order_by(func.count().desc())
    ).all()
    rules: dict[str, int] = {}
    examined = 0
    for (tr,) in db.execute(
        _tx_window(select(Prediction.triggered_rules).join(Transaction), start, end)
        .where(Prediction.risk_category != "LOW")
        .order_by(Prediction.id.desc())
        .limit(RULE_SCAN_CAP)
    ):
        examined += 1
        for r in tr or []:
            rules[r["code"]] = rules.get(r["code"], 0) + 1
    cm = db.execute(
        _tx_window(
            scored.add_columns(
                func.sum(
                    case(((Transaction.sim_is_suspicious.is_(True)) & (Prediction.risk_category != "LOW"), 1), else_=0)
                ),
                func.sum(
                    case(((Transaction.sim_is_suspicious.is_(False)) & (Prediction.risk_category != "LOW"), 1), else_=0)
                ),
                func.sum(
                    case(((Transaction.sim_is_suspicious.is_(True)) & (Prediction.risk_category == "LOW"), 1), else_=0)
                ),
                func.sum(
                    case(((Transaction.sim_is_suspicious.is_(False)) & (Prediction.risk_category == "LOW"), 1), else_=0)
                ),
            ),
            start,
            end,
        )
    ).one()
    tp, fp, fn, tn = (int(x or 0) for x in cm)
    precision, recall = _rate(tp, tp + fp), _rate(tp, tp + fn)
    h = _hour(db, Transaction.event_time).label("h")
    hours = {
        int(k): (int(c), int(fl or 0))
        for k, c, fl in db.execute(
            _tx_window(scored.add_columns(h, func.count(), func.sum(FLAGGED)), start, end).group_by(h)
        ).all()
    }
    rs = get_setting(db, "risk")
    return {
        "summary": [
            metric("transactions_scored", "Scored transactions", n),
            metric("flagged", "Flagged (MEDIUM/HIGH)", flagged),
            metric("flag_rate", "Flag rate", _rate(flagged, n), "pct"),
            metric("avg_risk", "Average risk score", avg_risk, "score"),
            metric("max_risk", "Maximum risk score", max_risk, "score"),
            metric(
                "precision", "Precision vs simulator labels", precision, "ratio", "flagged that were labelled suspicious"
            ),
            metric("recall", "Recall vs simulator labels", recall, "ratio", "labelled suspicious that were flagged"),
            metric("false_positive_rate", "False-positive rate", _rate(fp, fp + tn), "pct"),
        ],
        "sections": [
            section(
                "score_distribution",
                "Risk-score distribution",
                [("range", "Risk score", "text"), ("transactions", "Transactions", "int"), ("share", "Share", "pct")],
                [
                    {
                        "range": f"{i * 10}–{i * 10 + 10}" if i < 9 else "90–100",
                        "transactions": bins.get(i, 0),
                        "share": _rate(bins.get(i, 0), n),
                    }
                    for i in range(10)
                ]
                if n
                else [],
            ),
            section(
                "risk_categories",
                "Risk categories",
                [("category", "Category", "text"), ("transactions", "Transactions", "int"), ("share", "Share", "pct")],
                [
                    {"category": c, "transactions": int(cats.get(c, 0)), "share": _rate(cats.get(c, 0), n)}
                    for c in RISK_LEVELS
                ]
                if n
                else [],
                description=f"Thresholds active at generation time: MEDIUM ≥ {rs.get('medium_threshold')}, "
                f"HIGH ≥ {rs.get('high_threshold')}.",
            ),
            section(
                "detection_methods",
                "Detection methods",
                [("method", "Method", "text"), ("transactions", "Transactions", "int"), ("flagged", "Flagged", "int")],
                [{"method": m, "transactions": int(c), "flagged": int(fl or 0)} for m, c, fl in methods],
            ),
            section(
                "rule_triggers",
                "Rule triggers on flagged transactions",
                [("rule", "Rule", "text"), ("triggers", "Triggers", "int")],
                [{"rule": k, "triggers": v} for k, v in sorted(rules.items(), key=lambda kv: (-kv[1], kv[0]))],
                description=f"Counted over the {examined:,} most recent flagged predictions in the period (cap "
                f"{RULE_SCAN_CAP:,}); a prediction can trigger several rules.",
            ),
            section(
                "confusion",
                "Risk engine vs simulator labels",
                [("outcome", "Outcome", "text"), ("transactions", "Transactions", "int")],
                [
                    {"outcome": "True positive (flagged, labelled suspicious)", "transactions": tp},
                    {"outcome": "False positive (flagged, labelled benign)", "transactions": fp},
                    {"outcome": "False negative (not flagged, labelled suspicious)", "transactions": fn},
                    {"outcome": "True negative (not flagged, labelled benign)", "transactions": tn},
                ]
                if n
                else [],
            ),
            section(
                "hourly",
                "Activity by hour of event time (UTC)",
                [
                    ("hour", "Hour (UTC)", "int"),
                    ("transactions", "Transactions", "int"),
                    ("flagged", "Flagged", "int"),
                    ("flag_rate", "Flag rate", "pct"),
                ],
                [
                    {
                        "hour": i,
                        "transactions": hours.get(i, (0, 0))[0],
                        "flagged": hours.get(i, (0, 0))[1],
                        "flag_rate": _rate(hours.get(i, (0, 0))[1], hours.get(i, (0, 0))[0]),
                    }
                    for i in range(24)
                ]
                if n
                else [],
                description="Hours with no transactions are real zeros.",
            ),
        ],
        "limitations": [
            SYNTHETIC,
            FLAGGED_DEF + ".",
            "Risk scores are a bounded heuristic combination of model, anomaly and rule signals — not probabilities.",
            "Simulated traffic is stamped with generation time, so hourly activity shows when the simulator ran.",
        ],
    }


def _alert_summary(db: Session, start: datetime, end: datetime, f: dict[str, Any]) -> dict[str, Any]:
    cond = [Alert.created_at >= start, Alert.created_at < end]
    if f.get("risk_level"):
        cond.append(Alert.risk_category == f["risk_level"])
    if f.get("alert_status"):
        cond.append(Alert.status == f["alert_status"])

    def grouped(col):  # type: ignore[no-untyped-def]
        return db.execute(select(col, func.count()).where(*cond).group_by(col).order_by(func.count().desc(), col)).all()

    tot = db.execute(
        select(
            func.count(),
            func.avg(Alert.risk_score),
            func.sum(case((Alert.assigned_to_id.is_not(None), 1), else_=0)),
            func.sum(case((Alert.investigation_id.is_not(None), 1), else_=0)),
        ).where(*cond)
    ).one()
    n, avg_risk, assigned, linked = int(tot[0] or 0), tot[1], int(tot[2] or 0), int(tot[3] or 0)
    by_status = dict(grouped(Alert.status))
    by_cat = grouped(Alert.risk_category)
    by_method = grouped(Alert.detection_method)
    assignee = aliased(User)
    by_user = db.execute(
        select(assignee.username, func.count())
        .select_from(Alert)
        .outerjoin(assignee, Alert.assigned_to_id == assignee.id)
        .where(*cond)
        .group_by(assignee.username)
        .order_by(func.count().desc())
    ).all()
    rules: dict[str, int] = {}
    examined = 0
    for (tr,) in db.execute(select(Alert.triggered_rules).where(*cond).order_by(Alert.id.desc()).limit(RULE_SCAN_CAP)):
        examined += 1
        for r in tr or []:
            rules[r["code"]] = rules.get(r["code"], 0) + 1
    rows = db.execute(
        select(
            Alert.alert_uid,
            Alert.created_at,
            Alert.status,
            Alert.risk_category,
            Alert.risk_score,
            Alert.detection_method,
            Alert.customer_id,
            Transaction.transaction_id,
            Transaction.amount,
            Transaction.currency,
            Transaction.merchant_category,
            Alert.triggered_rules,
            assignee.username,
            Investigation.case_uid,
        )
        .join(Transaction, Alert.transaction_id == Transaction.id)
        .outerjoin(assignee, Alert.assigned_to_id == assignee.id)
        .outerjoin(Investigation, Alert.investigation_id == Investigation.id)
        .where(*cond)
        .order_by(Alert.created_at.desc(), Alert.id.desc())
        .limit(DETAIL_ROW_CAP)
    ).all()
    alert_rows = [
        {
            "alert_uid": u,
            "created_at": _iso(c),
            "status": s,
            "risk_category": rc,
            "risk_score": sc,
            "detection_method": dm,
            "customer_id": cu,
            "transaction_id": tx,
            "amount": float(a),
            "currency": ccy,
            "merchant_category": mc,
            "rules": "|".join(r["code"] for r in (tr or [])),
            "assigned_to": who,
            "case_uid": case_uid,
        }
        for u, c, s, rc, sc, dm, cu, tx, a, ccy, mc, tr, who, case_uid in rows
    ]
    open_ = by_status.get("NEW", 0) + by_status.get("UNDER_REVIEW", 0)
    return {
        "summary": [
            metric("alerts", "Alerts raised", n),
            metric("open", "Open (NEW or UNDER REVIEW)", open_),
            metric("confirmed", "Confirmed suspicious", by_status.get("CONFIRMED_SUSPICIOUS", 0)),
            metric("dismissed", "Dismissed", by_status.get("DISMISSED", 0)),
            metric("closed", "Closed", by_status.get("CLOSED", 0)),
            metric("high", "HIGH risk alerts", next((int(c) for k, c in by_cat if k == "HIGH"), 0)),
            metric("assigned", "Assigned to an analyst", assigned),
            metric("linked", "Linked to an investigation", linked),
            metric("avg_risk", "Average risk score", avg_risk, "score"),
        ],
        "sections": [
            section(
                "by_status",
                "By status",
                [("status", "Status", "text"), ("alerts", "Alerts", "int"), ("share", "Share", "pct")],
                [
                    {"status": s, "alerts": int(by_status[s]), "share": _rate(by_status[s], n)}
                    for s in ALERT_STATUSES
                    if s in by_status
                ],
            ),
            section(
                "by_risk",
                "By risk category",
                [("category", "Category", "text"), ("alerts", "Alerts", "int")],
                [{"category": c, "alerts": int(k)} for c, k in by_cat],
            ),
            section(
                "by_method",
                "By detection method",
                [("method", "Method", "text"), ("alerts", "Alerts", "int")],
                [{"method": m, "alerts": int(k)} for m, k in by_method],
            ),
            section(
                "by_rule",
                "By triggered rule",
                [("rule", "Rule", "text"), ("alerts", "Alerts", "int")],
                [{"rule": k, "alerts": v} for k, v in sorted(rules.items(), key=lambda kv: (-kv[1], kv[0]))],
                description=f"Over the {examined:,} most recent alerts in scope (cap {RULE_SCAN_CAP:,}); an alert can "
                "trigger several rules; alerts raised by the model alone have no rule.",
            ),
            section(
                "by_assignee",
                "By assignee",
                [("assignee", "Assignee", "text"), ("alerts", "Alerts", "int")],
                [{"assignee": u or "(unassigned)", "alerts": int(k)} for u, k in by_user],
            ),
            section(
                "alerts",
                "Alert list",
                [
                    ("alert_uid", "Alert id", "text"),
                    ("created_at", "Created (UTC)", "datetime"),
                    ("status", "Status", "text"),
                    ("risk_category", "Risk", "text"),
                    ("risk_score", "Score", "score"),
                    ("detection_method", "Method", "text"),
                    ("customer_id", "Customer", "text"),
                    ("transaction_id", "Transaction", "text"),
                    ("amount", "Amount", "money"),
                    ("currency", "Ccy", "text"),
                    ("merchant_category", "Merchant category", "text"),
                    ("rules", "Rules", "text"),
                    ("assigned_to", "Assigned to", "text"),
                    ("case_uid", "Case", "text"),
                ],
                alert_rows,
                total_rows=n,
                description="Newest first.",
            ),
        ],
        "limitations": [
            SYNTHETIC,
            "Status counts are the alerts' statuses at generation time, for alerts created in the period.",
        ],
    }


def _investigation(db: Session, start: datetime, end: datetime, f: dict[str, Any]) -> dict[str, Any]:
    cond = [Investigation.created_at >= start, Investigation.created_at < end]
    if f.get("investigation_status"):
        cond.append(Investigation.status == f["investigation_status"])
    by_status = dict(
        db.execute(select(Investigation.status, func.count()).where(*cond).group_by(Investigation.status)).all()
    )
    by_prio = dict(
        db.execute(select(Investigation.priority, func.count()).where(*cond).group_by(Investigation.priority)).all()
    )
    by_cls = db.execute(
        select(Investigation.final_classification, func.count())
        .where(*cond)
        .group_by(Investigation.final_classification)
        .order_by(func.count().desc())
    ).all()
    n = sum(int(v) for v in by_status.values())
    opener, assignee = aliased(User), aliased(User)
    alert_counts = select(Alert.investigation_id, func.count().label("k")).group_by(Alert.investigation_id).subquery()
    note_counts = (
        select(InvestigationNote.investigation_id, func.count().label("k"))
        .where(InvestigationNote.investigation_id.is_not(None))
        .group_by(InvestigationNote.investigation_id)
        .subquery()
    )
    rows = db.execute(
        select(
            Investigation.case_uid,
            Investigation.title,
            Investigation.customer_id,
            Investigation.status,
            Investigation.priority,
            Investigation.final_classification,
            func.coalesce(alert_counts.c.k, 0),
            func.coalesce(note_counts.c.k, 0),
            opener.username,
            assignee.username,
            Investigation.created_at,
            Investigation.closed_at,
            Investigation.findings,
        )
        .join(opener, Investigation.opened_by_id == opener.id)
        .outerjoin(assignee, Investigation.assigned_to_id == assignee.id)
        .outerjoin(alert_counts, alert_counts.c.investigation_id == Investigation.id)
        .outerjoin(note_counts, note_counts.c.investigation_id == Investigation.id)
        .where(*cond)
        .order_by(Investigation.created_at.desc(), Investigation.id.desc())
        .limit(DETAIL_ROW_CAP)
    ).all()
    case_rows, close_hours, linked = [], [], 0
    for uid, title, cust, st, pr, cls, k, notes, by, to, created, closed, findings in rows:
        linked += int(k)
        hours = (closed - created).total_seconds() / 3600 if closed and created else None
        if hours is not None:
            close_hours.append(hours)
        case_rows.append(
            {
                "case_uid": uid,
                "title": title,
                "customer_id": cust,
                "status": st,
                "priority": pr,
                "classification": cls,
                "alerts": int(k),
                "notes": int(notes),
                "opened_by": by,
                "assigned_to": to,
                "created_at": _iso(created),
                "closed_at": _iso(closed),
                "hours_to_close": hours,
                "findings": findings,
            }
        )
    close_hours.sort()
    cls = {c: int(k) for c, k in by_cls}
    unresolved = by_status.get("OPEN", 0) + by_status.get("IN_PROGRESS", 0)
    return {
        "summary": [
            metric("cases", "Cases opened", n),
            metric("unresolved", "Unresolved (OPEN or IN PROGRESS)", unresolved),
            metric("resolved", "Resolved", by_status.get("RESOLVED", 0)),
            metric("closed", "Closed", by_status.get("CLOSED", 0)),
            metric("confirmed", "Classified confirmed suspicious", cls.get("CONFIRMED_SUSPICIOUS", 0)),
            metric("not_suspicious", "Classified not suspicious", cls.get("NOT_SUSPICIOUS", 0)),
            metric("inconclusive", "Classified inconclusive", cls.get("INCONCLUSIVE", 0)),
            metric("linked_alerts", "Linked alerts", linked, hint="over the cases listed"),
            metric(
                "median_hours_to_close",
                "Median time to close",
                _pctl(close_hours, 0.5),
                "hours",
                f"{len(close_hours)} closed case(s)" if close_hours else "no closed cases",
            ),
        ],
        "sections": [
            section(
                "by_status",
                "By status",
                [("status", "Status", "text"), ("cases", "Cases", "int")],
                [{"status": s, "cases": int(by_status[s])} for s in INVESTIGATION_STATUSES if s in by_status],
            ),
            section(
                "by_priority",
                "By priority",
                [("priority", "Priority", "text"), ("cases", "Cases", "int")],
                [{"priority": p, "cases": int(by_prio[p])} for p in ("HIGH", "MEDIUM", "LOW") if p in by_prio],
            ),
            section(
                "by_classification",
                "By final classification",
                [("classification", "Classification", "text"), ("cases", "Cases", "int")],
                [{"classification": c or "(not classified yet)", "cases": int(k)} for c, k in by_cls],
            ),
            section(
                "cases",
                "Case list",
                [
                    ("case_uid", "Case id", "text"),
                    ("title", "Title", "text"),
                    ("customer_id", "Customer", "text"),
                    ("status", "Status", "text"),
                    ("priority", "Priority", "text"),
                    ("classification", "Classification", "text"),
                    ("alerts", "Alerts", "int"),
                    ("notes", "Notes", "int"),
                    ("opened_by", "Opened by", "text"),
                    ("assigned_to", "Assigned to", "text"),
                    ("created_at", "Opened (UTC)", "datetime"),
                    ("closed_at", "Closed (UTC)", "datetime"),
                    ("hours_to_close", "Hours to close", "float2"),
                    ("findings", "Findings", "longtext"),
                ],
                case_rows,
                total_rows=n,
                description="Newest first.",
            ),
        ],
        "limitations": [
            SYNTHETIC,
            "Statuses and classifications are as recorded at generation time, for cases opened in the period.",
            "Time to close is closed_at − created_at for cases that have been closed.",
        ],
    }


def _model_performance(db: Session, start: datetime, end: datetime, f: dict[str, Any]) -> dict[str, Any]:
    models = list(
        db.scalars(
            select(ModelVersion).order_by(ModelVersion.kind.desc(), ModelVersion.trained_at.desc(), ModelVersion.model_name)
        )
    )
    reg_rows = []
    for m in models:
        t = (m.metrics or {}).get("test") or {}
        reg_rows.append(
            {
                "model": m.model_name,
                "version": m.version,
                "kind": m.kind,
                "status": m.status,
                "threshold": m.threshold,
                "pr_auc": t.get("pr_auc"),
                "precision": t.get("precision"),
                "recall": t.get("recall"),
                "f1": t.get("f1"),
                "fpr": t.get("false_positive_rate"),
                "offline_p50_ms": (m.inference_latency or {}).get("p50_ms"),
            }
        )
    active = next((m for m in models if m.kind == "supervised" and m.status == "ACTIVE"), None)
    lat: dict[str, list[float]] = {}
    sampled = 0
    for mv, ms in db.execute(
        _tx_window(select(Prediction.model_version, Prediction.inference_ms).join(Transaction), start, end)
        .where(Prediction.inference_ms.is_not(None))
        .order_by(Prediction.id.desc())
        .limit(LATENCY_SCAN_CAP)
    ):
        sampled += 1
        lat.setdefault(mv or "(rules only)", []).append(float(ms))
    lat_rows = []
    for mv, vals in sorted(lat.items()):
        vals.sort()
        lat_rows.append(
            {
                "model_version": mv,
                "samples": len(vals),
                "mean_ms": sum(vals) / len(vals),
                "p50_ms": _pctl(vals, 0.5),
                "p95_ms": _pctl(vals, 0.95),
                "max_ms": vals[-1],
            }
        )
    above = Prediction.model_score >= Prediction.model_threshold
    lab = db.execute(
        _tx_window(
            select(
                Prediction.model_version,
                func.count(),
                func.sum(case((above & Transaction.sim_is_suspicious.is_(True), 1), else_=0)),
                func.sum(case((above & Transaction.sim_is_suspicious.is_(False), 1), else_=0)),
                func.sum(case((~above & Transaction.sim_is_suspicious.is_(True), 1), else_=0)),
                func.sum(case((~above & Transaction.sim_is_suspicious.is_(False), 1), else_=0)),
            ).join(Transaction),
            start,
            end,
        )
        .where(
            Prediction.model_score.is_not(None),
            Prediction.model_threshold.is_not(None),
            Transaction.sim_is_suspicious.is_not(None),
        )
        .group_by(Prediction.model_version)
        .order_by(Prediction.model_version)
    ).all()
    live_rows = [
        {
            "model_version": mv,
            "labelled": int(k),
            "tp": int(tp or 0),
            "fp": int(fp or 0),
            "fn": int(fn or 0),
            "tn": int(tn or 0),
            "precision": _rate(tp, (tp or 0) + (fp or 0)),
            "recall": _rate(tp, (tp or 0) + (fn or 0)),
        }
        for mv, k, tp, fp, fn, tn in lab
    ]
    drift = list(
        db.scalars(
            select(DriftReport)
            .where(DriftReport.computed_at >= start, DriftReport.computed_at < end)
            .order_by(DriftReport.computed_at.desc())
            .limit(DETAIL_ROW_CAP)
        )
    )
    active_label = f"{active.model_name}:{active.version}" if active else None
    act_live = next((r for r in live_rows if r["model_version"] == active_label), None)
    act_lat = next((r for r in lat_rows if r["model_version"] == active_label), None)
    at = ((active.metrics or {}).get("test") or {}) if active else {}
    return {
        "summary": [
            metric("active_model", "Active supervised model", active_label, "text", None if active else "no active model"),
            metric("test_pr_auc", "Test PR-AUC (offline)", at.get("pr_auc"), "ratio"),
            metric("test_precision", "Test precision (offline)", at.get("precision"), "ratio"),
            metric("test_recall", "Test recall (offline)", at.get("recall"), "ratio"),
            metric("live_predictions", "Live predictions (active model)", act_lat["samples"] if act_lat else 0),
            metric("live_p95_ms", "Live inference p95 (active model)", act_lat["p95_ms"] if act_lat else None, "ms"),
            metric(
                "live_precision", "Live precision vs simulator labels", act_live["precision"] if act_live else None, "ratio"
            ),
            metric("live_recall", "Live recall vs simulator labels", act_live["recall"] if act_live else None, "ratio"),
            metric("drift_checks", "Drift checks in period", len(drift)),
            metric(
                "latest_drift",
                "Latest drift status",
                drift[0].status if drift else None,
                "text",
                None if drift else "no drift check in the period",
            ),
        ],
        "sections": [
            section(
                "registry",
                "Registered models — offline test metrics",
                [
                    ("model", "Model", "text"),
                    ("version", "Version", "text"),
                    ("kind", "Kind", "text"),
                    ("status", "Status", "text"),
                    ("threshold", "Threshold", "ratio"),
                    ("pr_auc", "PR-AUC", "ratio"),
                    ("precision", "Precision", "ratio"),
                    ("recall", "Recall", "ratio"),
                    ("f1", "F1", "ratio"),
                    ("fpr", "FPR", "ratio"),
                    ("offline_p50_ms", "Offline p50 ms", "ms"),
                ],
                reg_rows,
                description="Stored by the training pipeline (temporal test split of the synthetic dataset). "
                "Anomaly models have no supervised metrics.",
            ),
            section(
                "live_latency",
                "Live inference latency in the period",
                [
                    ("model_version", "Model version", "text"),
                    ("samples", "Samples", "int"),
                    ("mean_ms", "Mean ms", "ms"),
                    ("p50_ms", "p50 ms", "ms"),
                    ("p95_ms", "p95 ms", "ms"),
                    ("max_ms", "Max ms", "ms"),
                ],
                lat_rows,
                description=f"Measured inference_ms of persisted predictions ({sampled:,} most recent in the period, cap "
                f"{LATENCY_SCAN_CAP:,}).",
            ),
            section(
                "live_labelled",
                "Live model decisions vs simulator labels",
                [
                    ("model_version", "Model version", "text"),
                    ("labelled", "Labelled", "int"),
                    ("tp", "TP", "int"),
                    ("fp", "FP", "int"),
                    ("fn", "FN", "int"),
                    ("tn", "TN", "int"),
                    ("precision", "Precision", "ratio"),
                    ("recall", "Recall", "ratio"),
                ],
                live_rows,
                description="Model score ≥ the model threshold stored with each prediction, against simulator labels "
                "(the model alone, before the risk engine combines it with rules).",
            ),
            section(
                "drift",
                "Drift checks in the period",
                [
                    ("computed_at", "Computed (UTC)", "datetime"),
                    ("model_version", "Model version", "text"),
                    ("rows", "Rows", "int"),
                    ("status", "Status", "text"),
                    ("max_psi", "Max PSI", "ratio"),
                ],
                [
                    {
                        "computed_at": _iso(d.computed_at),
                        "model_version": d.model_version,
                        "rows": d.window_rows,
                        "status": d.status,
                        "max_psi": d.max_psi,
                    }
                    for d in drift
                ],
            ),
        ],
        "limitations": [
            SYNTHETIC,
            "Offline metrics come from synthetic held-out data and are optimistic; they are not real-world "
            "fraud-detection performance.",
            "Model scores are uncalibrated and must not be read as probabilities.",
            "Drift checks exist only when someone ran one (Model Intelligence → Drift); they are not scheduled.",
        ],
    }


def _system_activity(db: Session, start: datetime, end: datetime, f: dict[str, Any]) -> dict[str, Any]:
    cond = [AuditLog.created_at >= start, AuditLog.created_at < end]

    def grouped(col):  # type: ignore[no-untyped-def]
        return db.execute(select(col, func.count()).where(*cond).group_by(col).order_by(func.count().desc(), col)).all()

    by_action = grouped(AuditLog.action)
    by_actor = grouped(AuditLog.actor_name)
    by_entity = grouped(AuditLog.entity_type)
    counts = {a: int(k) for a, k in by_action}
    n = sum(counts.values())
    config = sum(v for a, v in counts.items() if a in ("RULE_UPDATED", "RISK_SETTINGS_UPDATED", "SETTING_UPDATED"))
    rejected = sum(v for a, v in counts.items() if a.endswith("_REJECTED") or a == "CONFIG_CHANGE_DENIED")
    rows = db.scalars(select(AuditLog).where(*cond).order_by(AuditLog.id.desc()).limit(DETAIL_ROW_CAP))
    ev = [
        {
            "created_at": _iso(a.created_at),
            "actor": a.actor_name,
            "action": a.action,
            "entity_type": a.entity_type,
            "entity_id": a.entity_id,
            "field": a.field,
            "old_value": a.old_value,
            "new_value": a.new_value,
            "details": json.dumps(a.details, ensure_ascii=False, sort_keys=True, default=str) if a.details else None,
        }
        for a in rows
    ]
    return {
        "summary": [
            metric("events", "Audit events", n),
            metric("actors", "Distinct actors", len(by_actor)),
            metric("logins", "Successful logins", counts.get("LOGIN", 0)),
            metric("failed_logins", "Failed logins", counts.get("LOGIN_FAILED", 0)),
            metric("config_changes", "Configuration changes", config),
            metric("config_rejected", "Rejected or denied configuration changes", rejected),
            metric("reports", "Reports generated", counts.get("REPORT_GENERATED", 0)),
        ],
        "sections": [
            section(
                "by_action",
                "By action",
                [("action", "Action", "text"), ("events", "Events", "int")],
                [{"action": a, "events": int(k)} for a, k in by_action],
            ),
            section(
                "by_actor",
                "By actor",
                [("actor", "Actor", "text"), ("events", "Events", "int")],
                [{"actor": a, "events": int(k)} for a, k in by_actor],
            ),
            section(
                "by_entity",
                "By entity type",
                [("entity_type", "Entity type", "text"), ("events", "Events", "int")],
                [{"entity_type": a, "events": int(k)} for a, k in by_entity],
            ),
            section(
                "events",
                "Event list",
                [
                    ("created_at", "Time (UTC)", "datetime"),
                    ("actor", "Actor", "text"),
                    ("action", "Action", "text"),
                    ("entity_type", "Entity", "text"),
                    ("entity_id", "Entity id", "text"),
                    ("field", "Field", "text"),
                    ("old_value", "Old value", "longtext"),
                    ("new_value", "New value", "longtext"),
                    ("details", "Details", "longtext"),
                ],
                ev,
                total_rows=n,
                description="Newest first.",
            ),
        ],
        "limitations": [
            "The audit trail records actions taken through the API (logins, case actions, configuration, models, "
            "simulations, reports). It does not record read-only page views.",
            "Actor 'system' marks automatic actions (e.g. alerts created by the pipeline).",
        ],
    }


BUILDERS: dict[str, Callable[[Session, datetime, datetime, dict[str, Any]], dict[str, Any]]] = {
    "fraud_activity": _fraud_activity,
    "risk_analytics": _risk_analytics,
    "alert_summary": _alert_summary,
    "investigation": _investigation,
    "model_performance": _model_performance,
    "system_activity": _system_activity,
}


# ------------------------------------------------------------------ generation & persistence
def period_label(start: datetime, end: datetime) -> str:
    return f"{start.astimezone(timezone.utc):%Y-%m-%d %H:%M} UTC → {end.astimezone(timezone.utc):%Y-%m-%d %H:%M} UTC (end exclusive)"


def generate(
    db: Session, user: User, report_type: str, start: datetime, end: datetime, filters: dict[str, Any], profile: str
) -> GeneratedReport:
    """Build, persist and audit a report. A failure is persisted as a FAILED report (no internal details)."""
    spec = REPORT_TYPES[report_type]
    uid = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    applied = {k: v for k, v in filters.items() if v}
    content: dict[str, Any] | None = None
    error: str | None = None
    try:
        body = BUILDERS[report_type](db, start, end, applied)
        content = {
            "report_uid": uid,
            "report_type": report_type,
            "title": spec.title,
            "generated_at": now.isoformat(),
            "generated_by": user.username,
            "profile": profile,
            "period": {"start": _iso(start), "end": _iso(end), "basis": spec.basis, "label": period_label(start, end)},
            "filters": applied,
            "data_sources": [spec.basis],
            **body,
        }
    except Exception as exc:  # noqa: BLE001 - recorded as a FAILED report; details go to the server log only
        db.rollback()
        log.exception("report generation failed: type=%s", report_type)
        error = f"report generation failed ({exc.__class__.__name__})"
    rec = GeneratedReport(
        report_uid=uid,
        report_type=report_type,
        title=spec.title,
        period_start=start,
        period_end=end,
        filters=applied,
        status="COMPLETED" if content else "FAILED",
        error=error,
        row_count=sum(len(s["rows"]) for s in content["sections"]) if content else 0,
        generated_by_id=user.id,
        generated_by_name=user.username,
        created_at=now,
        content=content,
    )
    db.add(rec)
    audit.record(
        db,
        user,
        "REPORT_GENERATED" if content else "REPORT_FAILED",
        "report",
        uid,
        details={"report_type": report_type, "period_start": _iso(start), "period_end": _iso(end), "filters": applied},
    )
    db.commit()
    return rec


def report_meta(r: GeneratedReport) -> dict[str, Any]:
    return {
        "report_uid": r.report_uid,
        "report_type": r.report_type,
        "title": r.title,
        "period_start": _iso(r.period_start),
        "period_end": _iso(r.period_end),
        "period_label": period_label(r.period_start, r.period_end),
        "filters": r.filters,
        "status": r.status,
        "error": r.error,
        "row_count": r.row_count,
        "generated_by": r.generated_by_name,
        "created_at": _iso(r.created_at),
    }


def preview(r: GeneratedReport, rows: int | None = None) -> dict[str, Any]:
    """Report metadata + content with each section's rows cut to ``rows`` (counts are kept)."""
    rows = PREVIEW_ROWS if rows is None else rows
    out = report_meta(r)
    if r.content:
        c = dict(r.content)
        c["sections"] = [{**s, "rows": s["rows"][:rows], "stored_rows": len(s["rows"])} for s in c["sections"]]
        out["content"] = c
    else:
        out["content"] = None
    return out


# ------------------------------------------------------------------ rendering
def safe_filename(r: GeneratedReport, ext: str, section_id: str | None = None) -> str:
    part = f"-{''.join(ch for ch in section_id if ch.isalnum() or ch == '_')}" if section_id else ""
    return (
        f"fraudstream-{r.report_type.replace('_', '-')}-{r.period_start:%Y%m%d}-{r.period_end:%Y%m%d}"
        f"{part}-{r.report_uid[:8]}.{ext}"
    )


_FORMULA_START = ("=", "+", "-", "@", "\t", "\r")


def csv_value(v: Any) -> Any:
    """CSV cell. Strings that a spreadsheet would execute as a formula are prefixed with an apostrophe
    (CSV/formula injection guard); numbers are written as numbers; quoting/escaping is done by csv."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return v
    s = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False, default=str)
    return "'" + s if s.startswith(_FORMULA_START) else s


def _filters_text(filters: dict[str, Any]) -> str:
    return "; ".join(f"{k}={v}" for k, v in filters.items()) or "none"


def render_csv(content: dict[str, Any], section_id: str | None = None) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    sections = content["sections"]
    if section_id is not None:
        s = next(x for x in sections if x["id"] == section_id)
        w.writerow([c["label"] for c in s["columns"]])
        for row in s["rows"]:
            w.writerow([csv_value(row.get(c["key"])) for c in s["columns"]])
        return buf.getvalue().encode("utf-8-sig")
    p = content["period"]
    for k, v in (
        ("FraudStream report", content["title"]),
        ("Report ID", content["report_uid"]),
        ("Report type", content["report_type"]),
        ("Generated at (UTC)", content["generated_at"]),
        ("Generated by", content["generated_by"]),
        ("Profile", content.get("profile")),
        ("Period start (UTC)", p["start"]),
        ("Period end (UTC, exclusive)", p["end"]),
        ("Period basis", p["basis"]),
        ("Filters", _filters_text(content["filters"])),
    ):
        w.writerow([k, csv_value(v)])
    w.writerow([])
    w.writerow(["Summary"])
    w.writerow(["Metric", "Value", "Note"])
    for m in content["summary"]:
        w.writerow([m["label"], csv_value(m["value"]), csv_value(m.get("hint"))])
    for s in sections:
        w.writerow([])
        w.writerow(["Section", s["title"]])
        if s.get("description"):
            w.writerow(["Note", csv_value(s["description"])])
        w.writerow([c["label"] for c in s["columns"]])
        for row in s["rows"]:
            w.writerow([csv_value(row.get(c["key"])) for c in s["columns"]])
        if not s["rows"]:
            w.writerow(["(no rows for this period and filters)"])
        if s["truncated"]:
            w.writerow([f"(stored {len(s['rows'])} of {s['total_rows']} rows — detail cap {DETAIL_ROW_CAP})"])
    w.writerow([])
    w.writerow(["Limitations"])
    for line in content.get("limitations", []):
        w.writerow([csv_value(line)])
    return buf.getvalue().encode("utf-8-sig")


def fmt_value(v: Any, f: str) -> str:
    """Human-readable value (PDF). Missing values are 'n/a', never 0."""
    if v is None or v == "":
        return "n/a" if f not in ("text", "longtext") else "—"
    try:
        if f == "int":
            return f"{int(v):,}"
        if f == "pct":
            return f"{float(v) * 100:.1f}%"
        if f == "ratio":
            return f"{float(v):.4f}"
        if f == "score":
            return f"{float(v):.1f}"
        if f == "money":
            return f"{float(v):,.2f}"
        if f == "float2":
            return f"{float(v):,.2f}"
        if f == "ms":
            return f"{float(v):.1f} ms"
        if f == "hours":
            return f"{float(v):.1f} h"
        if f == "datetime":
            return datetime.fromisoformat(str(v)).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        pass
    return str(v)


_FONT_CANDIDATES = (
    ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
)


def _fonts() -> tuple[str, str]:
    """Register an embeddable TrueType font (Unicode). DejaVu if installed (app image), else the Bitstream
    Vera fonts bundled with reportlab, so the PDF never falls back to the Latin-1-only core fonts."""
    from reportlab import __file__ as rl_file
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    if "FSSans" in pdfmetrics.getRegisteredFontNames():
        return "FSSans", "FSSans-Bold"
    rl_fonts = os.path.join(os.path.dirname(rl_file), "fonts")
    for regular, bold in (*_FONT_CANDIDATES, (os.path.join(rl_fonts, "Vera.ttf"), os.path.join(rl_fonts, "VeraBd.ttf"))):
        if os.path.exists(regular) and os.path.exists(bold):
            pdfmetrics.registerFont(TTFont("FSSans", regular))
            pdfmetrics.registerFont(TTFont("FSSans-Bold", bold))
            return "FSSans", "FSSans-Bold"
    raise RuntimeError("no TrueType font available for PDF export")


# Phase 12: CJK (and other scripts DejaVu lacks) via a CHAIN of fallback fonts, applied per character: a character
# missing from the base font goes to the first fallback that has it. All are TrueType-outline fonts, which reportlab
# subsets and embeds (Noto Sans CJK is OpenType/CFF, which reportlab's TTFont cannot load):
#   Droid Sans Fallback (Apache-2.0, Debian fonts-droid-fallback) - Chinese ideographs (20,902 of U+4E00-9FFF), kana;
#   Nanum Gothic (SIL OFL 1.1, Debian fonts-nanum) - Korean Hangul (Droid has 3 of 11,172 syllables; measured).
# FRAUDSTREAM_PDF_FALLBACK_FONT (a TrueType .ttf/.ttc) is tried first. Without any fallback font the PDF behaves as
# before (missing glyphs); the CSV always keeps the text.
_FALLBACK_CANDIDATES = ("/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
                        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf")
_cmaps: dict[str, frozenset[int]] = {}
_fallbacks: list[str] | None = None


def _fallback_fonts() -> list[str]:
    """Registered fallback font names, in priority order (registered once per process)."""
    global _fallbacks
    if _fallbacks is not None:
        return _fallbacks
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFError, TTFont

    names: list[str] = []
    for path in filter(None, (os.environ.get("FRAUDSTREAM_PDF_FALLBACK_FONT"), *_FALLBACK_CANDIDATES)):
        if not os.path.exists(path):
            continue
        name = f"FSFallback{len(names)}"
        try:
            f = TTFont(name, path)
        except TTFError:
            log.warning("PDF fallback font %s is not usable (TrueType outlines required)", os.path.basename(path))
            continue
        pdfmetrics.registerFont(f)
        _cmaps[name] = frozenset(f.face.charToGlyph)
        names.append(name)
    _fallbacks = names
    return names


def _fallback_font() -> str | None:
    """First registered fallback font (None if none is available)."""
    fonts = _fallback_fonts()
    return fonts[0] if fonts else None


def _covered(font: str) -> frozenset[int]:
    if font not in _cmaps:
        from reportlab.pdfbase import pdfmetrics

        _cmaps[font] = frozenset(pdfmetrics.getFont(font).face.charToGlyph)
    return _cmaps[font]


def font_runs(s: str, base: frozenset[int], fallbacks: list[tuple[str, frozenset[int]]] | None
              ) -> list[tuple[str | None, str]]:
    """Split text into (font_or_None, run) pieces: a character goes to the first fallback font that has it, and only
    when the base font lacks it (whitespace and punctuation stay in the base font; None = base font)."""
    runs: list[tuple[str | None, str]] = []
    for ch in s:
        font: str | None = None
        if ord(ch) not in base:
            font = next((name for name, cmap in fallbacks or () if ord(ch) in cmap), None)
        if runs and runs[-1][0] == font:
            runs[-1] = (font, runs[-1][1] + ch)
        else:
            runs.append((font, ch))
    return runs


def _para_text(s: str, limit: int | None = None) -> str:
    if limit is not None and len(s) > limit:
        s = s[:limit] + " … [truncated in PDF; full text in CSV]"
    fbs = _fallback_fonts() if any(ord(c) > 0x2FF for c in s) else []
    if fbs:
        out = "".join(f'<font name="{font}">{escape(run)}</font>' if font else escape(run)
                      for font, run in font_runs(s, _covered("FSSans"), [(n, _cmaps[n]) for n in fbs]))
    else:
        out = escape(s)
    return out.replace("\r\n", "\n").replace("\n", "<br/>")


def render_pdf(content: dict[str, Any]) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import KeepTogether, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    regular, bold = _fonts()
    page = landscape(A4)
    margin = 14 * mm
    width = page[0] - 2 * margin
    st = {
        "title": ParagraphStyle("t", fontName=bold, fontSize=17, leading=21, spaceAfter=2),
        "sub": ParagraphStyle("s", fontName=regular, fontSize=9, leading=12, textColor=colors.HexColor("#4b5563")),
        "h2": ParagraphStyle("h2", fontName=bold, fontSize=11.5, leading=14, spaceBefore=10, spaceAfter=3),
        "body": ParagraphStyle("b", fontName=regular, fontSize=8.5, leading=11),
        "note": ParagraphStyle("n", fontName=regular, fontSize=7.5, leading=9.5, textColor=colors.HexColor("#4b5563")),
        "cell": ParagraphStyle("c", fontName=regular, fontSize=7, leading=8.6),
        "head": ParagraphStyle("h", fontName=bold, fontSize=7, leading=8.6, textColor=colors.white),
    }
    grid = TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#c7cdd6")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f3f5f8")]),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]
    )
    p = content["period"]
    story: list[Any] = [
        Paragraph(_para_text(f"FraudStream — {content['title']}"), st["title"]),
        Paragraph(
            _para_text("Educational platform · synthetic data only · generated from persisted application data"), st["sub"]
        ),
        Spacer(1, 6),
    ]
    meta = [
        ("Report ID", content["report_uid"]),
        ("Generated", fmt_value(content["generated_at"], "datetime") + " UTC"),
        ("Generated by", content["generated_by"]),
        ("Reporting period", p["label"]),
        ("Period basis", p["basis"]),
        ("Filters", _filters_text(content["filters"])),
        ("Profile", content.get("profile") or "—"),
    ]
    mt = Table(
        [[Paragraph(_para_text(k), st["head"]), Paragraph(_para_text(str(v)), st["cell"])] for k, v in meta],
        colWidths=[38 * mm, width - 38 * mm],
    )
    mt.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#1f2937")),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#c7cdd6")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    story += [mt, Paragraph("Summary", st["h2"])]
    summ = [[Paragraph("Metric", st["head"]), Paragraph("Value", st["head"]), Paragraph("Note", st["head"])]]
    summ += [
        [
            Paragraph(_para_text(m["label"]), st["cell"]),
            Paragraph(_para_text(fmt_value(m["value"], m["format"])), st["cell"]),
            Paragraph(_para_text(m.get("hint") or ""), st["cell"]),
        ]
        for m in content["summary"]
    ]
    t = Table(summ, colWidths=[width * 0.4, width * 0.25, width * 0.35], repeatRows=1)
    t.setStyle(grid)
    story.append(t)
    for s in content["sections"]:
        head: list[Any] = [Paragraph(_para_text(s["title"]), st["h2"])]
        if s.get("description"):
            head.append(Paragraph(_para_text(s["description"]), st["note"]))
        rows = s["rows"][:PDF_ROW_CAP]
        if not rows:
            story.append(KeepTogether([*head, Paragraph("No rows for this period and filters.", st["body"])]))
            continue
        cols = s["columns"]
        weights = [3.0 if c["format"] == "longtext" else 1.6 if c["format"] in ("text", "datetime") else 1.0 for c in cols]
        tw = sum(weights)
        data = [[Paragraph(_para_text(c["label"]), st["head"]) for c in cols]]
        for r in rows:
            data.append(
                [
                    Paragraph(
                        _para_text(
                            fmt_value(r.get(c["key"]), c["format"]),
                            PDF_CELL_CHARS if c["format"] in ("longtext", "text") else None,
                        ),
                        st["cell"],
                    )
                    for c in cols
                ]
            )
        tbl = Table(data, colWidths=[width * w / tw for w in weights], repeatRows=1)
        tbl.setStyle(grid)
        story += [*head, tbl]
        notes = []
        if len(s["rows"]) > PDF_ROW_CAP:
            notes.append(
                f"The PDF lists the first {PDF_ROW_CAP:,} of {len(s['rows']):,} stored rows; the CSV export contains all of them."
            )
        if s["truncated"]:
            notes.append(
                f"{s['total_rows']:,} rows matched; the report stores the first {len(s['rows']):,} (detail cap {DETAIL_ROW_CAP:,})."
            )
        for n in notes:
            story.append(Paragraph(_para_text(n), st["note"]))
    story.append(Paragraph("Limitations and data sources", st["h2"]))
    for line in [*content.get("limitations", []), f"Data basis: {p['basis']}."]:
        story.append(Paragraph(_para_text(f"• {line}"), st["body"]))

    def footer(canvas: Any, doc: Any) -> None:
        canvas.saveState()
        canvas.setFont(regular, 7)
        canvas.setFillColor(colors.HexColor("#6b7280"))
        canvas.drawString(margin, 8 * mm, f"FraudStream · {content['title']} · {content['report_uid']} · synthetic data")
        canvas.drawRightString(page[0] - margin, 8 * mm, f"Page {doc.page}")
        canvas.restoreState()

    out = io.BytesIO()
    doc = SimpleDocTemplate(
        out,
        pagesize=page,
        leftMargin=margin,
        rightMargin=margin,
        topMargin=margin,
        bottomMargin=14 * mm,
        title=f"FraudStream — {content['title']}",
        author="FraudStream",
        subject=p["label"],
        creator="FraudStream reports",
    )
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    return out.getvalue()
