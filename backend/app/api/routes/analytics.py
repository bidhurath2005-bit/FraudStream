"""Analytics, infrastructure status, reports and health. All numbers are computed from the
database / live system — nothing here is fabricated. Unavailable metrics are returned as
null with an explicit ``unavailable_reason``."""
from __future__ import annotations

import csv
import io
import json
import platform
import time
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Query
from fastapi.responses import PlainTextResponse, Response
from sqlalchemy import Integer, case, cast, func, select, text

from app.api.params import Page
from app.core.runtime import get_runtime
from app.models.entities import (
    Alert,
    AuditLog,
    DeadLetter,
    DriftReport,
    ModelVersion,
    Prediction,
    SimulationRun,
    Transaction,
)
from app.schemas.api import audit_out
from app.security.auth import DB, AdminUser, CurrentUser
from app.services.reports import csv_value

router = APIRouter()


def _since(minutes: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(minutes=minutes)


def _bucket_expr(db: DB, seconds: int):  # type: ignore[no-untyped-def]
    """Epoch-second bucket of processed_at, portable across SQLite/PostgreSQL."""
    if db.get_bind().dialect.name == "postgresql":
        return func.floor(func.extract("epoch", Transaction.processed_at) / seconds) * seconds
    # NOTE: SQLAlchemy 2 '/' is TRUE division (renders a float cast on SQLite); '//' is floor division.
    return (cast(func.strftime("%s", Transaction.processed_at), Integer) // seconds) * seconds


def _floor(db: DB, expr):  # type: ignore[no-untyped-def]
    return func.floor(expr) if db.get_bind().dialect.name == "postgresql" else cast(expr, Integer)


# ------------------------------------------------------------------ overview
@router.get("/analytics/overview", tags=["analytics"])
def overview(user: CurrentUser, db: DB) -> dict[str, Any]:
    rt = get_runtime()
    total = db.scalar(select(func.count(Transaction.id))) or 0
    last_min = db.scalar(select(func.count(Transaction.id)).where(Transaction.processed_at >= _since(1))) or 0
    cats = dict(db.execute(select(Prediction.risk_category, func.count()).group_by(Prediction.risk_category)).all())
    alerts_total = db.scalar(select(func.count(Alert.id))) or 0
    high_open = db.scalar(select(func.count(Alert.id)).where(Alert.risk_category == "HIGH",
                                                             Alert.status.in_(["NEW", "UNDER_REVIEW"]))) or 0
    lat = db.execute(select(Transaction.pipeline_latency_ms).where(Transaction.pipeline_latency_ms.is_not(None),
                                                                   Transaction.processed_at >= _since(15))
                     .order_by(Transaction.id.desc()).limit(5000)).scalars().all()
    lat_sorted = sorted(lat)

    def pct(p: float) -> float | None:
        return lat_sorted[min(len(lat_sorted) - 1, int(p * len(lat_sorted)))] if lat_sorted else None
    # detection rate against SIMULATOR ground truth (synthetic transactions only)
    labelled = db.execute(select(
        func.sum(case((Transaction.sim_is_suspicious.is_(True), 1), else_=0)),
        func.sum(case(((Transaction.sim_is_suspicious.is_(True)) & (Prediction.risk_category.in_(["MEDIUM", "HIGH"])), 1), else_=0)),
        func.sum(case(((Transaction.sim_is_suspicious.is_(False)) & (Prediction.risk_category.in_(["MEDIUM", "HIGH"])), 1), else_=0)),
        func.sum(case((Transaction.sim_is_suspicious.is_(False), 1), else_=0)),
    ).join(Prediction)).one()
    inj, detected, false_alerts, normal = (int(x or 0) for x in labelled)
    active = db.scalar(select(ModelVersion).where(ModelVersion.kind == "supervised", ModelVersion.status == "ACTIVE"))
    return {
        "profile": rt.settings.profile,
        "total_transactions": total,
        "transactions_last_minute": last_min,
        "tps_last_minute": round(last_min / 60, 3),
        "risk_categories": cats,
        "suspicious_transactions": int(cats.get("MEDIUM", 0)) + int(cats.get("HIGH", 0)),
        "alerts_total": alerts_total,
        "high_risk_open_alerts": high_open,
        "detection_latency_ms": {"avg": sum(lat) / len(lat) if lat else None, "p50": pct(0.5), "p95": pct(0.95),
                                 "p99": pct(0.99), "samples": len(lat),
                                 "window": "last 15 minutes",
                                 "definition": "processed_at - ingested_at (ingest -> persisted decision)"},
        "detection_rate": {
            "value": detected / inj if inj else None, "numerator": detected, "denominator": inj,
            "definition": "simulator-injected suspicious transactions scored MEDIUM/HIGH / all simulator-injected "
                          "suspicious transactions (synthetic ground truth only)",
            "false_alert_rate": false_alerts / normal if normal else None, "normal_labelled": normal},
        "model": ({"label": f"{active.model_name}:{active.version}", "test_pr_auc": active.metrics.get("test", {}).get("pr_auc"),
                   "test_recall": active.metrics.get("test", {}).get("recall"),
                   "test_precision": active.metrics.get("test", {}).get("precision")} if active else None),
        "model_error": rt.model_error,
        "simulation": rt.simulation.status().get("status"),
    }


@router.get("/analytics/timeline", tags=["analytics"])
def timeline(user: CurrentUser, db: DB, minutes: Annotated[int, Query(ge=1, le=1440)] = 30,
             bucket_seconds: Annotated[int, Query(ge=5, le=3600)] = 60) -> dict[str, Any]:
    b = _bucket_expr(db, bucket_seconds).label("b")
    rows = db.execute(select(b, func.count(Transaction.id),
                             func.sum(case((Prediction.risk_category == "MEDIUM", 1), else_=0)),
                             func.sum(case((Prediction.risk_category == "HIGH", 1), else_=0)),
                             func.avg(Transaction.pipeline_latency_ms))
                      .join(Prediction).where(Transaction.processed_at >= _since(minutes)).group_by(b).order_by(b)).all()
    # zero-fill every bucket in the window so idle periods are shown as zero, not interpolated over
    found = {int(r[0]): r for r in rows}
    now = int(datetime.now(timezone.utc).timestamp())
    first = (now - minutes * 60) // bucket_seconds * bucket_seconds
    points = []
    for b in range(first, now + 1, bucket_seconds):
        r = found.get(b)
        points.append({"t": datetime.fromtimestamp(b, timezone.utc).isoformat(), "total": int(r[1]) if r else 0,
                       "medium": int(r[2] or 0) if r else 0, "high": int(r[3] or 0) if r else 0,
                       "avg_latency_ms": r[4] if r else None})
    return {"bucket_seconds": bucket_seconds, "points": points if rows else []}


@router.get("/analytics/risk-distribution", tags=["analytics"])
def risk_distribution(user: CurrentUser, db: DB,
                      minutes: Annotated[int | None, Query(ge=1, le=43_200)] = None) -> dict[str, Any]:
    raw = _floor(db, Prediction.risk_score / 5) * 5
    bin_ = case((raw > 95, 95), else_=raw).label("bin")
    stmt = select(bin_, func.count()).group_by(bin_).order_by(bin_)
    if minutes:
        stmt = stmt.where(Prediction.predicted_at >= _since(minutes))
    rows = dict((int(b), c) for b, c in db.execute(stmt).all())
    methods = dict(db.execute(select(Prediction.detection_method, func.count()).group_by(Prediction.detection_method)).all())
    return {"bins": [{"from": b, "to": b + 5, "count": rows.get(b, 0)} for b in range(0, 100, 5)],
            "detection_methods": methods}


# Optional analysis window shared by the Risk Analytics endpoints: transactions PROCESSED in the last
# ``minutes`` (same column as timeline / velocity / geo). Omitted = all time (the original behaviour).
WindowMinutes = Annotated[int | None, Query(ge=1, le=43200)]
FLAGGED = case((Prediction.risk_category != "LOW", 1), else_=0)


def _windowed(stmt, minutes: int | None):  # type: ignore[no-untyped-def]
    return stmt.where(Transaction.processed_at >= _since(minutes)) if minutes else stmt


def _period(minutes: int | None) -> dict[str, Any]:
    return {"window_minutes": minutes, "basis": "transactions.processed_at",
            "label": f"last {minutes} minutes" if minutes else "all time"}


@router.get("/analytics/patterns", tags=["analytics"])
def patterns(user: CurrentUser, db: DB, minutes: WindowMinutes = None) -> dict[str, Any]:
    """Per simulated scenario: how many were flagged. Ground truth is SIMULATOR metadata."""
    rows = db.execute(_windowed(select(Transaction.sim_scenario, func.count(), func.sum(FLAGGED), func.avg(Prediction.risk_score),
                                       func.sum(case((Transaction.sim_is_suspicious.is_(True), 1), else_=0)))
                                .join(Prediction).where(Transaction.sim_scenario.is_not(None)), minutes)
                      .group_by(Transaction.sim_scenario)).all()
    rules: dict[str, int] = {}
    rule_rows = _windowed(select(Prediction.triggered_rules).join(Transaction).where(Prediction.risk_category != "LOW"), minutes)
    flagged_examined = 0
    for (tr,) in db.execute(rule_rows.order_by(Prediction.id.desc()).limit(20000)):
        flagged_examined += 1
        for r in tr or []:
            rules[r["code"]] = rules.get(r["code"], 0) + 1
    return {"scenarios": [{"scenario": s, "count": c, "flagged": int(f or 0), "flag_rate": (f or 0) / c if c else None,
                           "avg_risk": a, "labelled_suspicious": int(ls or 0)} for s, c, f, a, ls in rows],
            "rule_hits": [{"rule": k, "count": v} for k, v in sorted(rules.items(), key=lambda kv: (-kv[1], kv[0]))],
            "rule_hits_basis": {"flagged_predictions_examined": flagged_examined, "cap": 20000,
                                "definition": "rule triggers on the most recent MEDIUM/HIGH predictions in the period "
                                              "(a prediction can trigger several rules)"},
            "period": _period(minutes),
            "note": "Scenario labels come from the simulator (synthetic ground truth)."}


@router.get("/analytics/confusion", tags=["analytics"])
def confusion(user: CurrentUser, db: DB, minutes: WindowMinutes = None) -> dict[str, Any]:
    """Live confusion matrix of risk-engine decisions (MEDIUM/HIGH = flagged) vs simulator labels,
    and of analyst outcomes on closed alerts."""
    r = db.execute(_windowed(select(
        func.sum(case(((Transaction.sim_is_suspicious.is_(True)) & (Prediction.risk_category != "LOW"), 1), else_=0)),
        func.sum(case(((Transaction.sim_is_suspicious.is_(False)) & (Prediction.risk_category != "LOW"), 1), else_=0)),
        func.sum(case(((Transaction.sim_is_suspicious.is_(True)) & (Prediction.risk_category == "LOW"), 1), else_=0)),
        func.sum(case(((Transaction.sim_is_suspicious.is_(False)) & (Prediction.risk_category == "LOW"), 1), else_=0)),
    ).join(Prediction), minutes)).one()
    tp, fp, fn, tn = (int(x or 0) for x in r)
    analyst = dict(db.execute(_windowed(select(Alert.status, func.count()).join(Transaction, Alert.transaction_id == Transaction.id)
                                        .where(Alert.status.in_(["CONFIRMED_SUSPICIOUS", "DISMISSED"])), minutes)
                              .group_by(Alert.status)).all())
    return {"risk_engine_vs_simulator": {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
                                         "precision": tp / (tp + fp) if tp + fp else None,
                                         "recall": tp / (tp + fn) if tp + fn else None,
                                         "false_positive_rate": fp / (fp + tn) if fp + tn else None},
            "analyst_outcomes": analyst,
            "period": _period(minutes),
            "definition": "flagged = risk category MEDIUM or HIGH; labels = simulator ground truth (synthetic)"}


@router.get("/analytics/merchants", tags=["analytics"])
def merchants(user: CurrentUser, db: DB, limit: Annotated[int, Query(ge=1)] = 15, minutes: WindowMinutes = None,
              category: Annotated[str | None, Query(max_length=64)] = None) -> dict[str, Any]:
    base = select().select_from(Transaction).join(Prediction)
    base = _windowed(base, minutes)
    if category:
        base = base.where(Transaction.merchant_category == category)
    cat = db.execute(base.add_columns(Transaction.merchant_category, func.count(), func.sum(Transaction.amount),
                                      func.sum(FLAGGED), func.avg(Prediction.risk_score))
                     .group_by(Transaction.merchant_category)).all()
    # ties broken by volume then id, so the ranking is stable between refreshes
    top = db.execute(base.add_columns(Transaction.merchant_id, func.max(Transaction.merchant_category), func.count(),
                                      func.sum(FLAGGED), func.avg(Prediction.risk_score), func.max(Prediction.risk_score))
                     .group_by(Transaction.merchant_id)
                     .order_by(func.sum(FLAGGED).desc(), func.count().desc(), Transaction.merchant_id)
                     .limit(min(limit, 50))).all()
    return {"categories": [{"category": c, "count": n, "volume": float(v or 0), "flagged": int(f or 0),
                            "flag_rate": (f or 0) / n if n else 0, "avg_risk": a} for c, n, v, f, a in cat],
            "top_flagged_merchants": [{"merchant_id": m, "category": c, "count": n, "flagged": int(f or 0),
                                       "flag_rate": (f or 0) / n if n else 0, "avg_risk": a, "max_risk": mx}
                                      for m, c, n, f, a, mx in top],
            "period": _period(minutes), "category": category,
            "ranking": "merchants ordered by number of MEDIUM/HIGH-scored transactions, then transaction count"}


@router.get("/analytics/hourly", tags=["analytics"])
def hourly(user: CurrentUser, db: DB, minutes: WindowMinutes = None) -> dict[str, Any]:
    if db.get_bind().dialect.name == "postgresql":
        h = func.extract("hour", Transaction.event_time)
    else:
        h = cast(func.strftime("%H", Transaction.event_time), Integer)
    h = h.label("h")
    rows = db.execute(_windowed(select(h, func.count(), func.sum(FLAGGED)).join(Prediction), minutes).group_by(h)).all()
    m = {int(a): (b, int(c or 0)) for a, b, c in rows}
    return {"hours": [{"hour": i, "count": m.get(i, (0, 0))[0], "flagged": m.get(i, (0, 0))[1]} for i in range(24)],
            "timezone": "UTC (event_time)", "period": _period(minutes)}


@router.get("/analytics/velocity", tags=["analytics"])
def velocity(user: CurrentUser, db: DB, minutes: Annotated[int, Query(ge=1, le=1440)] = 15,
             limit: Annotated[int, Query(ge=1)] = 15) -> dict[str, Any]:
    rows = db.execute(select(Transaction.customer_id, func.count(), func.sum(Transaction.amount), func.max(Prediction.risk_score))
                      .join(Prediction).where(Transaction.processed_at >= _since(minutes))
                      .group_by(Transaction.customer_id)
                      .order_by(func.count().desc(), Transaction.customer_id).limit(min(limit, 50))).all()
    return {"window_minutes": minutes, "customers": [
        {"customer_id": c, "count": n, "amount": float(a or 0), "max_risk": r} for c, n, a, r in rows]}


@router.get("/analytics/geo", tags=["analytics"])
def geo(user: CurrentUser, db: DB, minutes: Annotated[int, Query(ge=1, le=10080)] = 60,
        min_risk: Annotated[float, Query(ge=0, le=100)] = 0) -> dict[str, Any]:
    rows = db.execute(select(Transaction.city, Transaction.country, func.avg(Transaction.lat), func.avg(Transaction.lon),
                             func.count(), func.sum(case((Prediction.risk_category == "HIGH", 1), else_=0)),
                             func.sum(case((Prediction.risk_category == "MEDIUM", 1), else_=0)),
                             func.avg(Prediction.risk_score), func.bool_and(Transaction.location_simulated)
                             if db.get_bind().dialect.name == "postgresql" else func.min(Transaction.location_simulated))
                      .join(Prediction).where(Transaction.processed_at >= _since(minutes), Prediction.risk_score >= min_risk)
                      .group_by(Transaction.city, Transaction.country)).all()
    points = [{"city": c, "country": k, "lat": la, "lon": lo, "count": n, "high": int(h or 0), "medium": int(m or 0),
               "avg_risk": a, "simulated": bool(s)} for c, k, la, lo, n, h, m, a, s in rows if not (la == 0 and lo == 0)]
    unknown = sum(r[4] for r in rows if r[2] == 0 and r[3] == 0)
    return {"window_minutes": minutes, "points": points, "without_location": unknown,
            "available": bool(points), "label": "SIMULATED GEOGRAPHY — synthetic locations, not real customers"}


# ------------------------------------------------------------------ infrastructure
@router.get("/infrastructure/status", tags=["infrastructure"])
def infrastructure(user: CurrentUser, db: DB) -> dict[str, Any]:
    rt = get_runtime()
    s = rt.settings
    out: dict[str, Any] = {"profile": s.profile, "checked_at": datetime.now(timezone.utc).isoformat()}
    t0 = time.perf_counter()
    try:
        db.execute(text("SELECT 1"))
        out["database"] = {"status": "UP", "dialect": db.get_bind().dialect.name,
                           "latency_ms": round((time.perf_counter() - t0) * 1000, 2)}
    except Exception as exc:  # noqa: BLE001
        out["database"] = {"status": "DOWN", "error": exc.__class__.__name__}
    out["redis"] = ({"status": "UP" if rt.redis_ok() else "DOWN"} if s.redis_url
                    else {"status": "NOT_CONFIGURED", "note": "not used in the local profile"})
    out["dead_letters_db"] = db.scalar(select(func.count(DeadLetter.id))) or 0
    recent_dl = db.scalars(select(DeadLetter).order_by(DeadLetter.id.desc()).limit(10))
    out["recent_dead_letters"] = [{"id": d.id, "source": d.source, "reason": d.reason, "event_id": d.event_id,
                                   "created_at": d.created_at} for d in recent_dl]
    if s.profile == "local":
        out["pipeline"] = {"mode": "LOCAL in-process pipeline (simplified replacement for Kafka + Spark)",
                           "counters": dict(rt.pipeline.counters) if rt.pipeline else {},
                           "feature_store_customers": len(rt.feature_store),
                           "warm_started_events": rt.warm_started_events}
        out["kafka"] = {"status": "NOT_USED", "note": "Kafka is only used in the DISTRIBUTED profile"}
        out["spark"] = {"status": "NOT_USED", "note": "Spark is only used in the DISTRIBUTED profile"}
        return out
    from app.services.streaming_status import detect_stream_processor, heartbeat, kafka_status, spark_status

    out["kafka"] = kafka_status(s.kafka_bootstrap_servers or "")
    out["spark"] = spark_status(rt.redis, out["kafka"])
    out["processor"] = heartbeat(rt.redis, "fraudstream:processor:heartbeat")
    out["consumer"] = heartbeat(rt.redis, "fraudstream:consumer:heartbeat")
    out["stream_processor"] = detect_stream_processor(rt.redis)
    return out


# ------------------------------------------------------------------ reports
@router.get("/reports/model-evaluation", tags=["reports"])
def model_report(user: CurrentUser, db: DB, fmt: Annotated[str, Query(alias="format", pattern="^(md|json)$")] = "md") -> Response:
    models = list(db.scalars(select(ModelVersion).order_by(ModelVersion.trained_at.desc(), ModelVersion.model_name)))
    if fmt == "json":
        payload = [{"model": f"{m.model_name}:{m.version}", "status": m.status, "threshold": m.threshold,
                    "metrics": m.metrics, "latency": m.inference_latency, "hyperparameters": m.hyperparameters}
                   for m in models]
        return Response(json.dumps(payload, indent=2, default=str), media_type="application/json",
                        headers={"Content-Disposition": "attachment; filename=model-evaluation.json"})
    lines = ["# FraudStream — Model Evaluation Report", "",
             f"Generated: {datetime.now(timezone.utc).isoformat()}  ", f"Host: {platform.platform()}", "",
             "All metrics below were produced by the training pipeline on the recorded dataset. "
             "Synthetic-data results are optimistic and do not represent real-world fraud detection performance. "
             "Model scores are uncalibrated and must not be read as probabilities.", "",
             "| Model | Version | Status | Thr | Test PR-AUC | Precision | Recall | F1 | FPR | ROC-AUC | p50 ms |",
             "|---|---|---|---|---|---|---|---|---|---|---|"]
    for m in models:
        t = (m.metrics or {}).get("test") or {}

        def f(v: Any) -> str:
            return f"{v:.4f}" if isinstance(v, (int, float)) else "n/a"
        lines.append(f"| {m.model_name} | {m.version} | {m.status} | {m.threshold:.3f} | {f(t.get('pr_auc'))} | "
                     f"{f(t.get('precision'))} | {f(t.get('recall'))} | {f(t.get('f1'))} | "
                     f"{f(t.get('false_positive_rate'))} | {f(t.get('roc_auc'))} | "
                     f"{f((m.inference_latency or {}).get('p50_ms'))} |")
    lines += ["", "## Confusion matrices (test split)", ""]
    for m in models:
        cm = ((m.metrics or {}).get("test") or {}).get("confusion_matrix")
        if cm:
            lines.append(f"- **{m.model_name}:{m.version}** TN={cm['tn']} FP={cm['fp']} FN={cm['fn']} TP={cm['tp']}")
    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/markdown",
                             headers={"Content-Disposition": "attachment; filename=model-evaluation.md"})


@router.get("/reports/alerts.csv", tags=["reports"])
def alerts_csv(user: CurrentUser, db: DB, status_: Annotated[str | None, Query(alias="status")] = None) -> Response:
    stmt = select(Alert).order_by(Alert.created_at.desc()).limit(50000)
    if status_:
        stmt = stmt.where(Alert.status.in_(status_.split(",")))
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["alert_uid", "created_at", "status", "risk_category", "risk_score", "detection_method", "customer_id",
                "transaction_id", "amount", "currency", "rules", "model_version", "assigned_to"])
    g = csv_value  # formula-injection guard (Phase 12): ids/usernames come from ingested or user-supplied text
    for a in db.scalars(stmt):
        w.writerow([g(a.alert_uid), a.created_at.isoformat(), g(a.status), g(a.risk_category), f"{a.risk_score:.2f}",
                    g(a.detection_method), g(a.customer_id), g(a.transaction.transaction_id), f"{a.transaction.amount}",
                    g(a.transaction.currency), g("|".join(r["code"] for r in a.triggered_rules)), g(a.model_version or ""),
                    g(a.assigned_to.username if a.assigned_to else "")])
    return Response(buf.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=alerts.csv"})


@router.get("/reports/performance", tags=["reports"])
def performance_report(user: CurrentUser, db: DB) -> Response:
    rt = get_runtime()
    bench_dir = rt.settings.artifacts_dir.parent.parent / "benchmarks" / "results"
    results = []
    for p in sorted(bench_dir.glob("*.json")) if bench_dir.exists() else []:
        try:
            results.append(json.loads(p.read_text()))
        except json.JSONDecodeError:
            continue
    runs = [{"run_uid": r.run_uid, "status": r.status, "events": r.events_generated, "started_at": r.started_at,
             "ended_at": r.ended_at, "config": r.config, "profile": r.profile}
            for r in db.scalars(select(SimulationRun).order_by(SimulationRun.started_at.desc()).limit(50))]
    drift = [{"computed_at": d.computed_at, "status": d.status, "max_psi": d.max_psi}
             for d in db.scalars(select(DriftReport).order_by(DriftReport.computed_at.desc()).limit(20))]
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "profile": rt.settings.profile,
               "overview": overview(user, db), "benchmarks_measured": results, "simulation_runs": runs,
               "drift_reports": drift,
               "note": "benchmarks_measured contains only results produced by benchmarks/*.py on this machine"}
    return Response(json.dumps(payload, indent=2, default=str), media_type="application/json",
                    headers={"Content-Disposition": "attachment; filename=performance-report.json"})


@router.get("/reports/benchmarks", tags=["reports"])
def benchmarks(user: CurrentUser) -> list[dict[str, Any]]:
    rt = get_runtime()
    bench_dir = rt.settings.artifacts_dir.parent.parent / "benchmarks" / "results"
    out = []
    for p in sorted(bench_dir.glob("*.json"), reverse=True) if bench_dir.exists() else []:
        try:
            out.append({"file": p.name, **json.loads(p.read_text())})
        except json.JSONDecodeError:
            continue
    return out


@router.get("/audit", tags=["audit"])
def audit_log(admin: AdminUser, db: DB, page: Page = 1,
              size: Annotated[int, Query(ge=1, le=200)] = 50, entity_type: str | None = None,
              actor: str | None = None) -> dict[str, Any]:
    stmt = select(AuditLog)
    if entity_type:   # one type, or several separated by commas (e.g. risk_rule,system_setting)
        stmt = stmt.where(AuditLog.entity_type.in_([t.strip() for t in entity_type.split(",") if t.strip()]))
    if actor:
        stmt = stmt.where(AuditLog.actor_name == actor)
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = db.scalars(stmt.order_by(AuditLog.id.desc()).offset((page - 1) * size).limit(size))
    return {"items": [audit_out(a) for a in rows], "total": total, "page": page, "size": size}
