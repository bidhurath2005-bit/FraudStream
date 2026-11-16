"""Model registry, training, comparison, drift, risk rules, settings, datasets."""
from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any, NoReturn

import pandas as pd
from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Query, Request, UploadFile, status
from pydantic import BaseModel, ValidationError
from sqlalchemy import func, select

from app.api.params import IdPath, parse_id_list
from app.core.runtime import get_runtime
from app.models.entities import (
    AuditLog,
    DatasetMetadata,
    DriftReport,
    ModelExperiment,
    ModelVersion,
    Prediction,
    RiskRule,
    SystemSetting,
    Transaction,
    User,
)
from app.risk_engine.engine import CHECKS, RiskSettings
from app.schemas.api import (
    ActivateRequest,
    RiskSettingsUpdate,
    RollbackRequest,
    RuleUpdate,
    SettingUpdate,
    TrainRequest,
    model_out,
    rule_out,
)
from app.security.auth import DB, AdminUser, CurrentUser
from app.services import audit
from app.services.model_registry import activate, rollback_target, sync_registry, write_registry_active
from app.services.runtime_config import DEFAULT_SETTINGS, get_setting
from ml.datasets.public import SCHEMAS, DatasetValidationError, validate_frame
from ml.drift.psi import compute_drift
from ml.features.definitions import FEATURE_CONFIG_VERSION, FEATURE_DESCRIPTIONS, NUMERIC_FEATURES

router = APIRouter()


# ------------------------------------------------------------------ models
@router.get("/models", tags=["models"])
def list_models(user: CurrentUser, db: DB, kind: str | None = None) -> list[dict[str, Any]]:
    stmt = select(ModelVersion).order_by(ModelVersion.trained_at.desc(), ModelVersion.model_name)
    if kind:
        stmt = stmt.where(ModelVersion.kind == kind)
    return [model_out(m) for m in db.scalars(stmt)]


@router.get("/models/active", tags=["models"])
def active(user: CurrentUser, db: DB) -> dict[str, Any]:
    rt = get_runtime()
    rows = {m.kind: model_out(m) for m in db.scalars(select(ModelVersion).where(ModelVersion.status == "ACTIVE"))}
    loaded = {"supervised": rt.scorer.supervised.label if rt.scorer.supervised else None,
              "anomaly": rt.scorer.anomaly.label if rt.scorer.anomaly else None}
    return {"active": rows, "loaded_in_api": loaded, "model_error": rt.model_error,
            "feature_config_version": FEATURE_CONFIG_VERSION,
            "artifacts_writable": os.access(rt.settings.artifacts_dir, os.W_OK)}


_SHA_CACHE: dict[tuple[str, int, int], str] = {}


def _sha256(p: Path) -> str:
    st = p.stat()
    key = (str(p), st.st_size, st.st_mtime_ns)
    if key not in _SHA_CACHE:
        import hashlib

        h = hashlib.sha256()
        with p.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
        _SHA_CACHE[key] = h.hexdigest()
    return _SHA_CACHE[key]


@router.get("/models/artifacts", tags=["models"])
def artifacts(user: CurrentUser, db: DB) -> dict[str, Any]:
    """Files actually present in the artifacts directory (read-only listing; nothing is modified)."""
    d = get_runtime().settings.artifacts_dir
    files = []
    for p in sorted(d.rglob("*")) if d.exists() else []:
        if p.is_file():
            st = p.stat()
            files.append({"path": p.relative_to(d).as_posix(), "bytes": st.st_size,
                          "modified_at": pd.Timestamp(st.st_mtime, unit="s", tz="UTC").isoformat(),
                          "sha256": _sha256(p)})
    reg_path = d / "registry.json"
    try:
        registry_active = json.loads(reg_path.read_text()).get("active", {}) if reg_path.exists() else {}
    except (OSError, json.JSONDecodeError):
        registry_active = {}
    db_active = {m.kind: f"{m.model_name}:{m.version}"
                 for m in db.scalars(select(ModelVersion).where(ModelVersion.status == "ACTIVE"))}
    return {"artifacts_dir_writable": os.access(d, os.W_OK), "exists": d.exists(), "files": files,
            "registry_active": {k: f"{v['model_name']}:{v['version']}" for k, v in registry_active.items()},
            "database_active": db_active,
            "note": "registry.json is the on-disk record of the active models; the database is authoritative "
                    "for the API. runtime_config.json is derived runtime output, not a model artifact."}


@router.get("/models/live-inference", tags=["models"])
def live_inference(user: CurrentUser, db: DB, limit: Annotated[int, Query(ge=10, le=20000)] = 5000) -> dict[str, Any]:
    """Inference latency MEASURED on live predictions (most recent ``limit`` rows), per model version."""
    rows = db.execute(select(Prediction.model_version, Prediction.inference_ms, Prediction.predicted_at)
                      .where(Prediction.inference_ms.is_not(None))
                      .order_by(Prediction.id.desc()).limit(limit)).all()
    by: dict[str, list[float]] = {}
    span: dict[str, list[Any]] = {}
    for mv, ms, at in rows:
        key = mv or "rules-only (no model)"
        by.setdefault(key, []).append(float(ms))
        s = span.setdefault(key, [at, at])
        s[0], s[1] = min(s[0], at), max(s[1], at)

    def q(v: list[float], p: float) -> float:
        return v[min(len(v) - 1, int(p * len(v)))]
    out = []
    for k, v in by.items():
        v.sort()
        out.append({"model_version": k, "samples": len(v), "mean_ms": sum(v) / len(v), "p50_ms": q(v, 0.5),
                    "p95_ms": q(v, 0.95), "p99_ms": q(v, 0.99), "max_ms": v[-1],
                    "first_at": span[k][0], "last_at": span[k][1]})
    total = db.scalar(select(func.count(Prediction.id))) or 0
    return {"window_rows": len(rows), "total_predictions": total, "models": sorted(out, key=lambda r: -r["samples"]),
            "definition": "Prediction.inference_ms recorded by the stream processor for each scored event "
                          "(feature vector -> model score), most recent predictions first."}


@router.get("/models/experiments/{experiment_id}", tags=["models"])
def experiment_detail(experiment_id: IdPath, user: CurrentUser, db: DB) -> dict[str, Any]:
    e = db.get(ModelExperiment, experiment_id)
    if e is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "experiment not found")
    versions = [{"id": m.id, "model_name": m.model_name, "kind": m.kind, "status": m.status}
                for m in db.scalars(select(ModelVersion).where(ModelVersion.experiment_id == e.id)
                                    .order_by(ModelVersion.model_name))]
    return {"id": e.id, "experiment_version": e.experiment_version, "status": e.status,
            "triggered_by": e.triggered_by, "started_at": e.started_at, "finished_at": e.finished_at,
            "summary": e.summary, "model_versions": versions}


@router.get("/models/compare", tags=["models"])
def compare(user: CurrentUser, db: DB, ids: Annotated[str | None, Query()] = None,
            experiment: str | None = None) -> dict[str, Any]:
    stmt = select(ModelVersion)
    if ids:
        try:
            stmt = stmt.where(ModelVersion.id.in_(parse_id_list(ids)))
        except ValueError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "ids must be integers in range") from exc
    elif experiment:
        stmt = stmt.where(ModelVersion.version == experiment)
    else:
        latest = db.scalar(select(func.max(ModelVersion.version)))
        stmt = stmt.where(ModelVersion.version == latest)
    rows = []
    for m in db.scalars(stmt.order_by(ModelVersion.model_name)):
        t = (m.metrics or {}).get("test") or {}
        v = (m.metrics or {}).get("validation") or {}
        u = (m.metrics or {}).get("unseen_customer_test") or {}
        rows.append({"id": m.id, "model_name": m.model_name, "version": m.version, "kind": m.kind, "status": m.status,
                     "threshold": m.threshold, "validation_pr_auc": v.get("pr_auc"), "test": t,
                     "unseen_customer_test": u, "latency": m.inference_latency,
                     "dataset": m.dataset.name if m.dataset else None})
    return {"models": rows, "evaluation_note": "Test metrics come from a temporally held-out split never used for "
            "model or threshold selection. Synthetic-data results are optimistic and not representative of real fraud."}


@router.get("/models/experiments", tags=["models"])
def experiments(user: CurrentUser, db: DB) -> list[dict[str, Any]]:
    return [{"id": e.id, "experiment_version": e.experiment_version, "status": e.status, "triggered_by": e.triggered_by,
             "started_at": e.started_at, "finished_at": e.finished_at,
             "recommended": e.summary.get("recommended_supervised"), "split": e.summary.get("split"),
             "dataset": e.summary.get("dataset", {}).get("name"), "duration_seconds": e.summary.get("duration_seconds")}
            for e in db.scalars(select(ModelExperiment).order_by(ModelExperiment.started_at.desc()))]


@router.get("/models/training/status", tags=["models"])
def training_status(user: CurrentUser) -> dict[str, Any]:
    st = dict(get_runtime().training.state)
    st.pop("traceback", None)
    return st


def _require_writable_artifacts() -> None:
    """Model changes write to ml/artifacts. In the container stack it is mounted READ-ONLY by
    default (to protect the trained artifacts); refuse explicitly instead of failing half-way."""
    d = get_runtime().settings.artifacts_dir
    if not os.access(d, os.W_OK):
        raise HTTPException(status.HTTP_409_CONFLICT,
                            "model artifacts are mounted read-only in this deployment; "
                            "set ARTIFACTS_MOUNT_MODE=rw (docker compose) to allow training/activation")


@router.post("/models/train", tags=["models"], status_code=202)
def train(body: TrainRequest, admin: AdminUser, db: DB) -> dict[str, Any]:
    _require_writable_artifacts()
    try:
        st = get_runtime().training.start(admin.username, body.quick)
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    audit.record(db, admin, "MODEL_TRAINING_STARTED", "model_experiment", "pending", details={"quick": body.quick})
    db.commit()
    return st


@router.post("/models/sync", tags=["models"])
def sync(admin: AdminUser, db: DB) -> dict[str, Any]:
    return {"added": sync_registry(db, get_runtime().settings.artifacts_dir)}


@router.get("/models/{model_id}", tags=["models"])
def get_model(model_id: IdPath, user: CurrentUser, db: DB) -> dict[str, Any]:
    m = db.get(ModelVersion, model_id)
    if m is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "model not found")
    out = model_out(m)
    live = db.execute(select(func.count(), func.avg(Prediction.inference_ms))
                      .where(Prediction.model_version == f"{m.model_name}:{m.version}")).one()
    out["live"] = {"predictions": live[0], "avg_inference_ms": live[1]}
    return out


def _apply_model_change(db: DB) -> None:
    rt = get_runtime()
    write_registry_active(db, rt.settings.artifacts_dir)
    rt.reload(db)


@router.post("/models/{model_id}/activate", tags=["models"])
def activate_model(model_id: IdPath, body: ActivateRequest, admin: AdminUser, db: DB) -> dict[str, Any]:
    m = db.get(ModelVersion, model_id)
    if m is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "model not found")
    if m.feature_config_version != FEATURE_CONFIG_VERSION:
        raise HTTPException(status.HTTP_409_CONFLICT, "model feature config does not match runtime")
    _require_writable_artifacts()
    prev = activate(db, m, admin, body.reason)
    db.commit()
    _apply_model_change(db)
    return {"activated": model_out(m), "previous": model_out(prev) if prev else None,
            "loaded_in_api": get_runtime().model_error is None}


@router.post("/models/rollback", tags=["models"])
def rollback(body: RollbackRequest, admin: AdminUser, db: DB) -> dict[str, Any]:
    target = rollback_target(db, body.kind)
    if target is None:
        raise HTTPException(status.HTTP_409_CONFLICT, "no previously active model to roll back to")
    _require_writable_artifacts()
    prev = activate(db, target, admin, f"rollback: {body.reason}")
    db.commit()
    _apply_model_change(db)
    return {"activated": model_out(target), "previous": model_out(prev) if prev else None}


# ------------------------------------------------------------------ drift
@router.get("/drift/current", tags=["drift"])
def drift_current(user: CurrentUser, db: DB, persist: bool = True) -> dict[str, Any]:
    rt = get_runtime()
    sup = rt.scorer.supervised
    if sup is None or sup.reference_profile is None:
        raise HTTPException(status.HTTP_409_CONFLICT, "no active model with a reference profile")
    cfg = get_setting(db, "drift")
    rows = db.execute(select(Prediction.features, Prediction.model_score, Transaction.sim_is_suspicious)
                      .join(Transaction).where(Prediction.model_version == sup.label)
                      .order_by(Prediction.id.desc()).limit(int(cfg.get("window_rows", 2000)))).all()
    feats = pd.DataFrame([r[0] for r in rows]) if rows else pd.DataFrame()
    scores = [r[1] for r in rows if r[1] is not None]
    report = compute_drift(sup.reference_profile, feats, scores,
                           {"moderate": cfg["moderate"], "significant": cfg["significant"]}, int(cfg.get("min_rows", 200)))
    # performance on labelled live data (simulator ground truth) - an estimate, not proof of concept drift
    labelled = [(r[1], r[2]) for r in rows if r[1] is not None and r[2] is not None]
    perf = None
    if labelled and any(lbl for _, lbl in labelled):
        tp = sum(1 for s, lbl in labelled if s >= sup.threshold and lbl)
        fp = sum(1 for s, lbl in labelled if s >= sup.threshold and not lbl)
        fn = sum(1 for s, lbl in labelled if s < sup.threshold and lbl)
        test = sup.metadata["metrics"]["test"]
        perf = {"rows": len(labelled), "positives": tp + fn, "precision": tp / (tp + fp) if tp + fp else None,
                "recall": tp / (tp + fn) if tp + fn else None, "reference_test_precision": test["precision"],
                "reference_test_recall": test["recall"], "label_source": "simulator ground truth (synthetic)"}
    report.update(model_version=sup.label, labelled_performance=perf,
                  interpretation="PSI indicates DATA drift (input distribution change). Concept drift can only be "
                                 "assessed from labelled outcomes (see labelled_performance).")
    if persist and report["status"] != "INSUFFICIENT_DATA":
        db.add(DriftReport(model_version=sup.label, window_rows=report["rows"], status=report["status"],
                           max_psi=report.get("max_psi"), report=report))
        db.commit()
    return report


@router.get("/drift/history", tags=["drift"])
def drift_history(user: CurrentUser, db: DB, limit: Annotated[int, Query(ge=1)] = 50) -> list[dict[str, Any]]:
    rows = db.scalars(select(DriftReport).order_by(DriftReport.computed_at.desc()).limit(min(limit, 500)))
    return [{"id": r.id, "model_version": r.model_version, "computed_at": r.computed_at, "rows": r.window_rows,
             "status": r.status, "max_psi": r.max_psi,
             "drifted_features": r.report.get("drifted_features", []),
             "score_psi": (r.report.get("score_psi") or {}).get("psi")} for r in rows]


@router.get("/drift/reports/{report_id}", tags=["drift"])
def drift_report(report_id: IdPath, user: CurrentUser, db: DB) -> dict[str, Any]:
    r = db.get(DriftReport, report_id)
    if r is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "drift report not found")
    return {"id": r.id, "computed_at": r.computed_at, "model_version": r.model_version, "rows": r.window_rows,
            "status": r.status, "max_psi": r.max_psi, "report": r.report}


# ------------------------------------------------------------------ risk rules & settings
# Configuration changes (Phase 11C hardening): admin only, validated on the server, optimistic
# concurrency via ``expected_updated_at`` (409 if someone else changed it first), row lock on
# PostgreSQL, only fields that actually change are audited, and rejected or denied attempts are
# audited too (``*_REJECTED`` / ``CONFIG_CHANGE_DENIED``). No secrets are involved in any of these.

# Per-parameter bounds for rule params: (integer?, minimum, minimum inclusive?, maximum)
PARAM_BOUNDS: dict[str, tuple[bool, float, bool, float]] = {
    "count": (True, 1, True, 1000), "min_history": (True, 1, True, 1000),
    "ratio": (False, 1, True, 1000), "zscore": (False, 0, False, 100),
    "absolute": (False, 0, False, 1e9), "kmh": (False, 0, False, 20000),
}
DRIFT_BOUNDS = {"window_rows": (50, 100_000), "min_rows": (1, 100_000)}
SETTING_USED_BY = {
    "risk": "risk engine (API scoring, Spark job via the published runtime config)",
    "drift": "drift check (Model Intelligence → Drift, GET /drift/current)",
    "simulation_defaults": None,   # stored, but no component reads it
    "preferences": None,           # stored, but no component reads it
}


def _config_target(request: Request) -> tuple[str, str]:
    pp = request.path_params
    if "code" in pp:
        return "risk_rule", str(pp["code"])[:64]
    if "key" in pp:
        return "system_setting", str(pp["key"])[:64]
    return "system_setting", "risk"


def require_config_admin(request: Request, user: CurrentUser, db: DB) -> User:
    """Admin-only configuration change; a denied attempt is recorded in the audit trail."""
    if user.role.name != "ADMIN":
        et, eid = _config_target(request)
        audit.record(db, user, "CONFIG_CHANGE_DENIED", et, eid,
                     details={"method": request.method, "path": request.url.path, "role": user.role.name})
        db.commit()
        raise HTTPException(status.HTTP_403_FORBIDDEN, "administrator role required")
    return user


ConfigAdmin = Annotated[User, Depends(require_config_admin)]


def _attempt(body: Any) -> str:
    try:
        return json.dumps(body, sort_keys=True, default=str)[:1000]
    except (TypeError, ValueError):
        return "<unserialisable>"


def _reject(db: DB, user: User, action: str, entity_type: str, entity_id: str, reason: str, body: Any,
            code: int = status.HTTP_422_UNPROCESSABLE_CONTENT) -> NoReturn:
    """Abort a configuration change: nothing is changed; the rejection itself is audited."""
    db.rollback()
    audit.record(db, user, action, entity_type, entity_id,
                 details={"reason": reason[:300], "attempted": _attempt(body), "status_code": code})
    db.commit()
    raise HTTPException(code, reason)


def _validate(model: type[BaseModel], body: Any, reject: Callable[[str], NoReturn]) -> Any:
    try:
        return model.model_validate(body)
    except ValidationError as exc:
        reject("invalid value — " + "; ".join(f"{'.'.join(str(x) for x in e['loc']) or 'body'}: {e['msg']}"
                                               for e in exc.errors()))


def _same_instant(a: datetime | None, b: datetime | None) -> bool:
    if a is None or b is None:
        return False
    a = a if a.tzinfo else a.replace(tzinfo=timezone.utc)
    b = b if b.tzinfo else b.replace(tzinfo=timezone.utc)
    return a == b


VERSION_REQUIRED = ("expected_updated_at is required: send the updated_at value of the configuration you loaded "
                    "(null if it has never been saved), so a concurrent change cannot be overwritten silently")


def _check_version(present: bool, expected: datetime | None, current: datetime | None, conflict: str,
                   reject: Callable[..., NoReturn]) -> None:
    """Optimistic concurrency (Phase 12: mandatory). Called only after the values were validated, so an invalid
    value is still answered 422 with its reason. Missing version -> 428; stale version -> 409."""
    if not present:
        reject(VERSION_REQUIRED, status.HTTP_428_PRECONDITION_REQUIRED)
    if expected is None and current is None:      # never saved, and the client saw it unsaved
        return
    if not _same_instant(expected, current):
        reject(conflict, status.HTTP_409_CONFLICT)


def _last_changes(db: DB, entity_type: str, actions: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    """Latest successful change per entity id, from the audit trail."""
    ids = (select(func.max(AuditLog.id)).where(AuditLog.entity_type == entity_type, AuditLog.action.in_(actions))
           .group_by(AuditLog.entity_id))
    return {a.entity_id: {"by": a.actor_name, "at": a.created_at, "action": a.action}
            for a in db.scalars(select(AuditLog).where(AuditLog.id.in_(ids)))}


def _propagation(rt: Any) -> dict[str, str]:
    # "ok"/"error" only: the raw publish status can contain file-system paths
    return {k: ("ok" if v == "ok" else "error") for k, v in (rt.config_publish_status or {}).items()}


def _check_param(name: str, v: float) -> str | None:
    if not math.isfinite(v):
        return f"{name} must be a finite number"
    is_int, lo, lo_incl, hi = PARAM_BOUNDS.get(name, (False, 0, True, 1e9))
    if is_int and v != int(v):
        return f"{name} must be a whole number"
    if (v < lo if lo_incl else v <= lo) or v > hi:
        return f"{name} must be {'>=' if lo_incl else '>'} {lo:g} and <= {hi:g}"
    return None


@router.get("/risk/rules", tags=["risk"])
def list_rules(user: CurrentUser, db: DB) -> list[dict[str, Any]]:
    last = _last_changes(db, "risk_rule", ("RULE_UPDATED",))
    return [{**rule_out(r), "last_change": last.get(r.code), "editable": r.code in CHECKS,
             "param_bounds": {k: dict(zip(("integer", "min", "min_inclusive", "max"), PARAM_BOUNDS[k], strict=True))
                              for k in r.params if k in PARAM_BOUNDS}}
            for r in db.scalars(select(RiskRule).order_by(RiskRule.id))]


@router.patch("/risk/rules/{code}", tags=["risk"])
def update_rule(code: str, body: Annotated[dict[str, Any], Body()], admin: ConfigAdmin, db: DB) -> dict[str, Any]:
    def reject(reason: str, c: int = status.HTTP_422_UNPROCESSABLE_CONTENT) -> NoReturn:
        _reject(db, admin, "RULE_UPDATE_REJECTED", "risk_rule", code[:64], reason, body, c)
    r = db.scalar(select(RiskRule).where(RiskRule.code == code).with_for_update())
    if r is None or code not in CHECKS:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "rule not found")
    upd: RuleUpdate = _validate(RuleUpdate, body, reject)
    if upd.params is not None:
        unknown = set(upd.params) - set(r.params)
        if unknown:
            reject(f"unknown params {sorted(unknown)}")
        for k, v in upd.params.items():
            problem = _check_param(k, v)
            if problem:
                reject(problem)
    _check_version("expected_updated_at" in upd.model_fields_set, upd.expected_updated_at, r.updated_at,
                   "this rule was changed by someone else since you loaded it; reload and review the current values",
                   reject)
    changed: list[str] = []
    if upd.params is not None:
        new_params = {**r.params, **{k: int(v) if PARAM_BOUNDS.get(k, (False,))[0] else float(v)
                                     for k, v in upd.params.items()}}
        if new_params != r.params:
            audit.record(db, admin, "RULE_UPDATED", "risk_rule", code, field="params",
                         old=json.dumps(r.params, sort_keys=True), new=json.dumps(new_params, sort_keys=True))
            r.params = new_params
            changed.append("params")
    if upd.weight is not None and upd.weight != r.weight:
        audit.record(db, admin, "RULE_UPDATED", "risk_rule", code, field="weight", old=r.weight, new=upd.weight)
        r.weight = upd.weight
        changed.append("weight")
    if upd.enabled is not None and upd.enabled != r.enabled:
        audit.record(db, admin, "RULE_UPDATED", "risk_rule", code, field="enabled", old=r.enabled, new=upd.enabled)
        r.enabled = upd.enabled
        changed.append("enabled")
    db.commit()
    rt = get_runtime()
    if changed:
        rt.reload(db)
    db.refresh(r)
    last = _last_changes(db, "risk_rule", ("RULE_UPDATED",)).get(code)
    return {**rule_out(r), "last_change": last, "editable": True, "changed_fields": changed,
            "propagation": _propagation(rt) if changed else None}


def _risk_state(db: DB) -> dict[str, Any]:
    row = db.get(SystemSetting, "risk")
    last = _last_changes(db, "system_setting", ("RISK_SETTINGS_UPDATED",)).get("risk")
    return {"settings": get_setting(db, "risk"), "defaults": DEFAULT_SETTINGS["risk"],
            "updated_at": row.updated_at if row else None, "last_change": last, "editable_by": "ADMIN",
            "used_by": SETTING_USED_BY["risk"]}


@router.get("/risk/settings", tags=["risk"])
def risk_settings(user: CurrentUser, db: DB) -> dict[str, Any]:
    return _risk_state(db)


@router.put("/risk/settings", tags=["risk"])
def update_risk_settings(body: Annotated[dict[str, Any], Body()], admin: ConfigAdmin, db: DB) -> dict[str, Any]:
    def reject(reason: str, c: int = status.HTTP_422_UNPROCESSABLE_CONTENT) -> NoReturn:
        _reject(db, admin, "RISK_SETTINGS_REJECTED", "system_setting", "risk", reason, body, c)
    payload = dict(body)
    version_sent = "expected_updated_at" in payload
    expected_raw = payload.pop("expected_updated_at", None)
    upd: RiskSettingsUpdate = _validate(RiskSettingsUpdate, payload, reject)
    expected = None
    if expected_raw is not None:
        try:
            expected = datetime.fromisoformat(str(expected_raw).replace("Z", "+00:00"))
        except ValueError:
            reject("expected_updated_at must be an ISO-8601 timestamp")
    new = upd.model_dump()
    if not all(math.isfinite(v) for v in new.values() if isinstance(v, float)):
        reject("values must be finite numbers")
    try:
        RiskSettings(**new).validate()
    except ValueError as exc:
        reject(str(exc))
    row = db.get(SystemSetting, "risk", with_for_update=True)
    _check_version(version_sent, expected, row.updated_at if row else None,
                   "risk settings were changed by someone else since you loaded them; reload and review the current values",
                   reject)
    old = dict(row.value) if row else None
    if old == new:
        db.rollback()
        return {**_risk_state(db), "changed": False, "propagation": None}
    if row is None:
        db.add(SystemSetting(key="risk", value=new))
    else:
        row.value = new
    audit.record(db, admin, "RISK_SETTINGS_UPDATED", "system_setting", "risk", old=json.dumps(old, sort_keys=True),
                 new=json.dumps(new, sort_keys=True),
                 details={"changed": sorted(k for k in new if (old or {}).get(k) != new[k])})
    db.commit()
    rt = get_runtime()
    rt.reload(db)
    return {**_risk_state(db), "changed": True, "propagation": _propagation(rt)}


@router.get("/settings", tags=["settings"])
def get_settings_all(user: CurrentUser, db: DB) -> dict[str, Any]:
    return {k: get_setting(db, k) for k in DEFAULT_SETTINGS}


@router.get("/settings/{key}", tags=["settings"])
def get_setting_one(key: str, user: CurrentUser, db: DB) -> dict[str, Any]:
    if key not in DEFAULT_SETTINGS:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown setting")
    row = db.get(SystemSetting, key)
    action = "RISK_SETTINGS_UPDATED" if key == "risk" else "SETTING_UPDATED"
    return {"key": key, "value": get_setting(db, key), "defaults": DEFAULT_SETTINGS[key],
            "updated_at": row.updated_at if row else None,
            "last_change": _last_changes(db, "system_setting", (action,)).get(key),
            "used_by": SETTING_USED_BY.get(key), "editable_by": "ADMIN",
            "bounds": ({k: {"min": lo, "max": hi} for k, (lo, hi) in DRIFT_BOUNDS.items()} if key == "drift" else None)}


@router.put("/settings/{key}", tags=["settings"])
def put_setting(key: str, body: Annotated[dict[str, Any], Body()], admin: ConfigAdmin, db: DB) -> dict[str, Any]:
    if key not in DEFAULT_SETTINGS or key == "risk":
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown setting (use /risk/settings for risk)")

    def reject(reason: str, c: int = status.HTTP_422_UNPROCESSABLE_CONTENT) -> NoReturn:
        _reject(db, admin, "SETTING_UPDATE_REJECTED", "system_setting", key, reason, body, c)
    upd: SettingUpdate = _validate(SettingUpdate, body, reject)
    unknown = set(upd.value) - set(DEFAULT_SETTINGS[key])
    if unknown:
        reject(f"unknown keys {sorted(unknown)}")
    for k, v in upd.value.items():
        d = DEFAULT_SETTINGS[key][k]
        # bool is a subclass of int in Python: compare exact types so `true` is not accepted as a number
        if not (type(v) is type(d) or (type(d) is float and type(v) is int)):
            reject(f"{k} has wrong type")
        if isinstance(v, float) and not math.isfinite(v):
            reject(f"{k} must be a finite number")
    row = db.get(SystemSetting, key, with_for_update=True)
    old = dict(row.value) if row else {}
    new = {**DEFAULT_SETTINGS[key], **old, **upd.value}
    if key == "drift":
        if not 0 < new["moderate"] < new["significant"] <= 10:
            reject("require 0 < moderate < significant <= 10")
        for k, (lo, hi) in DRIFT_BOUNDS.items():
            if not lo <= new[k] <= hi:
                reject(f"{k} must be between {lo} and {hi}")
        if new["min_rows"] > new["window_rows"]:
            reject("min_rows must not exceed window_rows")
    _check_version("expected_updated_at" in upd.model_fields_set, upd.expected_updated_at, row.updated_at if row else None,
                   "this setting was changed by someone else since you loaded it; reload and review the current values",
                   reject)
    if new == old:
        db.rollback()
        return {key: new, "changed": False, "updated_at": row.updated_at if row else None}
    if row is None:
        db.add(SystemSetting(key=key, value=new))
    else:
        row.value = new
    audit.record(db, admin, "SETTING_UPDATED", "system_setting", key, old=json.dumps(old, sort_keys=True),
                 new=json.dumps(new, sort_keys=True), details={"changed": sorted(k for k in new if old.get(k) != new[k])})
    db.commit()
    row = db.get(SystemSetting, key)
    return {key: new, "changed": True, "updated_at": row.updated_at if row else None}


@router.get("/features", tags=["models"])
def features(user: CurrentUser) -> dict[str, Any]:
    return {"feature_config_version": FEATURE_CONFIG_VERSION, "numeric": list(NUMERIC_FEATURES),
            "descriptions": FEATURE_DESCRIPTIONS}


# ------------------------------------------------------------------ datasets
@router.get("/datasets", tags=["datasets"])
def list_datasets(user: CurrentUser, db: DB) -> dict[str, Any]:
    rows = db.scalars(select(DatasetMetadata).order_by(DatasetMetadata.created_at.desc()))
    return {"datasets": [{"id": d.id, "name": d.name, "version": d.version, "source": d.source, "rows": d.rows,
                          "positive_rate": d.positive_rate, "provenance": d.provenance, "created_at": d.created_at,
                          "content_sha256": d.content_sha256} for d in rows],
            "supported_public_schemas": [{"key": s.key, "title": s.title, "url": s.url,
                                          "required_columns": list(s.required_columns), "behavioural": s.behavioural,
                                          "notes": s.notes} for s in SCHEMAS.values()]}


@router.post("/datasets/upload", tags=["datasets"], status_code=201)
async def upload_dataset(admin: AdminUser, db: DB, kind: Annotated[str, Form()],
                         file: Annotated[UploadFile, File()]) -> dict[str, Any]:
    rt = get_runtime()
    if kind not in SCHEMAS:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, f"kind must be one of {list(SCHEMAS)}")
    name = Path(file.filename or "").name
    if not name.lower().endswith(".csv"):
        raise HTTPException(status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, "only .csv files are accepted")
    limit = rt.settings.max_upload_mb * 1024 * 1024
    fd, tmp_name = tempfile.mkstemp(suffix=".csv")
    os.close(fd)  # we reopen by path; an open handle would block deletion on Windows
    tmp = Path(tmp_name)
    size = 0
    try:
        with tmp.open("wb") as fh:
            while chunk := await file.read(1024 * 1024):
                size += len(chunk)
                if size > limit:
                    raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, f"file exceeds {rt.settings.max_upload_mb} MB")
                if size == len(chunk) and b"\x00" in chunk[:4096]:
                    raise HTTPException(status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, "binary content is not a CSV")
                fh.write(chunk)
        try:
            df = pd.read_csv(tmp)
            profile = validate_frame(df, kind)
        except (DatasetValidationError, pd.errors.ParserError, UnicodeDecodeError, ValueError) as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, f"invalid dataset: {exc}") from exc
        import hashlib

        sha = hashlib.sha256(tmp.read_bytes()).hexdigest()
        dest_dir = rt.settings.data_dir / "uploads"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f"{kind}-{sha[:12]}.csv"
        shutil.move(str(tmp), dest)
    finally:
        tmp.unlink(missing_ok=True)
    version = sha[:12]
    existing = db.scalar(select(DatasetMetadata).where(DatasetMetadata.name == kind, DatasetMetadata.version == version))
    if existing:
        raise HTTPException(status.HTTP_409_CONFLICT, "identical dataset already imported")
    ds = DatasetMetadata(name=kind, version=version, source=kind, provenance=f"uploaded by {admin.username}: {name}",
                         rows=profile["rows"], positive_rate=profile.get("positive_rate"), content_sha256=sha,
                         profile=profile, path=str(dest.relative_to(rt.settings.data_dir).as_posix()))
    db.add(ds)
    audit.record(db, admin, "DATASET_UPLOADED", "dataset", f"{kind}:{version}", details={"rows": profile["rows"]})
    db.commit()
    return {"id": ds.id, "name": kind, "version": version, "profile": profile}
