"""Request/response schemas and ORM serialisers."""
from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, Strict

from app.models.entities import (
    Alert,
    AuditLog,
    Investigation,
    InvestigationNote,
    ModelVersion,
    Prediction,
    RiskRule,
    Transaction,
    User,
)

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int
    size: int


# ----------------------------------------------------------------- requests
class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=256)


class UserCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str = Field(min_length=3, max_length=64, pattern=r"^[a-zA-Z0-9_.-]+$")
    full_name: str = Field("", max_length=120)
    password: str = Field(min_length=12, max_length=256)
    role: Literal["ADMIN", "ANALYST"]


class UserUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    full_name: str | None = Field(None, max_length=120)
    role: Literal["ADMIN", "ANALYST"] | None = None
    is_active: bool | None = None
    password: str | None = Field(None, min_length=12, max_length=256)


class AlertStatusUpdate(BaseModel):
    status: Literal["NEW", "UNDER_REVIEW", "CONFIRMED_SUSPICIOUS", "DISMISSED", "CLOSED"]
    note: str | None = Field(None, max_length=5000)


class AssignRequest(BaseModel):
    user_id: int | None


class NoteCreate(BaseModel):
    body: str = Field(min_length=1, max_length=5000)


class InvestigationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    alert_uids: list[str] = Field(min_length=1, max_length=100)
    title: str = Field(min_length=3, max_length=200)
    priority: Literal["LOW", "MEDIUM", "HIGH"] = "MEDIUM"
    assigned_to_id: int | None = None


class InvestigationStatusUpdate(BaseModel):
    status: Literal["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"]
    final_classification: Literal["CONFIRMED_SUSPICIOUS", "NOT_SUSPICIOUS", "INCONCLUSIVE"] | None = None
    findings: str | None = Field(None, max_length=10000)


class LinkAlertRequest(BaseModel):
    alert_uid: str


# Configuration changes use STRICT numbers/booleans: "40" or "yes" are rejected instead of being coerced
# (JSON integers are still accepted for float fields).
SFloat = Annotated[float, Strict()]
SBool = Annotated[bool, Strict()]


class RuleUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: SBool | None = None
    weight: SFloat | None = Field(None, ge=0, le=1)
    params: dict[str, SFloat] | None = None
    # optimistic concurrency: the rule's updated_at as loaded by the client (409 if it changed since)
    expected_updated_at: datetime | None = None


class RiskSettingsUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    medium_threshold: SFloat = Field(ge=1, le=99)
    high_threshold: SFloat = Field(ge=2, le=100)
    model_weight: SFloat = Field(ge=0, le=1)
    anomaly_weight: SFloat = Field(ge=0, le=1)
    anomaly_min_percentile: SFloat = Field(ge=0, lt=1)
    block_simulation_enabled: SBool


class SimulationStart(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Literal["normal", "fraud", "mixed", "high_volume", "custom"] = "mixed"
    tps: float = Field(10.0, gt=0, le=2000)
    duration_seconds: float | None = Field(300, gt=0, le=86400)
    max_events: int | None = Field(None, gt=0, le=10_000_000)
    customer_count: int = Field(20000, ge=10, le=500_000)
    suspicious_pct: float = Field(3.0, ge=0, le=100)
    avg_amount: float = Field(60.0, gt=0, le=100_000)
    scenarios: list[str] | None = None
    seed: int | None = Field(42, ge=0, le=2**31)
    population_seed: int = Field(1000, ge=0, le=2**31)
    label: str | None = Field(None, max_length=80)


class InjectRequest(BaseModel):
    scenario: str
    customer_id: str | None = Field(None, max_length=64)
    seed: int | None = 42
    population_seed: int = Field(1000, ge=0, le=2**31)
    customer_count: int = Field(20000, ge=10, le=500_000)


class SavedConfigCreate(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    config: SimulationStart


class ActivateRequest(BaseModel):
    reason: str = Field(min_length=5, max_length=500)


class RollbackRequest(BaseModel):
    kind: Literal["supervised", "anomaly"] = "supervised"
    reason: str = Field(min_length=5, max_length=500)


class TrainRequest(BaseModel):
    quick: bool = False


class SettingUpdate(BaseModel):
    value: dict[str, Any]
    expected_updated_at: datetime | None = None


# ----------------------------------------------------------------- serialisers
def user_out(u: User) -> dict[str, Any]:
    return {"id": u.id, "username": u.username, "full_name": u.full_name, "role": u.role.name,
            "is_active": u.is_active, "created_at": u.created_at, "last_login_at": u.last_login_at}


def prediction_out(p: Prediction | None) -> dict[str, Any] | None:
    if p is None:
        return None
    return {"model_version": p.model_version, "anomaly_version": p.anomaly_version, "model_score": p.model_score,
            "model_threshold": p.model_threshold, "anomaly_score": p.anomaly_score, "risk_score": p.risk_score,
            "risk_category": p.risk_category, "recommended_action": p.recommended_action,
            "detection_method": p.detection_method, "triggered_rules": p.triggered_rules, "components": p.components,
            "features": p.features, "contributions": p.contributions, "explanation_method": p.explanation_method,
            "explanation": p.explanation, "inference_ms": p.inference_ms, "engine_version": p.engine_version,
            "predicted_at": p.predicted_at}


def transaction_out(t: Transaction, with_prediction: bool = False) -> dict[str, Any]:
    d: dict[str, Any] = {
        "id": t.id, "event_id": t.event_id, "transaction_id": t.transaction_id, "customer_id": t.customer_id,
        "account_id": t.account_id, "merchant_id": t.merchant_id, "amount": float(t.amount), "currency": t.currency,
        "event_time": t.event_time, "processed_at": t.processed_at, "transaction_type": t.transaction_type,
        "merchant_category": t.merchant_category, "channel": t.channel, "device_id": t.device_id,
        "ip_address": t.ip_address, "city": t.city, "country": t.country, "lat": t.lat, "lon": t.lon,
        "location_simulated": t.location_simulated, "status": t.status, "synthetic": t.synthetic,
        "source": t.source, "profile": t.profile, "pipeline_latency_ms": t.pipeline_latency_ms,
        "sim_is_suspicious": t.sim_is_suspicious, "sim_scenario": t.sim_scenario,
    }
    p = t.prediction
    if p is not None:
        d.update(risk_score=p.risk_score, risk_category=p.risk_category, detection_method=p.detection_method,
                 recommended_action=p.recommended_action, model_score=p.model_score)
    if with_prediction:
        d["prediction"] = prediction_out(p)
    return d


def alert_out(a: Alert) -> dict[str, Any]:
    t = a.transaction
    return {"alert_uid": a.alert_uid, "id": a.id, "transaction_pk": a.transaction_id,
            "transaction_id": t.transaction_id if t else None, "customer_id": a.customer_id,
            "amount": float(t.amount) if t else None, "currency": t.currency if t else None,
            "merchant_category": t.merchant_category if t else None, "event_time": t.event_time if t else None,
            "risk_score": a.risk_score, "risk_category": a.risk_category, "detection_method": a.detection_method,
            "triggered_rules": a.triggered_rules, "model_version": a.model_version, "explanation": a.explanation,
            "status": a.status, "assigned_to": user_out(a.assigned_to) if a.assigned_to else None,
            "investigation_id": a.investigation_id, "created_at": a.created_at, "updated_at": a.updated_at}


def note_out(n: InvestigationNote) -> dict[str, Any]:
    return {"id": n.id, "body": n.body, "author": n.author.username, "created_at": n.created_at,
            "alert_id": n.alert_id, "investigation_id": n.investigation_id}


def audit_out(a: AuditLog) -> dict[str, Any]:
    return {"id": a.id, "actor": a.actor_name, "action": a.action, "entity_type": a.entity_type,
            "entity_id": a.entity_id, "field": a.field, "old_value": a.old_value, "new_value": a.new_value,
            "details": a.details, "created_at": a.created_at}


def investigation_out(i: Investigation, full: bool = False) -> dict[str, Any]:
    d = {"case_uid": i.case_uid, "id": i.id, "title": i.title, "customer_id": i.customer_id, "status": i.status,
         "priority": i.priority, "opened_by": i.opened_by.username,
         "assigned_to": user_out(i.assigned_to) if i.assigned_to else None,
         "final_classification": i.final_classification, "findings": i.findings, "created_at": i.created_at,
         "updated_at": i.updated_at, "closed_at": i.closed_at, "alert_count": len(i.alerts)}
    if full:
        d["alerts"] = [alert_out(a) for a in i.alerts]
    return d


def rule_out(r: RiskRule) -> dict[str, Any]:
    return {"code": r.code, "name": r.name, "description": r.description, "family": r.family, "weight": r.weight,
            "params": r.params, "enabled": r.enabled, "updated_at": r.updated_at}


def model_out(m: ModelVersion) -> dict[str, Any]:
    return {"id": m.id, "model_name": m.model_name, "version": m.version, "kind": m.kind, "algorithm": m.algorithm,
            "status": m.status, "feature_config_version": m.feature_config_version,
            "hyperparameters": m.hyperparameters, "metrics": m.metrics, "inference_latency": m.inference_latency,
            "threshold": m.threshold, "artifact_path": m.artifact_path, "trained_at": m.trained_at,
            "activated_at": m.activated_at, "experiment_id": m.experiment_id,
            "dataset": ({"name": m.dataset.name, "version": m.dataset.version, "source": m.dataset.source,
                         "rows": m.dataset.rows} if m.dataset else None)}


def iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None
