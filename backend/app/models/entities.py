"""ORM models. All timestamps are stored and returned as UTC (tz-aware)."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database.session import Base

JSONType = JSON().with_variant(JSONB(), "postgresql")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class UTCDateTime(TypeDecorator[datetime]):
    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Any) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("naive datetime not allowed; use UTC-aware datetimes")
        return value.astimezone(timezone.utc)

    def process_result_value(self, value: datetime | None, dialect: Any) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


# --------------------------------------------------------------------------- auth
class Role(Base):
    __tablename__ = "roles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(32), unique=True)
    description: Mapped[str] = mapped_column(String(200), default="")


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    full_name: Mapped[str] = mapped_column(String(120), default="")
    password_hash: Mapped[str] = mapped_column(String(255))
    role_id: Mapped[int] = mapped_column(ForeignKey("roles.id"))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    last_login_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    role: Mapped[Role] = relationship(lazy="joined")


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime)
    revoked_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)


# --------------------------------------------------------------------------- reference data
class Customer(Base):
    __tablename__ = "customers"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    home_city: Mapped[str | None] = mapped_column(String(80), nullable=True)
    synthetic: Mapped[bool] = mapped_column(Boolean, default=True)
    first_seen_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)


class Merchant(Base):
    __tablename__ = "merchants"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(120), default="")
    category: Mapped[str] = mapped_column(String(40), index=True)


# --------------------------------------------------------------------------- transactions
class Transaction(Base):
    __tablename__ = "transactions"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_tx_amount_positive"),
        Index("ix_tx_customer_time", "customer_id", "event_time"),
        Index("ix_tx_processed", "processed_at"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), unique=True)
    transaction_id: Mapped[str] = mapped_column(String(64), unique=True)
    customer_id: Mapped[str] = mapped_column(ForeignKey("customers.id"), index=True)
    account_id: Mapped[str] = mapped_column(String(64))
    merchant_id: Mapped[str] = mapped_column(ForeignKey("merchants.id"), index=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    currency: Mapped[str] = mapped_column(String(3))
    event_time: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    ingested_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    processed_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    transaction_type: Mapped[str] = mapped_column(String(20))
    merchant_category: Mapped[str] = mapped_column(String(40), index=True)
    channel: Mapped[str] = mapped_column(String(20))
    device_id: Mapped[str] = mapped_column(String(64))
    ip_address: Mapped[str] = mapped_column(String(45))
    city: Mapped[str] = mapped_column(String(80))
    country: Mapped[str] = mapped_column(String(2))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    location_simulated: Mapped[bool] = mapped_column(Boolean, default=True)
    status: Mapped[str] = mapped_column(String(12))
    schema_version: Mapped[str] = mapped_column(String(8))
    synthetic: Mapped[bool] = mapped_column(Boolean, default=True)
    source: Mapped[str] = mapped_column(String(32), default="simulator")
    profile: Mapped[str] = mapped_column(String(16))
    # Simulator ground truth: for EVALUATION ONLY (never used as a model feature)
    sim_is_suspicious: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    sim_scenario: Mapped[str | None] = mapped_column(String(64), nullable=True)
    pipeline_latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    prediction: Mapped[Prediction | None] = relationship(back_populates="transaction", uselist=False)


class Prediction(Base):
    __tablename__ = "predictions"
    __table_args__ = (Index("ix_pred_category_time", "risk_category", "predicted_at"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.id", ondelete="CASCADE"), unique=True)
    model_version: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    anomaly_version: Mapped[str | None] = mapped_column(String(80), nullable=True)
    model_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    model_threshold: Mapped[float | None] = mapped_column(Float, nullable=True)
    anomaly_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    risk_score: Mapped[float] = mapped_column(Float, index=True)
    risk_category: Mapped[str] = mapped_column(String(8))
    recommended_action: Mapped[str] = mapped_column(String(20))
    detection_method: Mapped[str] = mapped_column(String(12))
    triggered_rules: Mapped[list[dict[str, Any]]] = mapped_column(JSONType, default=list)
    components: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)
    features: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)
    contributions: Mapped[list[dict[str, Any]]] = mapped_column(JSONType, default=list)
    explanation_method: Mapped[str | None] = mapped_column(String(40), nullable=True)
    explanation: Mapped[str] = mapped_column(Text, default="")
    inference_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    engine_version: Mapped[str] = mapped_column(String(16))
    predicted_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    transaction: Mapped[Transaction] = relationship(back_populates="prediction")


# --------------------------------------------------------------------------- alerts & investigations
ALERT_STATUSES = ("NEW", "UNDER_REVIEW", "CONFIRMED_SUSPICIOUS", "DISMISSED", "CLOSED")
INVESTIGATION_STATUSES = ("OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED")


class Alert(Base):
    __tablename__ = "alerts"
    __table_args__ = (
        CheckConstraint(f"status IN {ALERT_STATUSES}", name="ck_alert_status"),
        Index("ix_alert_status_created", "status", "created_at"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    alert_uid: Mapped[str] = mapped_column(String(36), unique=True)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.id", ondelete="CASCADE"), unique=True)
    customer_id: Mapped[str] = mapped_column(ForeignKey("customers.id"), index=True)
    risk_score: Mapped[float] = mapped_column(Float, index=True)
    risk_category: Mapped[str] = mapped_column(String(8), index=True)
    detection_method: Mapped[str] = mapped_column(String(12))
    triggered_rules: Mapped[list[dict[str, Any]]] = mapped_column(JSONType, default=list)
    model_version: Mapped[str | None] = mapped_column(String(80), nullable=True)
    explanation: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(24), default="NEW")
    assigned_to_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True, index=True)
    investigation_id: Mapped[int | None] = mapped_column(ForeignKey("investigations.id"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)
    transaction: Mapped[Transaction] = relationship(lazy="joined")
    assigned_to: Mapped[User | None] = relationship(lazy="joined")


class Investigation(Base):
    __tablename__ = "investigations"
    __table_args__ = (CheckConstraint(f"status IN {INVESTIGATION_STATUSES}", name="ck_inv_status"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    case_uid: Mapped[str] = mapped_column(String(36), unique=True)
    title: Mapped[str] = mapped_column(String(200))
    customer_id: Mapped[str] = mapped_column(ForeignKey("customers.id"), index=True)
    status: Mapped[str] = mapped_column(String(16), default="OPEN")
    priority: Mapped[str] = mapped_column(String(8), default="MEDIUM")
    opened_by_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    assigned_to_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    final_classification: Mapped[str | None] = mapped_column(String(32), nullable=True)
    findings: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)
    closed_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    opened_by: Mapped[User] = relationship(foreign_keys=[opened_by_id], lazy="joined")
    assigned_to: Mapped[User | None] = relationship(foreign_keys=[assigned_to_id], lazy="joined")
    alerts: Mapped[list[Alert]] = relationship(foreign_keys=[Alert.investigation_id], lazy="selectin")


class InvestigationNote(Base):
    __tablename__ = "investigation_notes"
    __table_args__ = (CheckConstraint("investigation_id IS NOT NULL OR alert_id IS NOT NULL", name="ck_note_target"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    investigation_id: Mapped[int | None] = mapped_column(ForeignKey("investigations.id", ondelete="CASCADE"),
                                                        nullable=True, index=True)
    alert_id: Mapped[int | None] = mapped_column(ForeignKey("alerts.id", ondelete="CASCADE"), nullable=True, index=True)
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    body: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    author: Mapped[User] = relationship(lazy="joined")


class AuditLog(Base):
    __tablename__ = "audit_logs"
    __table_args__ = (Index("ix_audit_entity", "entity_type", "entity_id"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    actor_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    actor_name: Mapped[str] = mapped_column(String(64))
    action: Mapped[str] = mapped_column(String(64))
    entity_type: Mapped[str] = mapped_column(String(32))
    entity_id: Mapped[str] = mapped_column(String(64))
    field: Mapped[str | None] = mapped_column(String(64), nullable=True)
    old_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    details: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, index=True)


# --------------------------------------------------------------------------- configuration
class RiskRule(Base):
    __tablename__ = "risk_rules"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(40), unique=True)
    name: Mapped[str] = mapped_column(String(120))
    description: Mapped[str] = mapped_column(Text, default="")
    family: Mapped[str] = mapped_column(String(24))
    weight: Mapped[float] = mapped_column(Float)
    params: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)


class SystemSetting(Base):
    __tablename__ = "system_settings"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[dict[str, Any]] = mapped_column(JSONType)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)


# --------------------------------------------------------------------------- ML
class DatasetMetadata(Base):
    __tablename__ = "datasets"
    __table_args__ = (UniqueConstraint("name", "version", name="uq_dataset_name_version"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(80))
    version: Mapped[str] = mapped_column(String(32))
    source: Mapped[str] = mapped_column(String(32))
    provenance: Mapped[str] = mapped_column(Text, default="")
    rows: Mapped[int] = mapped_column(Integer, default=0)
    positive_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    content_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    profile: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)
    path: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)


class ModelExperiment(Base):
    __tablename__ = "model_experiments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_version: Mapped[str] = mapped_column(String(40), unique=True)
    dataset_id: Mapped[int | None] = mapped_column(ForeignKey("datasets.id"), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="COMPLETED")
    triggered_by: Mapped[str] = mapped_column(String(64), default="cli")
    started_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    summary: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class ModelVersion(Base):
    __tablename__ = "model_versions"
    __table_args__ = (UniqueConstraint("model_name", "version", name="uq_model_name_version"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_name: Mapped[str] = mapped_column(String(40))
    version: Mapped[str] = mapped_column(String(40))
    kind: Mapped[str] = mapped_column(String(16))
    algorithm: Mapped[str] = mapped_column(String(40))
    experiment_id: Mapped[int | None] = mapped_column(ForeignKey("model_experiments.id"), nullable=True)
    dataset_id: Mapped[int | None] = mapped_column(ForeignKey("datasets.id"), nullable=True)
    feature_config_version: Mapped[str] = mapped_column(String(16))
    hyperparameters: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)
    inference_latency: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)
    threshold: Mapped[float] = mapped_column(Float)
    artifact_path: Mapped[str] = mapped_column(String(300))
    status: Mapped[str] = mapped_column(String(16), default="CANDIDATE")  # CANDIDATE | ACTIVE | ARCHIVED
    trained_at: Mapped[datetime] = mapped_column(UTCDateTime)
    activated_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    dataset: Mapped[DatasetMetadata | None] = relationship(lazy="joined")


class DriftReport(Base):
    __tablename__ = "drift_reports"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_version: Mapped[str] = mapped_column(String(80))
    computed_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, index=True)
    window_rows: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(24))
    max_psi: Mapped[float | None] = mapped_column(Float, nullable=True)
    report: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)


# --------------------------------------------------------------------------- simulation
class SimulationConfigRecord(Base):
    __tablename__ = "simulation_configs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(80), unique=True)
    config: Mapped[dict[str, Any]] = mapped_column(JSONType)
    created_by_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)


class SimulationRun(Base):
    __tablename__ = "simulation_runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_uid: Mapped[str] = mapped_column(String(36), unique=True)
    config: Mapped[dict[str, Any]] = mapped_column(JSONType)
    profile: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16))
    started_by_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    ended_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    events_generated: Mapped[int] = mapped_column(Integer, default=0)
    events_suspicious: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class DeadLetter(Base):
    """Events rejected by the LOCAL pipeline (malformed, late, processing failure).
    In the DISTRIBUTED profile rejected events go to the Kafka topic ``fraud.dlq``."""
    __tablename__ = "dead_letters"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[str] = mapped_column(String(32))
    reason: Mapped[str] = mapped_column(String(300))
    event_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, index=True)


# --------------------------------------------------------------------------- reports (Phase 11C)
class GeneratedReport(Base):
    """A report generated from persisted data. ``content`` is the exact snapshot that was shown
    when it was generated, so later downloads (CSV/PDF) reproduce it rather than re-querying."""
    __tablename__ = "generated_reports"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    report_uid: Mapped[str] = mapped_column(String(36), unique=True)
    report_type: Mapped[str] = mapped_column(String(32), index=True)
    title: Mapped[str] = mapped_column(String(200))
    period_start: Mapped[datetime] = mapped_column(UTCDateTime)
    period_end: Mapped[datetime] = mapped_column(UTCDateTime)
    filters: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)
    status: Mapped[str] = mapped_column(String(16))  # COMPLETED | FAILED
    error: Mapped[str | None] = mapped_column(String(300), nullable=True)
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    generated_by_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    generated_by_name: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, index=True)
    content: Mapped[dict[str, Any] | None] = mapped_column(JSONType, nullable=True)
