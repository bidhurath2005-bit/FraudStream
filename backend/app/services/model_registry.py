"""Model registry: syncs file artifacts (ml/artifacts) into the DB, controlled activation,
rollback, and background training jobs. A model is only ever activated by an explicit
admin action, except for the very first bootstrap when no model is active at all."""
from __future__ import annotations

import json
import logging
import threading
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.entities import DatasetMetadata, ModelExperiment, ModelVersion, User
from app.services import audit

log = logging.getLogger("fraudstream.registry")


def _dt(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def _dataset(db: Session, meta: dict[str, Any]) -> DatasetMetadata | None:
    if not meta or not meta.get("name"):
        return None
    ver = str(meta.get("version") or "unknown")
    ds = db.scalar(select(DatasetMetadata).where(DatasetMetadata.name == meta["name"], DatasetMetadata.version == ver))
    if ds is None:
        ds = DatasetMetadata(name=meta["name"], version=ver, source=meta.get("source", "unknown"),
                             provenance=meta.get("provenance", ""), rows=int(meta.get("rows") or 0),
                             positive_rate=meta.get("positive_rate"), content_sha256=meta.get("content_sha256"),
                             profile=meta.get("profile", {"spec": meta.get("spec")}), path=meta.get("path"))
        db.add(ds)
        db.flush()
    return ds


def sync_registry(db: Session, artifacts_dir: Path) -> int:
    """Import experiments/model versions found on disk that are not yet in the DB."""
    added = 0
    exp_dir = artifacts_dir / "experiments"
    exps: dict[str, ModelExperiment] = {}
    for p in sorted(exp_dir.glob("*.json")) if exp_dir.exists() else []:
        summary = json.loads(p.read_text())
        ev = summary["experiment_version"]
        exp = db.scalar(select(ModelExperiment).where(ModelExperiment.experiment_version == ev))
        if exp is None:
            ds = _dataset(db, summary.get("dataset", {}))
            exp = ModelExperiment(experiment_version=ev, dataset_id=ds.id if ds else None, status="COMPLETED",
                                  triggered_by=summary.get("triggered_by", "cli"),
                                  started_at=_dt(summary["started_at"]), finished_at=_dt(summary.get("finished_at")),
                                  summary=summary)
            db.add(exp)
            db.flush()
        exps[ev] = exp
    models_dir = artifacts_dir / "models"
    for meta_path in sorted(models_dir.glob("*/*/metadata.json")) if models_dir.exists() else []:
        meta = json.loads(meta_path.read_text())
        exists = db.scalar(select(ModelVersion.id).where(ModelVersion.model_name == meta["model_name"],
                                                         ModelVersion.version == meta["version"]))
        if exists:
            continue
        exp = exps.get(meta["version"])
        db.add(ModelVersion(
            model_name=meta["model_name"], version=meta["version"], kind=meta["kind"], algorithm=meta["algorithm"],
            experiment_id=exp.id if exp else None, dataset_id=exp.dataset_id if exp else None,
            feature_config_version=meta["feature_config_version"], hyperparameters=meta["hyperparameters"],
            metrics=meta["metrics"], inference_latency=meta.get("inference_latency", {}),
            threshold=float(meta["threshold"]), artifact_path=meta["artifact_path"], status="CANDIDATE",
            trained_at=_dt(meta["trained_at"]) or datetime.now(timezone.utc)))
        added += 1
    db.flush()
    _bootstrap_active(db, artifacts_dir)
    db.commit()
    return added


def _bootstrap_active(db: Session, artifacts_dir: Path) -> None:
    reg_path = artifacts_dir / "registry.json"
    if not reg_path.exists():
        return
    reg = json.loads(reg_path.read_text())
    for kind in ("supervised", "anomaly"):
        if db.scalar(select(ModelVersion.id).where(ModelVersion.kind == kind, ModelVersion.status == "ACTIVE")):
            continue
        ref = reg.get("active", {}).get(kind)
        if not ref:
            continue
        mv = db.scalar(select(ModelVersion).where(ModelVersion.model_name == ref["model_name"],
                                                  ModelVersion.version == ref["version"]))
        if mv:
            mv.status, mv.activated_at = "ACTIVE", datetime.now(timezone.utc)
            audit.record(db, None, "MODEL_BOOTSTRAP_ACTIVATED", "model_version", f"{mv.model_name}:{mv.version}",
                         field="status", old="CANDIDATE", new="ACTIVE")


def active_models(db: Session) -> dict[str, ModelVersion]:
    return {mv.kind: mv for mv in db.scalars(select(ModelVersion).where(ModelVersion.status == "ACTIVE"))}


def write_registry_active(db: Session, artifacts_dir: Path) -> None:
    reg_path = artifacts_dir / "registry.json"
    reg = json.loads(reg_path.read_text()) if reg_path.exists() else {"models": [], "active": {}}
    reg["active"] = {k: {"model_name": v.model_name, "version": v.version} for k, v in active_models(db).items()}
    reg_path.write_text(json.dumps(reg, indent=2))


def activate(db: Session, mv: ModelVersion, actor: User, reason: str) -> ModelVersion | None:
    """Controlled replacement: the previously active model of the same kind becomes a
    CANDIDATE again (kept for rollback). Returns the previously active version."""
    previous = db.scalar(select(ModelVersion).where(ModelVersion.kind == mv.kind, ModelVersion.status == "ACTIVE"))
    if previous and previous.id == mv.id:
        return None
    if previous:
        previous.status = "CANDIDATE"
        audit.record(db, actor, "MODEL_DEACTIVATED", "model_version", f"{previous.model_name}:{previous.version}",
                     field="status", old="ACTIVE", new="CANDIDATE", details={"reason": reason})
    old = mv.status
    mv.status, mv.activated_at = "ACTIVE", datetime.now(timezone.utc)
    audit.record(db, actor, "MODEL_ACTIVATED", "model_version", f"{mv.model_name}:{mv.version}", field="status",
                 old=old, new="ACTIVE", details={"reason": reason,
                                                 "replaced": f"{previous.model_name}:{previous.version}" if previous else None})
    return previous


def rollback_target(db: Session, kind: str) -> ModelVersion | None:
    current = db.scalar(select(ModelVersion).where(ModelVersion.kind == kind, ModelVersion.status == "ACTIVE"))
    q = select(ModelVersion).where(ModelVersion.kind == kind, ModelVersion.activated_at.is_not(None))
    if current:
        q = q.where(ModelVersion.id != current.id)
    return db.scalar(q.order_by(ModelVersion.activated_at.desc()).limit(1))


class TrainingJobManager:
    """Runs ml.training.train in a background thread (one job at a time)."""

    def __init__(self, artifacts_dir: Path, session_factory: Any) -> None:
        self.artifacts_dir = artifacts_dir
        self.session_factory = session_factory
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self.state: dict[str, Any] = {"status": "IDLE"}

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self, actor: str, quick: bool) -> dict[str, Any]:
        with self._lock:
            if self.running:
                raise RuntimeError("a training job is already running")
            self.state = {"status": "RUNNING", "started_at": datetime.now(timezone.utc).isoformat(),
                          "triggered_by": actor, "quick": quick}
            self._thread = threading.Thread(target=self._run, args=(actor, quick), daemon=True, name="training-job")
            self._thread.start()
            return dict(self.state)

    def _run(self, actor: str, quick: bool) -> None:
        from ml.training.train import run

        try:
            summary = run("synthetic", quick=quick, artifacts_dir=self.artifacts_dir)
            summary["triggered_by"] = actor
            (self.artifacts_dir / "experiments" / f"{summary['experiment_version']}.json").write_text(
                json.dumps(summary, indent=2, default=str))
            db = self.session_factory()
            try:
                added = sync_registry(db, self.artifacts_dir)
            finally:
                db.close()
            self.state.update(status="COMPLETED", finished_at=datetime.now(timezone.utc).isoformat(),
                              experiment_version=summary["experiment_version"], models_added=added,
                              recommended=summary["recommended_supervised"])
        except Exception as exc:  # noqa: BLE001
            log.exception("training job failed")
            self.state.update(status="FAILED", error=f"{exc.__class__.__name__}: {exc}",
                              traceback=traceback.format_exc()[-2000:],
                              finished_at=datetime.now(timezone.utc).isoformat())
