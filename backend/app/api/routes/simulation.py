from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query, Response, status
from sqlalchemy import select

from app.api.params import IdPath
from app.core.runtime import get_runtime
from app.models.entities import SimulationConfigRecord, SimulationRun
from app.schemas.api import InjectRequest, SavedConfigCreate, SimulationStart
from app.security.auth import DB, CurrentUser
from app.services import audit
from simulator.generator import SCENARIOS, SimulationConfig
from streaming.kafka.client import KafkaUnavailableError

router = APIRouter(prefix="/simulation", tags=["simulation"])


def _cfg(body: SimulationStart) -> SimulationConfig:
    try:
        return SimulationConfig(mode=body.mode, tps=body.tps, duration_seconds=body.duration_seconds,
                                max_events=body.max_events, customer_count=body.customer_count,
                                suspicious_pct=body.suspicious_pct, avg_amount=body.avg_amount,
                                scenarios=body.scenarios, seed=body.seed, population_seed=body.population_seed)
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc


def _start(body: SimulationStart, user: Any, db: Any) -> dict[str, Any]:
    rt = get_runtime()
    try:
        st = rt.simulation.start(_cfg(body), user, body.label)
    except KafkaUnavailableError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    audit.record(db, user, "SIMULATION_STARTED", "simulation_run", st["run_uid"], details=st["config"])
    db.commit()
    return st


@router.get("/scenarios")
def scenarios(user: CurrentUser) -> dict[str, Any]:
    return {"scenarios": list(SCENARIOS), "modes": ["normal", "fraud", "mixed", "high_volume", "custom"],
            "note": "All generated transactions are synthetic."}


@router.get("/status")
def sim_status(user: CurrentUser) -> dict[str, Any]:
    rt = get_runtime()
    st = rt.simulation.status()
    st["model_ready"] = rt.scorer.ready
    if rt.pipeline is not None:
        st["pipeline"] = dict(rt.pipeline.counters)
    return st


@router.post("/start", status_code=201)
def start(body: SimulationStart, user: CurrentUser, db: DB) -> dict[str, Any]:
    return _start(body, user, db)


@router.post("/pause")
def pause(user: CurrentUser) -> dict[str, Any]:
    try:
        return get_runtime().simulation.pause()
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc


@router.post("/resume")
def resume(user: CurrentUser) -> dict[str, Any]:
    try:
        return get_runtime().simulation.resume()
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc


@router.post("/stop")
def stop(user: CurrentUser, db: DB) -> dict[str, Any]:
    try:
        st = get_runtime().simulation.stop()
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    audit.record(db, user, "SIMULATION_STOPPED", "simulation_run", st.get("run_uid", "?"))
    db.commit()
    return st


@router.post("/inject")
def inject(body: InjectRequest, user: CurrentUser, db: DB) -> dict[str, Any]:
    if body.scenario not in SCENARIOS:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, f"unknown scenario; choose from {list(SCENARIOS)}")
    try:
        events = get_runtime().simulation.inject(body.scenario, body.customer_id, body.seed, body.customer_count,
                                                  body.population_seed)
    except KafkaUnavailableError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    audit.record(db, user, "SCENARIO_INJECTED", "simulation", body.scenario,
                 details={"events": len(events), "customer_id": body.customer_id})
    db.commit()
    return {"scenario": body.scenario, "events": events, "synthetic": True}


@router.get("/runs")
def runs(user: CurrentUser, db: DB, limit: Annotated[int, Query(ge=1)] = 20) -> list[dict[str, Any]]:
    rows = db.scalars(select(SimulationRun).order_by(SimulationRun.started_at.desc()).limit(min(limit, 100)))
    return [{"id": r.id, "run_uid": r.run_uid, "config": r.config, "profile": r.profile, "status": r.status,
             "started_at": r.started_at, "ended_at": r.ended_at, "events_generated": r.events_generated,
             "events_suspicious": r.events_suspicious, "error": r.error} for r in rows]


@router.post("/runs/{run_id}/replay", status_code=201)
def replay_run(run_id: IdPath, user: CurrentUser, db: DB) -> dict[str, Any]:
    """Re-run a previous configuration with the same seed: identical behavioural pattern,
    fresh event ids and current timestamps (so it is processed as new traffic)."""
    run = db.get(SimulationRun, run_id)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    cfg = {k: v for k, v in run.config.items() if k in SimulationStart.model_fields}
    return _start(SimulationStart(**cfg, label=f"replay of run {run_id}"), user, db)


@router.get("/configs")
def list_configs(user: CurrentUser, db: DB) -> list[dict[str, Any]]:
    return [{"id": c.id, "name": c.name, "config": c.config, "created_at": c.created_at}
            for c in db.scalars(select(SimulationConfigRecord).order_by(SimulationConfigRecord.name))]


@router.post("/configs", status_code=201)
def save_config(body: SavedConfigCreate, user: CurrentUser, db: DB) -> dict[str, Any]:
    if db.scalar(select(SimulationConfigRecord.id).where(SimulationConfigRecord.name == body.name)):
        raise HTTPException(status.HTTP_409_CONFLICT, "a configuration with this name exists")
    _cfg(body.config)  # validate
    rec = SimulationConfigRecord(name=body.name, config=body.config.model_dump(), created_by_id=user.id)
    db.add(rec)
    db.commit()
    return {"id": rec.id, "name": rec.name, "config": rec.config}


@router.post("/configs/{config_id}/run", status_code=201)
def run_config(config_id: IdPath, user: CurrentUser, db: DB) -> dict[str, Any]:
    rec = db.get(SimulationConfigRecord, config_id)
    if rec is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "configuration not found")
    body = SimulationStart(**{**rec.config, "label": rec.name})
    return _start(body, user, db)


@router.delete("/configs/{config_id}", status_code=204, response_model=None)
def delete_config(config_id: IdPath, user: CurrentUser, db: DB) -> Response:
    rec = db.get(SimulationConfigRecord, config_id)
    if rec is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "configuration not found")
    db.delete(rec)
    db.commit()
    return Response(status_code=204)
