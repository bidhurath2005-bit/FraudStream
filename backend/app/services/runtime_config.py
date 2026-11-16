"""Risk rules / settings stored in the DB, plus the runtime config document shared with
the Spark job (via Redis key ``fraudstream:runtime_config`` and a JSON file fallback)."""
from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.entities import ModelVersion, RiskRule, SystemSetting
from app.risk_engine.engine import DEFAULT_RULES, RiskEngine, RiskSettings, RuleConfig

log = logging.getLogger("fraudstream.runtime_config")
REDIS_KEY = "fraudstream:runtime_config"

DEFAULT_SETTINGS: dict[str, dict[str, Any]] = {
    "risk": asdict(RiskSettings()),
    "drift": {"moderate": 0.10, "significant": 0.25, "window_rows": 2000, "min_rows": 200},
    "simulation_defaults": {"mode": "mixed", "tps": 10.0, "duration_seconds": 300, "customer_count": 20000,
                            "suspicious_pct": 3.0, "avg_amount": 60.0, "seed": 42},
    "preferences": {"live_feed_max_rows": 200, "default_page_size": 25},
}


def seed_defaults(db: Session) -> None:
    if db.scalar(select(RiskRule.id).limit(1)) is None:
        for r in DEFAULT_RULES:
            db.add(RiskRule(code=r.code, name=r.name, description=r.description, family=r.family,
                            weight=r.weight, params=dict(r.params), enabled=r.enabled))
    for k, v in DEFAULT_SETTINGS.items():
        if db.get(SystemSetting, k) is None:
            db.add(SystemSetting(key=k, value=v))
    db.commit()


def get_setting(db: Session, key: str) -> dict[str, Any]:
    row = db.get(SystemSetting, key)
    return dict(row.value) if row else dict(DEFAULT_SETTINGS.get(key, {}))


def load_rules(db: Session) -> list[RuleConfig]:
    return [RuleConfig(code=r.code, name=r.name, description=r.description, family=r.family, weight=r.weight,
                       params=dict(r.params), enabled=r.enabled)
            for r in db.scalars(select(RiskRule).order_by(RiskRule.id))]


def build_engine(db: Session) -> RiskEngine:
    return RiskEngine(load_rules(db), RiskSettings(**get_setting(db, "risk")))


def build_runtime_config(db: Session) -> dict[str, Any]:
    active = {mv.kind: {"model_name": mv.model_name, "version": mv.version}
              for mv in db.scalars(select(ModelVersion).where(ModelVersion.status == "ACTIVE"))}
    return {"rules": [asdict(r) for r in load_rules(db)], "settings": get_setting(db, "risk"),
            "active_models": active, "generated_at": datetime.now(timezone.utc).isoformat()}


def publish_runtime_config(cfg: dict[str, Any], artifacts_dir: Path, redis_client: Any | None) -> dict[str, str]:
    """Write the config for out-of-process consumers. Returns per-target status."""
    status: dict[str, str] = {}
    try:
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        tmp = artifacts_dir / "runtime_config.json.tmp"
        tmp.write_text(json.dumps(cfg, indent=2))
        tmp.replace(artifacts_dir / "runtime_config.json")
        status["file"] = "ok"
    except OSError as exc:
        status["file"] = f"error: {exc}"
    if redis_client is not None:
        try:
            redis_client.set(REDIS_KEY, json.dumps(cfg))
            status["redis"] = "ok"
        except Exception as exc:  # noqa: BLE001
            status["redis"] = f"error: {exc.__class__.__name__}"
            log.warning("could not publish runtime config to redis: %s", exc)
    return status
