"""The detection step shared by BOTH execution profiles:
features -> ML inference (+ anomaly) -> risk engine -> 'scored message'.

The scored message is the payload published to ``transactions.normal`` /
``transactions.suspicious`` by Spark and consumed by the persistence consumer; the local
pipeline builds exactly the same structure in-process.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.risk_engine.engine import RiskEngine
from ml.inference.scorer import ModelUnavailableError, Scorer, ScoreResult


def detect(event_wire: dict[str, Any], features: dict[str, Any], scorer: Scorer | None, engine: RiskEngine,
           *, ingested_at: datetime | None, explain: bool = True,
           precomputed: ScoreResult | None = None) -> dict[str, Any]:
    score: ScoreResult | None = precomputed
    model_error: str | None = None
    if score is None:
        try:
            if scorer is None:
                raise ModelUnavailableError("no scorer configured")
            score = scorer.score(features, explain)
        except ModelUnavailableError as exc:
            model_error = str(exc)  # explicit: rules-only decision, flagged in the message
    decision = engine.evaluate(
        features,
        model_score=score.model_score if score else None,
        model_threshold=score.model_threshold if score else None,
        model_version=score.model_version if score else None,
        anomaly_score=score.anomaly_score if score else None,
        top_contributions=score.top_contributions if score else None,
    )
    return {
        "event": event_wire,
        "features": features,
        "decision": decision.to_dict(),
        "inference": {
            "inference_ms": score.inference_ms if score else None,
            "anomaly_version": score.anomaly_version if score else None,
            "contributions": score.top_contributions if score else [],
            "explanation_method": (score.explanation or {}).get("method") if score else None,
        },
        "model_error": model_error,
        "ingested_at": ingested_at.isoformat() if ingested_at else None,
    }


def is_suspicious(scored: dict[str, Any]) -> bool:
    return scored["decision"]["risk_category"] in ("MEDIUM", "HIGH")
