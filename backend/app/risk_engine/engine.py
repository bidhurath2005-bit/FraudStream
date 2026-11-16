"""Hybrid risk scoring: ML score + anomaly score + configurable rules.

Scoring method (documented in docs/DATA_AND_ML.md section 7):

1. Model component  c_m = w_model * n(s), where n() maps the uncalibrated model score s
   piecewise-linearly so the model's validation-selected threshold maps to 0.5
   (n(thr)=0.5, n(1)=1, n(0)=0). The raw score is NOT treated as a probability.
2. Anomaly component c_a = w_anomaly * max(0, (p - p_min) / (1 - p_min)) where p is the
   isolation-forest training percentile (only extreme anomalies contribute).
3. Rule components: rules are grouped into signal *families* (amount, velocity, device,
   location, failures, timing, merchant). Only the strongest triggered rule per family
   counts, to avoid double-counting correlated signals.
4. Combination (noisy-OR):  risk = 1 - PROD_i (1 - c_i);  risk_score = 100 * risk.
   This is a bounded, monotone heuristic — not a probability of fraud.

Categories: LOW < medium_threshold <= MEDIUM < high_threshold <= HIGH.
Actions: LOW->ALLOW, MEDIUM->REVIEW, HIGH->BLOCK_SIMULATION (only if enabled, else REVIEW).
BLOCK_SIMULATION affects the educational simulation only; nothing real is ever blocked.

This module is intentionally free of database/web imports so the Spark job can reuse it.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

RISK_ENGINE_VERSION = "re-1.0"


@dataclass
class RuleConfig:
    code: str
    name: str
    description: str
    family: str
    weight: float
    params: dict[str, float] = field(default_factory=dict)
    enabled: bool = True


@dataclass
class RiskSettings:
    medium_threshold: float = 40.0
    high_threshold: float = 70.0
    model_weight: float = 0.90
    anomaly_weight: float = 0.30
    anomaly_min_percentile: float = 0.97
    block_simulation_enabled: bool = True

    def validate(self) -> None:
        if not 0 < self.medium_threshold < self.high_threshold <= 100:
            raise ValueError("require 0 < medium_threshold < high_threshold <= 100")
        for n in ("model_weight", "anomaly_weight"):
            if not 0 <= getattr(self, n) <= 1:
                raise ValueError(f"{n} must be in [0, 1]")
        if not 0 <= self.anomaly_min_percentile < 1:
            raise ValueError("anomaly_min_percentile must be in [0, 1)")


DEFAULT_RULES: tuple[RuleConfig, ...] = (
    RuleConfig("LARGE_AMOUNT", "Unusually large transaction",
               "Amount >= absolute limit, or >= ratio x the customer's historical mean (with enough history).",
               "amount", 0.35, {"absolute": 5000.0, "ratio": 8.0, "min_history": 3}),
    RuleConfig("AMOUNT_DEVIATION", "Strong spending deviation",
               "Amount z-score vs customer's history >= threshold.", "amount", 0.30,
               {"zscore": 4.0, "min_history": 5}),
    RuleConfig("HIGH_VELOCITY_1M", "Excessive 1-minute velocity",
               "Prior transactions in the last 60 s >= count.", "velocity", 0.35, {"count": 4}),
    RuleConfig("HIGH_VELOCITY_5M", "Excessive 5-minute velocity",
               "Prior transactions in the last 5 min >= count.", "velocity", 0.30, {"count": 8}),
    RuleConfig("NEW_DEVICE_HIGH_SPEND", "New device with unusual spending",
               "Unseen device and amount >= ratio x historical mean.", "device", 0.35, {"ratio": 3.0}),
    RuleConfig("IMPOSSIBLE_TRAVEL", "Implausible travel speed",
               "Distance from previous transaction implies speed >= kmh.", "location", 0.45, {"kmh": 900.0}),
    RuleConfig("NEW_LOCATION", "New location", "City never seen for this customer.", "location", 0.10, {}),
    RuleConfig("REPEATED_FAILURES", "Repeated failed transactions",
               "Prior FAILED/DECLINED in the last hour >= count.", "failures", 0.35, {"count": 3}),
    RuleConfig("ODD_HOUR_HIGH_SPEND", "Night-time unusual spending",
               "Between 00:00-05:59 UTC and amount >= ratio x historical mean.", "timing", 0.15, {"ratio": 3.0}),
    RuleConfig("HIGH_RISK_MERCHANT_NEW", "New high-risk merchant",
               "First transaction with a merchant in a high-risk category.", "merchant", 0.20, {}),
)


@dataclass
class TriggeredRule:
    code: str
    name: str
    family: str
    weight: float
    detail: str


Check = Callable[[Mapping[str, Any], dict[str, float]], str | None]


def _large_amount(f: Mapping[str, Any], p: dict[str, float]) -> str | None:
    if f["amount"] >= p["absolute"]:
        return f"amount {f['amount']:.2f} >= {p['absolute']:.0f}"
    if f["hist_tx_count"] >= p["min_history"] and f["amount_to_mean_ratio"] >= p["ratio"]:
        return f"amount is {f['amount_to_mean_ratio']:.1f}x customer mean ({f['hist_mean_amount']:.2f})"
    return None


CHECKS: dict[str, Check] = {
    "LARGE_AMOUNT": _large_amount,
    "AMOUNT_DEVIATION": lambda f, p: (f"z-score {f['amount_zscore']:.1f} >= {p['zscore']}"
                                      if f["hist_tx_count"] >= p["min_history"] and f["amount_zscore"] >= p["zscore"] else None),
    "HIGH_VELOCITY_1M": lambda f, p: (f"{int(f['tx_count_1m'])} prior transactions in 60s"
                                      if f["tx_count_1m"] >= p["count"] else None),
    "HIGH_VELOCITY_5M": lambda f, p: (f"{int(f['tx_count_5m'])} prior transactions in 5 min"
                                      if f["tx_count_5m"] >= p["count"] else None),
    "NEW_DEVICE_HIGH_SPEND": lambda f, p: (f"new device, amount {f['amount_to_mean_ratio']:.1f}x mean"
                                           if f["is_new_device"] and f["amount_to_mean_ratio"] >= p["ratio"] else None),
    "IMPOSSIBLE_TRAVEL": lambda f, p: (f"implied speed {f['geo_velocity_kmh']:.0f} km/h"
                                       if f["geo_velocity_kmh"] >= p["kmh"] else None),
    "NEW_LOCATION": lambda f, p: "first transaction from this city" if f["is_new_location"] else None,
    "REPEATED_FAILURES": lambda f, p: (f"{int(f['failed_count_1h'])} failed/declined in the last hour"
                                       if f["failed_count_1h"] >= p["count"] else None),
    "ODD_HOUR_HIGH_SPEND": lambda f, p: (f"{int(f['hour_of_day']):02d}:00 UTC, {f['amount_to_mean_ratio']:.1f}x mean"
                                         if f["is_night"] and f["has_history"] and f["amount_to_mean_ratio"] >= p["ratio"] else None),
    "HIGH_RISK_MERCHANT_NEW": lambda f, p: (f"first {f['merchant_category']} merchant"
                                            if f["is_high_risk_category"] and f["is_new_merchant"] else None),
}


@dataclass
class RiskDecision:
    risk_score: float
    risk_category: str
    recommended_action: str
    detection_method: str
    triggered_rules: list[dict[str, Any]]
    components: dict[str, float]
    model_score: float | None
    model_threshold: float | None
    model_version: str | None
    anomaly_score: float | None
    relevant_features: dict[str, Any]
    explanation: str
    predicted_at: str
    engine_version: str = RISK_ENGINE_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


RELEVANT_FEATURES = ("amount", "amount_to_mean_ratio", "amount_zscore", "tx_count_1m", "tx_count_5m",
                     "failed_count_1h", "is_new_device", "is_new_location", "geo_velocity_kmh",
                     "seconds_since_prev", "merchant_category", "hist_tx_count")


def normalise_model_score(score: float, threshold: float) -> float:
    thr = min(max(threshold, 1e-6), 1 - 1e-6)
    if score < thr:
        return 0.5 * score / thr
    return 0.5 + 0.5 * (score - thr) / (1 - thr)


class RiskEngine:
    def __init__(self, rules: list[RuleConfig] | None = None, settings: RiskSettings | None = None) -> None:
        self.rules = list(rules) if rules is not None else [RuleConfig(**asdict(r)) for r in DEFAULT_RULES]
        self.settings = settings or RiskSettings()
        self.settings.validate()
        unknown = [r.code for r in self.rules if r.code not in CHECKS]
        if unknown:
            raise ValueError(f"no implementation for rules {unknown}")

    def evaluate(self, features: Mapping[str, Any], *, model_score: float | None, model_threshold: float | None,
                 model_version: str | None, anomaly_score: float | None = None,
                 top_contributions: list[dict[str, float]] | None = None,
                 now: datetime | None = None) -> RiskDecision:
        st = self.settings
        triggered: list[TriggeredRule] = []
        for r in self.rules:
            if not r.enabled:
                continue
            detail = CHECKS[r.code](features, r.params)
            if detail:
                triggered.append(TriggeredRule(r.code, r.name, r.family, r.weight, detail))
        family_max: dict[str, float] = {}
        for t in triggered:
            family_max[t.family] = max(family_max.get(t.family, 0.0), t.weight)

        comps: dict[str, float] = {}
        if model_score is not None and model_threshold is not None:
            comps["model"] = st.model_weight * normalise_model_score(model_score, model_threshold)
        if anomaly_score is not None and anomaly_score > st.anomaly_min_percentile:
            comps["anomaly"] = st.anomaly_weight * (anomaly_score - st.anomaly_min_percentile) / (1 - st.anomaly_min_percentile)
        for fam, w in family_max.items():
            comps[f"rules:{fam}"] = w
        keep = 1.0
        for c in comps.values():
            keep *= 1 - min(max(c, 0.0), 1.0)
        score = round(100 * (1 - keep), 2)

        category = "HIGH" if score >= st.high_threshold else "MEDIUM" if score >= st.medium_threshold else "LOW"
        if category == "HIGH":
            action = "BLOCK_SIMULATION" if st.block_simulation_enabled else "REVIEW"
        elif category == "MEDIUM":
            action = "REVIEW"
        else:
            action = "ALLOW"

        sources = []
        if model_score is not None and model_threshold is not None and model_score >= model_threshold:
            sources.append("ML")
        if "anomaly" in comps:
            sources.append("ANOMALY")
        if triggered:
            sources.append("RULES")
        method = "HYBRID" if len(sources) > 1 else (sources[0] if sources else "NONE")

        parts = [f"Risk {score:.1f}/100 ({category})."]
        if model_score is not None:
            rel = "above" if model_threshold is not None and model_score >= model_threshold else "below"
            parts.append(f"Model {model_version} score {model_score:.3f} is {rel} its threshold {model_threshold:.3f} "
                         f"(uncalibrated score, not a probability).")
        else:
            parts.append("No ML score available; rules only.")
        if "anomaly" in comps:
            parts.append(f"Anomaly detector: more unusual than {anomaly_score * 100:.1f}% of training events.")
        if triggered:
            parts.append("Rules: " + "; ".join(f"{t.code} ({t.detail})" for t in triggered) + ".")
        if top_contributions:
            parts.append("Top model factors: " + ", ".join(
                f"{c['feature']} ({c['contribution']:+.3f})" for c in top_contributions[:4]) + ".")
        return RiskDecision(
            risk_score=score, risk_category=category, recommended_action=action, detection_method=method,
            triggered_rules=[asdict(t) for t in triggered], components={k: round(v, 4) for k, v in comps.items()},
            model_score=model_score, model_threshold=model_threshold, model_version=model_version,
            anomaly_score=anomaly_score,
            relevant_features={k: features[k] for k in RELEVANT_FEATURES if k in features},
            explanation=" ".join(parts), predicted_at=(now or datetime.now(timezone.utc)).isoformat(),
        )


def engine_from_config(cfg: Mapping[str, Any] | None) -> RiskEngine:
    """Build an engine from a JSON-able runtime config {"rules": [...], "settings": {...}}."""
    if not cfg:
        return RiskEngine()
    rules = [RuleConfig(**r) for r in cfg.get("rules", [])] or None
    settings = RiskSettings(**cfg.get("settings", {})) if cfg.get("settings") else None
    return RiskEngine(rules, settings)
