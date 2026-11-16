"""Versioned feature definitions. Changing any definition requires bumping
FEATURE_CONFIG_VERSION so models trained on old definitions are not silently reused."""
from __future__ import annotations

FEATURE_CONFIG_VERSION = "fc-1.0"

# Stateful-window parameters
LATENESS_TOLERANCE_SECONDS = 600       # events older than (customer's latest event - 10 min) are "late"
RECENT_WINDOW_SECONDS = 3600           # history retained for windowed features
MAX_RECENT_EVENTS = 500
MAX_TRACKED_DEVICES = 20
MAX_TRACKED_LOCATIONS = 30
MAX_TRACKED_MERCHANTS = 200
MAX_SEEN_EVENT_IDS = 1000
NO_HISTORY_GAP_SECONDS = 7 * 24 * 3600  # imputation for "time since previous" when no history

NUMERIC_FEATURES: tuple[str, ...] = (
    "amount", "log_amount", "hour_of_day", "day_of_week", "is_night",
    "tx_count_1m", "tx_count_5m", "tx_count_1h", "spend_sum_15m", "spend_sum_1h",
    "hist_tx_count", "hist_mean_amount", "hist_std_amount", "amount_zscore", "amount_to_mean_ratio",
    "failed_count_1h", "failed_ratio_hist", "seconds_since_prev",
    "is_new_device", "is_new_location", "is_new_merchant", "merchant_frequency",
    "geo_velocity_kmh", "has_history", "is_high_risk_category",
)
CATEGORICAL_FEATURES: tuple[str, ...] = ("merchant_category", "channel", "transaction_type", "status")
ALL_FEATURES: tuple[str, ...] = NUMERIC_FEATURES + CATEGORICAL_FEATURES

# Inclusive valid ranges used by validate_features()
FEATURE_RANGES: dict[str, tuple[float, float]] = {
    "amount": (0.0, 1e7), "log_amount": (0.0, 17.0), "hour_of_day": (0, 23), "day_of_week": (0, 6),
    "is_night": (0, 1), "tx_count_1m": (0, 1e5), "tx_count_5m": (0, 1e5), "tx_count_1h": (0, 1e6),
    "spend_sum_15m": (0, 1e9), "spend_sum_1h": (0, 1e9), "hist_tx_count": (0, 1e9),
    "hist_mean_amount": (0, 1e7), "hist_std_amount": (0, 1e7), "amount_zscore": (-50, 50),
    "amount_to_mean_ratio": (0, 1000), "failed_count_1h": (0, 1e5), "failed_ratio_hist": (0, 1),
    "seconds_since_prev": (0, NO_HISTORY_GAP_SECONDS), "is_new_device": (0, 1), "is_new_location": (0, 1),
    "is_new_merchant": (0, 1), "merchant_frequency": (0, 1), "geo_velocity_kmh": (0, 20000),
    "has_history": (0, 1), "is_high_risk_category": (0, 1),
}

FEATURE_DESCRIPTIONS: dict[str, str] = {
    "amount": "Transaction amount in transaction currency.",
    "log_amount": "ln(1 + amount).",
    "hour_of_day": "UTC hour of event_time (0-23).",
    "day_of_week": "UTC weekday of event_time (Mon=0).",
    "is_night": "1 if hour_of_day in [0, 5].",
    "tx_count_1m": "Customer's prior events with event_time in [t-60s, t]. Current event excluded.",
    "tx_count_5m": "Customer's prior events in [t-300s, t].",
    "tx_count_1h": "Customer's prior events in [t-3600s, t].",
    "spend_sum_15m": "Sum of non-failed prior amounts in [t-900s, t].",
    "spend_sum_1h": "Sum of non-failed prior amounts in [t-3600s, t].",
    "hist_tx_count": "Number of prior events ever processed for the customer.",
    "hist_mean_amount": "Running mean of prior non-failed amounts (Welford). 0 if none.",
    "hist_std_amount": "Running sample std of prior non-failed amounts. 0 if < 2.",
    "amount_zscore": "(amount - hist_mean) / max(hist_std, 1). 0 if < 2 prior; clipped to [-50, 50].",
    "amount_to_mean_ratio": "amount / hist_mean; 1 if no history; clipped to [0, 1000].",
    "failed_count_1h": "Prior FAILED/DECLINED events in [t-3600s, t].",
    "failed_ratio_hist": "Prior failed events / prior events (0 if none).",
    "seconds_since_prev": "Seconds since the most recent prior event (capped at 7 days; 7 days if none).",
    "is_new_device": "1 if device_id never seen for this customer (0 if no history).",
    "is_new_location": "1 if city/country never seen for this customer (0 if no history).",
    "is_new_merchant": "1 if merchant never used by this customer (0 if no history).",
    "merchant_frequency": "Share of prior events at this merchant.",
    "geo_velocity_kmh": "Great-circle km from previous event location / elapsed hours (min 1 minute).",
    "has_history": "1 if the customer has at least one prior event.",
    "is_high_risk_category": "1 if merchant_category is in the configured high-risk set.",
    "merchant_category": "Categorical (one-hot encoded in model pipeline).",
    "channel": "Categorical payment channel.",
    "transaction_type": "Categorical transaction type.",
    "status": "Authorisation outcome reported with the event (APPROVED/DECLINED/FAILED).",
}

# Fields that must NEVER reach the model (evaluation labels / identifiers)
FORBIDDEN_FEATURE_FIELDS: frozenset[str] = frozenset(
    {"ground_truth", "is_suspicious", "scenario", "label", "is_fraud", "event_id", "transaction_id"}
)
