"""Canonical transaction event schema shared by the simulator, API, Kafka and Spark layers.

The schema is versioned (``schema_version``). Every component validates events against
this model (Python) or the equivalent JVM-native Spark validation (``EVENT_JSON_SCHEMA`` and the validation
expressions in ``streaming/spark/fraud_stream_job.py``; parity is asserted by the Spark integration tests).

``ground_truth`` is simulator-only metadata used for *evaluation*. It is never passed to
feature engineering or models (see ``ml.features.engineering.strip_ground_truth``).
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = "1.0"


class TransactionType(str, Enum):
    PURCHASE = "PURCHASE"
    TRANSFER = "TRANSFER"
    WITHDRAWAL = "WITHDRAWAL"
    PAYMENT = "PAYMENT"
    REFUND = "REFUND"


class PaymentChannel(str, Enum):
    CARD_PRESENT = "CARD_PRESENT"
    ONLINE = "ONLINE"
    MOBILE_APP = "MOBILE_APP"
    ATM = "ATM"
    BANK_TRANSFER = "BANK_TRANSFER"


class TransactionStatus(str, Enum):
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"
    FAILED = "FAILED"


MERCHANT_CATEGORIES: tuple[str, ...] = (
    "grocery", "restaurants", "fuel", "utilities", "retail", "electronics", "travel",
    "entertainment", "healthcare", "online_services", "gift_cards", "crypto_exchange",
    "gambling", "money_transfer",
)
HIGH_RISK_MERCHANT_CATEGORIES: frozenset[str] = frozenset(
    {"gift_cards", "crypto_exchange", "gambling", "money_transfer"}
)


class GeoLocation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    city: str = Field(min_length=1, max_length=80)
    country: str = Field(min_length=2, max_length=2)
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    simulated: bool = True


class GroundTruth(BaseModel):
    """Simulator-only evaluation label. NEVER a model input."""
    model_config = ConfigDict(extra="forbid")
    is_suspicious: bool
    scenario: str = Field(max_length=64)


class TransactionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    event_id: str = Field(min_length=8, max_length=64)
    transaction_id: str = Field(min_length=4, max_length=64)
    customer_id: str = Field(min_length=1, max_length=64)
    account_id: str = Field(min_length=1, max_length=64)
    merchant_id: str = Field(min_length=1, max_length=64)
    merchant_name: str = Field(default="", max_length=120)
    amount: Decimal = Field(gt=0, le=Decimal("10000000"), max_digits=14, decimal_places=2)
    currency: str = Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    event_time: datetime
    transaction_type: TransactionType
    merchant_category: str
    channel: PaymentChannel
    device_id: str = Field(min_length=1, max_length=64)
    ip_address: str = Field(min_length=3, max_length=45)
    location: GeoLocation
    status: TransactionStatus
    schema_version: str = SCHEMA_VERSION
    synthetic: bool = True
    source: str = Field(default="simulator", max_length=32)
    ground_truth: GroundTruth | None = None

    @field_validator("event_time")
    @classmethod
    def _utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("event_time must be timezone-aware (UTC)")
        return v.astimezone(timezone.utc)

    @field_validator("merchant_category")
    @classmethod
    def _category(cls, v: str) -> str:
        if v not in MERCHANT_CATEGORIES:
            raise ValueError(f"unknown merchant_category '{v}'")
        return v

    @field_validator("schema_version")
    @classmethod
    def _version(cls, v: str) -> str:
        if v.split(".")[0] != SCHEMA_VERSION.split(".")[0]:
            raise ValueError(f"unsupported schema_version '{v}'")
        return v

    def to_wire(self) -> dict[str, Any]:
        """JSON-serialisable dict used on Kafka and HTTP."""
        d = self.model_dump(mode="json")
        d["amount"] = float(self.amount)
        return d


class EventValidationError(ValueError):
    def __init__(self, reason: str, payload: Any = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.payload = payload


def parse_event(raw: bytes | str | dict[str, Any]) -> TransactionEvent:
    """Parse + validate a raw event. Raises EventValidationError with a reason."""
    import json

    from pydantic import ValidationError

    try:
        data = json.loads(raw) if isinstance(raw, (bytes, str)) else raw
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise EventValidationError(f"MALFORMED_JSON: {exc.__class__.__name__}", raw) from exc
    if not isinstance(data, dict):
        raise EventValidationError("MALFORMED_JSON: payload is not an object", raw)
    try:
        return TransactionEvent.model_validate(data)
    except ValidationError as exc:
        first = exc.errors()[0]
        loc = ".".join(str(p) for p in first.get("loc", ()))
        raise EventValidationError(f"SCHEMA_VIOLATION: {loc}: {first.get('msg')}", data) from exc
