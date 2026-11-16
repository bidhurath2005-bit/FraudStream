"""Adapters for public fraud datasets. NONE of these datasets are bundled or downloaded
automatically (Kaggle requires an account and licence acceptance). Place a CSV file locally and
train on it with ``python -m ml.training.train --dataset paysim|ieee --csv PATH`` (or upload it through
``POST /api/v1/datasets/upload``); see docs/DATA_AND_ML.md section 1.

Schemas differ substantially and are NOT combined:

* PaySim: synthetic mobile-money simulation; hourly ``step`` resolution; customer
  (``nameOrig``) available -> mapped into the behavioural pipeline. Balance columns are
  intentionally DROPPED: ``newbalance*`` are post-event values (leakage) and PaySim's
  fraud logic empties balances, which makes them trivially predictive.
* Credit Card Fraud (ULB): PCA components V1..V28, no customer identifier -> behavioural
  features are impossible; supported only as a separate tabular benchmark.
* IEEE-CIS: ``TransactionDT`` is a relative time offset in seconds; there is no true
  customer id, so a documented heuristic pseudo-customer key (card1|addr1|P_emaildomain)
  is used.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

MAX_UPLOAD_BYTES = 200 * 1024 * 1024
BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class DatasetSchema:
    key: str
    title: str
    url: str
    required_columns: tuple[str, ...]
    label_column: str
    behavioural: bool
    notes: str


SCHEMAS: dict[str, DatasetSchema] = {
    "paysim": DatasetSchema(
        "paysim", "PaySim synthetic financial transactions", "https://www.kaggle.com/datasets/ealaxi/paysim1",
        ("step", "type", "amount", "nameOrig", "nameDest", "isFraud"), "isFraud", True,
        "Balance columns dropped (post-event leakage). Hourly time resolution."),
    "creditcard": DatasetSchema(
        "creditcard", "Credit Card Fraud Detection (ULB)", "https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud",
        ("Time", "Amount", "Class") + tuple(f"V{i}" for i in range(1, 29)), "Class", False,
        "No customer id: tabular benchmark only, not deployable to live scoring."),
    "ieee": DatasetSchema(
        "ieee", "IEEE-CIS Fraud Detection (train_transaction.csv)",
        "https://www.kaggle.com/competitions/ieee-fraud-detection",
        ("TransactionID", "isFraud", "TransactionDT", "TransactionAmt", "ProductCD", "card1"), "isFraud", True,
        "Pseudo-customer = card1|addr1|P_emaildomain (heuristic)."),
}


class DatasetValidationError(ValueError):
    pass


def validate_frame(df: pd.DataFrame, kind: str) -> dict[str, Any]:
    """Validate columns/labels and return a profile. Raises DatasetValidationError."""
    if kind not in SCHEMAS:
        raise DatasetValidationError(f"unknown dataset kind '{kind}'")
    schema = SCHEMAS[kind]
    missing = [c for c in schema.required_columns if c not in df.columns]
    if missing:
        raise DatasetValidationError(f"missing required columns for {kind}: {missing}")
    if df.empty:
        raise DatasetValidationError("dataset is empty")
    labels = pd.to_numeric(df[schema.label_column], errors="coerce")
    if labels.isna().any() or not set(labels.unique()).issubset({0, 1}):
        raise DatasetValidationError(f"label column '{schema.label_column}' must be binary 0/1")
    return profile_frame(df, schema.label_column)


def profile_frame(df: pd.DataFrame, label_col: str | None) -> dict[str, Any]:
    prof: dict[str, Any] = {
        "rows": int(len(df)), "columns": int(df.shape[1]),
        "missing_by_column": {c: round(float(v), 6) for c, v in df.isna().mean().items() if v > 0},
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
    }
    if label_col and label_col in df.columns:
        pos = int(df[label_col].sum())
        prof.update(positives=pos, negatives=int(len(df) - pos), positive_rate=pos / max(1, len(df)),
                    imbalance_ratio=(len(df) - pos) / max(1, pos))
    amt_col = next((c for c in ("amount", "Amount", "TransactionAmt") if c in df.columns), None)
    if amt_col:
        a = pd.to_numeric(df[amt_col], errors="coerce")
        prof["amount"] = {k: float(v) for k, v in a.describe(percentiles=[0.5, 0.95, 0.99]).items()}
    return prof


def read_csv_checked(path: Path, kind: str, nrows: int | None = None) -> pd.DataFrame:
    if path.suffix.lower() != ".csv":
        raise DatasetValidationError("only .csv files are accepted")
    if path.stat().st_size > MAX_UPLOAD_BYTES and nrows is None:
        raise DatasetValidationError(f"file exceeds {MAX_UPLOAD_BYTES // 1024 // 1024} MB; pass nrows to sample")
    df = pd.read_csv(path, nrows=nrows)
    validate_frame(df, kind)
    return df


def _loc(city: str = "Unknown", country: str = "ZZ") -> dict[str, Any]:
    return {"city": city, "country": country, "lat": 0.0, "lon": 0.0, "simulated": True}


def paysim_to_events(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Map PaySim rows into the canonical event dict (ground_truth from isFraud)."""
    type_map = {"PAYMENT": ("PAYMENT", "ONLINE"), "TRANSFER": ("TRANSFER", "BANK_TRANSFER"),
                "CASH_OUT": ("WITHDRAWAL", "ATM"), "DEBIT": ("WITHDRAWAL", "ATM"), "CASH_IN": ("PAYMENT", "MOBILE_APP")}
    out = []
    for i, r in enumerate(df.itertuples(index=False)):
        tx_type, channel = type_map.get(str(r.type), ("PAYMENT", "ONLINE"))
        dest = str(r.nameDest)
        out.append({
            "event_id": f"paysim-{i:010d}", "transaction_id": f"PS{i:012d}", "customer_id": str(r.nameOrig),
            "account_id": str(r.nameOrig), "merchant_id": dest,
            "amount": max(0.01, float(r.amount)), "currency": "USD",
            # step = hour index; spread rows within the hour deterministically (documented approximation)
            "event_time": BASE_TIME + timedelta(hours=int(r.step), seconds=(i % 3600)),
            "transaction_type": tx_type, "merchant_category": "retail" if dest.startswith("M") else "money_transfer",
            "channel": channel, "device_id": "unknown", "ip_address": "0.0.0.0", "location": _loc(),
            "status": "APPROVED", "ground_truth": {"is_suspicious": bool(r.isFraud), "scenario": "paysim_label"},
        })
    return out


def ieee_to_events(df: pd.DataFrame) -> list[dict[str, Any]]:
    cat = {"W": "retail", "C": "online_services", "R": "travel", "H": "entertainment", "S": "electronics"}
    out = []
    for r in df.to_dict("records"):
        key = "|".join(str(r.get(k, "na")) for k in ("card1", "addr1", "P_emaildomain"))
        amt = float(r["TransactionAmt"])
        if not math.isfinite(amt) or amt <= 0:
            continue
        out.append({
            "event_id": f"ieee-{int(r['TransactionID'])}", "transaction_id": f"IE{int(r['TransactionID'])}",
            "customer_id": key, "account_id": key, "merchant_id": f"P{r['ProductCD']}",
            "amount": amt, "currency": "USD",
            "event_time": BASE_TIME + timedelta(seconds=int(r["TransactionDT"])),
            "transaction_type": "PURCHASE", "merchant_category": cat.get(str(r["ProductCD"]), "retail"),
            "channel": "ONLINE", "device_id": str(r.get("DeviceInfo", "unknown") or "unknown"),
            "ip_address": "0.0.0.0", "location": _loc(), "status": "APPROVED",
            "ground_truth": {"is_suspicious": bool(r["isFraud"]), "scenario": "ieee_label"},
        })
    return out


def creditcard_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    """Tabular benchmark features (V1..V28, Amount, log Amount), label and time."""
    x = df[[f"V{i}" for i in range(1, 29)] + ["Amount"]].copy()
    x["log_amount"] = x["Amount"].clip(lower=0).map(math.log1p)
    return x, df["Class"].astype(int), df["Time"]
