"""Leakage-resistant splitting.

* ``temporal_split`` — train/validation/test ordered by event time (no shuffling), so
  every validation/test event is strictly later than every training event.
* ``customer_holdout`` — removes a deterministic subset of customers from training so the
  model can be evaluated on customers it has never seen.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pandas as pd

from ml.features.definitions import ALL_FEATURES, CATEGORICAL_FEATURES, NUMERIC_FEATURES


@dataclass
class Splits:
    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame
    unseen_customer_test: pd.DataFrame
    train_end: pd.Timestamp
    validation_end: pd.Timestamp


def _holdout_customer(cid: str, fraction: float, salt: str) -> bool:
    h = int(hashlib.sha256(f"{salt}:{cid}".encode()).hexdigest()[:8], 16)
    return (h % 10_000) < fraction * 10_000


def temporal_split(df: pd.DataFrame, *, train_frac: float = 0.6, val_frac: float = 0.2,
                   holdout_customer_frac: float = 0.15, time_col: str = "meta_event_time",
                   customer_col: str = "meta_customer_id", salt: str = "fraudstream") -> Splits:
    if not 0 < train_frac < 1 or not 0 < val_frac < 1 or train_frac + val_frac >= 1:
        raise ValueError("invalid split fractions")
    d = df.sort_values(time_col, kind="mergesort").reset_index(drop=True)
    t_train = d[time_col].quantile(train_frac, interpolation="lower")
    t_val = d[time_col].quantile(train_frac + val_frac, interpolation="lower")
    held = d[customer_col].map(lambda c: _holdout_customer(str(c), holdout_customer_frac, salt))
    train = d[(d[time_col] <= t_train) & ~held]
    val = d[(d[time_col] > t_train) & (d[time_col] <= t_val) & ~held]
    test = d[(d[time_col] > t_val) & ~held]
    unseen = d[(d[time_col] > t_val) & held]
    return Splits(train, val, test, unseen, pd.Timestamp(t_train), pd.Timestamp(t_val))


def xy(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    x = df[list(ALL_FEATURES)].copy()
    for c in NUMERIC_FEATURES:
        x[c] = x[c].astype(float)
    for c in CATEGORICAL_FEATURES:
        x[c] = x[c].astype(str)
    return x, df["meta_label"].astype(int)
