"""Evaluation metrics for imbalanced binary classification.

PR-AUC is reported as *average precision* (sklearn ``average_precision_score``):
AP = sum_n (R_n - R_{n-1}) * P_n, a step-wise summary of the precision-recall curve that
does not use linear interpolation (which can be optimistic). Accuracy is deliberately
not reported as a quality metric.
"""
from __future__ import annotations

import time
from typing import Any

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)


def choose_threshold(y_true: np.ndarray, scores: np.ndarray, strategy: str = "max_f1",
                     target_recall: float = 0.9) -> float:
    """Pick a decision threshold on VALIDATION data only."""
    precision, recall, thresholds = precision_recall_curve(y_true, scores)
    if len(thresholds) == 0:
        return 0.5
    p, r = precision[:-1], recall[:-1]
    if strategy == "target_recall":
        ok = np.where(r >= target_recall)[0]
        idx = ok[np.argmax(p[ok])] if len(ok) else int(np.argmax(r))
    else:
        f1 = np.where(p + r > 0, 2 * p * r / np.maximum(p + r, 1e-12), 0)
        idx = int(np.argmax(f1))
    return float(thresholds[idx])


def classification_report(y_true: np.ndarray, scores: np.ndarray, threshold: float) -> dict[str, Any]:
    y_true = np.asarray(y_true).astype(int)
    pred = (np.asarray(scores) >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
    has_both = len(np.unique(y_true)) == 2
    return {
        "threshold": float(threshold),
        "support": int(len(y_true)), "positives": int(y_true.sum()),
        "precision": float(precision_score(y_true, pred, zero_division=0)),
        "recall": float(recall_score(y_true, pred, zero_division=0)),
        "f1": float(f1_score(y_true, pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, scores)) if has_both else None,
        "pr_auc": float(average_precision_score(y_true, scores)) if has_both else None,
        "false_positive_rate": float(fp / (fp + tn)) if (fp + tn) else 0.0,
        "false_negative_rate": float(fn / (fn + tp)) if (fn + tp) else 0.0,
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
        "pr_auc_definition": "average_precision (step-wise, sklearn)",
    }


def measure_latency(predict_fn, rows, repeats: int = 200) -> dict[str, float]:
    """Measure single-row inference latency in milliseconds (wall clock)."""
    samples = []
    n = len(rows)
    for i in range(repeats):
        row = rows.iloc[[i % n]]
        t0 = time.perf_counter()
        predict_fn(row)
        samples.append((time.perf_counter() - t0) * 1000)
    arr = np.array(samples)
    return {"mean_ms": float(arr.mean()), "p50_ms": float(np.percentile(arr, 50)),
            "p95_ms": float(np.percentile(arr, 95)), "p99_ms": float(np.percentile(arr, 99)),
            "samples": repeats}
