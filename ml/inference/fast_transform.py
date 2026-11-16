"""Low-overhead re-implementation of the FITTED preprocessing ColumnTransformer.

sklearn's ColumnTransformer on a 1-row pandas DataFrame costs ~20 ms per call on this
machine (column selection/validation overhead), which dominated per-event latency.
This class reads the fitted parameters (StandardScaler mean_/scale_, OneHotEncoder
categories_) and applies exactly the same arithmetic with NumPy. Equality with
``ColumnTransformer.transform`` is asserted in ml/tests/test_inference_fast_path.py.
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from ml.features.definitions import CATEGORICAL_FEATURES, NUMERIC_FEATURES


class FastPreprocessor:
    def __init__(self, ct: ColumnTransformer) -> None:
        names = [name for name, _, _ in ct.transformers_ if name != "remainder"]
        if names != ["num", "cat"]:
            raise ValueError(f"unexpected transformer layout {names}")
        num_cols = list(ct.transformers_[0][2])
        cat_cols = list(ct.transformers_[1][2])
        if num_cols != list(NUMERIC_FEATURES) or cat_cols != list(CATEGORICAL_FEATURES):
            raise ValueError("fitted column order differs from feature definitions")
        num = ct.named_transformers_["num"]
        self.mean: np.ndarray | None = None
        self.scale: np.ndarray | None = None
        if isinstance(num, StandardScaler):
            self.mean = num.mean_.astype(float)
            self.scale = num.scale_.astype(float)
        cat = ct.named_transformers_["cat"]
        if not isinstance(cat, OneHotEncoder) or cat.drop is not None:
            raise ValueError("expected a OneHotEncoder without drop")
        self.lookups = [{str(v): i for i, v in enumerate(c)} for c in cat.categories_]
        sizes = [len(c) for c in cat.categories_]
        self.offsets = np.concatenate([[0], np.cumsum(sizes)[:-1]]).astype(int) + len(NUMERIC_FEATURES)
        self.width = len(NUMERIC_FEATURES) + sum(sizes)
        self.feature_names = list(ct.get_feature_names_out())

    def transform(self, rows: Sequence[dict[str, Any]]) -> np.ndarray:
        n = len(rows)
        out = np.zeros((n, self.width), dtype=float)
        num = np.array([[float(r[c]) for c in NUMERIC_FEATURES] for r in rows], dtype=float)
        if self.mean is not None and self.scale is not None:
            num = (num - self.mean) / self.scale
        out[:, : len(NUMERIC_FEATURES)] = num
        for j, col in enumerate(CATEGORICAL_FEATURES):
            lookup, off = self.lookups[j], self.offsets[j]
            for i, r in enumerate(rows):
                k = lookup.get(str(r[col]))
                if k is not None:          # handle_unknown="ignore": unseen category -> all zeros
                    out[i, off + k] = 1.0
        return out


class FastIsolationForest:
    """Exact re-implementation of IsolationForest anomaly scoring for small batches.

    Mirrors sklearn 1.5 ``_compute_score_samples``: inputs are cast to float32 (sklearn's
    tree dtype), each tree is traversed with ``x[feature] <= threshold`` and the leaf's
    precomputed decision-path length + average path length are summed. Returns the same
    value as ``-IsolationForest.score_samples(X)``. Equality is asserted in tests.
    """

    def __init__(self, est: Any) -> None:
        from sklearn.ensemble._iforest import _average_path_length

        self.trees = []
        for tree_idx, (t, feats) in enumerate(zip(est.estimators_, est.estimators_features_, strict=True)):
            tr = t.tree_
            leaf_cost = est._decision_path_lengths[tree_idx] + est._average_path_length_per_tree[tree_idx] - 1.0
            self.trees.append((tr.children_left, tr.children_right, tr.feature, tr.threshold, np.asarray(feats), leaf_cost))
        self.subsample = est._max_features != est.n_features_in_
        self.denominator = len(est.estimators_) * float(_average_path_length([est._max_samples])[0])

    def anomaly(self, x: np.ndarray) -> np.ndarray:
        x = np.asarray(x, dtype=np.float32)
        depths = np.zeros(x.shape[0])
        for left, right, feature, threshold, feats, leaf_cost in self.trees:
            xs = x[:, feats] if self.subsample else x
            for i in range(xs.shape[0]):
                row = xs[i]
                node = 0
                while left[node] != -1:
                    node = left[node] if row[feature[node]] <= threshold[node] else right[node]
                depths[i] += leaf_cost[node]
        if self.denominator == 0:
            return np.ones_like(depths)
        return 2 ** (-depths / self.denominator)
