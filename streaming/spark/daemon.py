"""Custom PySpark worker daemon (configured via ``spark.python.daemon.module``).

PySpark forks every Python worker from this daemon. Importing pandas/scikit-learn/XGBoost
and loading the active models HERE, before any fork, lets every worker inherit them
copy-on-write, removing the per-worker import + model-load cost that made the first
micro-batches take 4-6 s (measured in Phase 9). Each forked worker still refreshes the
runtime config (active model / rules) itself via logic.get_watcher().
"""
from __future__ import annotations

import sys

try:
    from streaming.spark import logic

    logic.get_watcher()          # imports the ML stack and loads the active models once
except Exception as exc:  # noqa: BLE001 - never prevent workers from starting; they load lazily instead
    print(f"fraudstream daemon preload failed: {exc!r}", file=sys.stderr)

from pyspark.daemon import manager  # noqa: E402

if __name__ == "__main__":
    manager()
