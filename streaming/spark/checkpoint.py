"""Read-only inspection of a Structured Streaming checkpoint directory (stdlib only, host-testable).

Spark writes ``offsets/<batchId>`` when a micro-batch is planned and ``commits/<batchId>`` once it
has completed; a batch with an offset file but no commit file is in flight (or will be replayed
after a restart). Nothing here writes to or deletes from the checkpoint.
"""
from __future__ import annotations

import os
import time
from typing import Any


def _latest_batch(directory: str) -> tuple[int | None, float | None]:
    try:
        names = os.listdir(directory)
    except OSError:
        return None, None
    ids = [int(n) for n in names if n.isdigit()]
    if not ids:
        return None, None
    latest = max(ids)
    try:
        mtime = os.path.getmtime(os.path.join(directory, str(latest)))
    except OSError:
        mtime = None
    return latest, mtime


def checkpoint_info(location: str, now: float | None = None) -> dict[str, Any]:
    now = time.time() if now is None else now
    if not os.path.isdir(location):
        return {"location": location, "exists": False}
    committed, commit_mtime = _latest_batch(os.path.join(location, "commits"))
    planned, _ = _latest_batch(os.path.join(location, "offsets"))
    return {
        "location": location, "exists": True,
        "last_committed_batch": committed,
        "last_planned_batch": planned,
        "uncommitted_batches": (planned - committed) if planned is not None and committed is not None else None,
        "last_commit_at": commit_mtime,
        "last_commit_age_seconds": round(now - commit_mtime, 1) if commit_mtime else None,
        "has_state": os.path.isdir(os.path.join(location, "state")),
    }
