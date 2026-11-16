"""Checkpoint inspection used by the Streaming Infrastructure page (no JVM needed)."""
from __future__ import annotations

import os

from streaming.spark.checkpoint import checkpoint_info


def test_checkpoint_info_reads_offsets_and_commits(tmp_path):
    loc = tmp_path / "score"
    (loc / "offsets").mkdir(parents=True)
    (loc / "commits").mkdir()
    (loc / "state").mkdir()
    for b in (0, 1, 2, 3):
        (loc / "offsets" / str(b)).write_text("v1")
    for b in (0, 1, 2):
        (loc / "commits" / str(b)).write_text("v1")
    (loc / "offsets" / ".3.crc").write_text("x")      # Spark's checksum files must be ignored
    (loc / "commits" / "metadata").write_text("x")
    os.utime(loc / "commits" / "2", (1000.0, 1000.0))
    info = checkpoint_info(str(loc), now=1010.0)
    assert info == {"location": str(loc), "exists": True, "last_committed_batch": 2, "last_planned_batch": 3,
                    "uncommitted_batches": 1, "last_commit_at": 1000.0, "last_commit_age_seconds": 10.0,
                    "has_state": True}


def test_checkpoint_info_missing_or_empty(tmp_path):
    assert checkpoint_info(str(tmp_path / "nope")) == {"location": str(tmp_path / "nope"), "exists": False}
    (tmp_path / "new").mkdir()
    info = checkpoint_info(str(tmp_path / "new"))
    assert info["exists"] is True and info["last_committed_batch"] is None and info["uncommitted_batches"] is None
    assert info["has_state"] is False
