"""Reproducible synthetic training dataset built from the transaction simulator.

The per-customer activity rate (~1 event / 2000 s on average) intentionally matches the
default live simulation (20 000 customers at 10 TPS) so feature distributions are
comparable between training and live scoring. Changing the live configuration
substantially (e.g. 200 customers at 50 TPS) WILL produce data drift — that is real
and is surfaced by the drift monitor.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from simulator.generator import SimulationConfig, TransactionGenerator
from streaming.schemas.events import TransactionEvent


@dataclass(frozen=True)
class SyntheticDatasetSpec:
    name: str = "synthetic-fraudstream"
    version: str = "1.0"
    customer_count: int = 3000
    tps: float = 1.5
    duration_seconds: float = 86_400.0
    suspicious_pct: float = 1.5
    avg_amount: float = 60.0
    seed: int = 2026
    start_time: str = "2026-01-05T00:00:00+00:00"

    def to_config(self) -> SimulationConfig:
        # population_seed=None: LEGACY population mode, required to reproduce dataset v1.0 exactly
        # (it was generated before stable customer identities were introduced in Phase 8).
        return SimulationConfig(mode="mixed", tps=self.tps, duration_seconds=self.duration_seconds,
                                customer_count=self.customer_count, suspicious_pct=self.suspicious_pct,
                                avg_amount=self.avg_amount, seed=self.seed, population_seed=None)


def generate_events(spec: SyntheticDatasetSpec) -> list[TransactionEvent]:
    start = datetime.fromisoformat(spec.start_time).astimezone(timezone.utc)
    return list(TransactionGenerator(spec.to_config(), start).events())


def write_dataset(spec: SyntheticDatasetSpec, out_dir: Path) -> dict:
    """Write events as gzipped JSONL + metadata. Returns metadata dict."""
    out_dir.mkdir(parents=True, exist_ok=True)
    events = generate_events(spec)
    path = out_dir / f"{spec.name}-v{spec.version}.jsonl.gz"
    h = hashlib.sha256()
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for ev in events:
            line = json.dumps(ev.to_wire(), sort_keys=True)
            h.update(line.encode())
            fh.write(line + "\n")
    pos = sum(1 for e in events if e.ground_truth and e.ground_truth.is_suspicious)
    meta = {
        "name": spec.name, "version": spec.version, "source": "synthetic",
        "provenance": "Generated locally by simulator.generator.TransactionGenerator (fully synthetic).",
        "spec": asdict(spec), "rows": len(events), "positives": pos,
        "positive_rate": pos / max(1, len(events)), "content_sha256": h.hexdigest(),
        "path": path.name, "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (out_dir / f"{spec.name}-v{spec.version}.meta.json").write_text(json.dumps(meta, indent=2))
    return meta


def load_events(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]
