"""Micro-benchmark of the persistence step (Phase 12 bottleneck analysis): persist_scored_event with one DB
transaction per event (current consumer) vs one transaction per batch with a SAVEPOINT per event.

    python benchmarks/persist_microbench.py --events 2000 --batch 200

Runs against a THROWAWAY database `fs_persist_bench` created in the ISOLATED PostgreSQL (port from .env.isolated)
and dropped afterwards. Scored payloads are produced with the real detection code (local scorer + risk engine).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "backend")]
from benchmarks.common import read_env, write_result  # noqa: E402

DBNAME = "fs_persist_bench"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default=str(ROOT / ".env.isolated"))
    ap.add_argument("--events", type=int, default=2000)
    ap.add_argument("--batch", type=int, default=200)
    a = ap.parse_args()
    env = read_env(Path(a.env_file))
    import psycopg

    env = {**env, **{k: v for k, v in os.environ.items() if k.startswith("BENCH_PG_")}}
    host, port = env.get("BENCH_PG_HOST", "127.0.0.1"), int(env.get("BENCH_PG_PORT", env.get("POSTGRES_HOST_PORT", "5432")))
    admin = dict(host=host, port=port, user=env.get("POSTGRES_USER", "fraudstream"),
                 password=env.get("BENCH_PG_PASSWORD") or env["POSTGRES_PASSWORD"], dbname=env.get("POSTGRES_DB", "fraudstream"), autocommit=True)
    with psycopg.connect(**admin) as c:
        c.execute(f"DROP DATABASE IF EXISTS {DBNAME}")
        c.execute(f"CREATE DATABASE {DBNAME}")
    url = f"postgresql+psycopg://{admin['user']}:{admin['password']}@{host}:{port}/{DBNAME}"
    os.environ.update({"DATABASE_URL": url, "FRAUDSTREAM_PROFILE": "local", "FRAUDSTREAM_ENV": "test",
                       "JWT_SECRET": "x" * 40})
    try:
        from app.database.migrate import upgrade
        from app.database.session import SessionLocal, init_engine
        from app.risk_engine.detection import detect
        from app.risk_engine.engine import RiskEngine
        from app.services.persistence import persist_scored_event
        from ml.features.engineering import FeatureStore
        from ml.inference.scorer import Scorer
        from simulator.generator import SimulationConfig, TransactionGenerator

        init_engine(url)
        upgrade(url)
        scorer = Scorer(ROOT / "ml" / "artifacts")
        scorer.load_from_registry()
        engine, store = RiskEngine(), FeatureStore()
        gen = TransactionGenerator(SimulationConfig(tps=200, max_events=a.events * 2, duration_seconds=None, seed=77,
                                                    customer_count=500), datetime.now(timezone.utc))
        scored = []
        for ev in gen.events():
            res = store.process(ev)
            if not res.features:
                continue
            feats = res.features
            s = detect(ev.to_wire(), feats, scorer, engine, ingested_at=datetime.now(timezone.utc), explain=False)
            scored.append((ev, feats, s))
        half = len(scored) // 2

        def persist(db, item):  # type: ignore[no-untyped-def]
            ev, feats, s = item
            inf = s.get("inference") or {}
            return persist_scored_event(db, ev, feats, s["decision"], profile="distributed",
                                        ingested_at=datetime.now(timezone.utc), inference_ms=inf.get("inference_ms"),
                                        anomaly_version=inf.get("anomaly_version"), contributions=inf.get("contributions"),
                                        explanation_method=inf.get("explanation_method"))

        t0 = time.perf_counter()
        for item in scored[:half]:
            db = SessionLocal()
            persist(db, item)
            db.commit()
            db.close()
        per_event = time.perf_counter() - t0
        t0 = time.perf_counter()
        for i in range(half, len(scored), a.batch):
            db = SessionLocal()
            for item in scored[i:i + a.batch]:
                with db.begin_nested():
                    persist(db, item)
            db.commit()
            db.close()
        batched = time.perf_counter() - t0
        out = {"benchmark": "persist-micro", "measured": True, "events_each": half, "batch": a.batch,
               "per_event_tx_eps": round(half / per_event, 1), "batched_tx_eps": round((len(scored) - half) / batched, 1),
               "per_event_ms": round(per_event / half * 1000, 2), "batched_ms": round(batched / (len(scored) - half) * 1000, 2),
               "speedup": round(per_event / batched, 2),
               "client": "container on the isolated network" if host != "127.0.0.1" else "Windows host -> 127.0.0.1"}
        print(out)
        print(f"result: {write_result('persist-micro', out)}")
        from app.database.session import get_engine

        get_engine().dispose()
    finally:
        with psycopg.connect(**admin) as c:
            c.execute(f"DROP DATABASE IF EXISTS {DBNAME} WITH (FORCE)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
