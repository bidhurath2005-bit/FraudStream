"""Streaming benchmark: Kafka -> Spark Structured Streaming -> Kafka -> persistence consumer -> PostgreSQL.

    python benchmarks/stream_bench.py --rates 10,25,50,100 --duration 60          # paced levels
    python benchmarks/stream_bench.py --burst 20000                                # backlog: as fast as possible
    python benchmarks/stream_bench.py --source api --rates 10,25 --concurrency 4   # HTTP ingestion (/transactions/ingest)

Run from the host against the ISOLATED stack (default: project fraudstream-p12, .env.isolated). The measuring
process runs INSIDE a container on that stack's Docker network, so every timestamp used for a latency comes
from the same (Docker VM) clock: the producer's send time (= Kafka record timestamp of the raw event), the Kafka
record timestamp of the scored output written by Spark, and `transactions.processed_at` written by the
persistence consumer. The host samples `docker stats` and volume sizes meanwhile.

Latencies are reported SEPARATELY (never merged into one number):
  producer_ack_ms   send -> broker acknowledgement (acks=all, idempotent producer)            [Kafka source]
  http_accept_ms    HTTP request -> 202 from the API (includes the API's own Kafka ack wait)  [API source]
  stream_ms         raw record timestamp -> scored record timestamp (Spark processing incl. trigger wait)
  persist_ms        scored record timestamp -> processed_at (consumer poll + DB transaction)
  end_to_end_ms     raw record timestamp -> processed_at (= processed_at - ingested_at)
Every event carries a unique `BM<hex>-` customer namespace; only these events are measured.
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmarks.common import (  # noqa: E402
    ROOT,
    DockerStats,
    container_limits,
    dist,
    machine_info,
    read_env,
    volume_sizes,
    write_result,
)

TOPICS = ("transactions.raw", "transactions.normal", "transactions.suspicious", "fraud.dlq")


class Http:
    """Minimal keep-alive JSON client (standard library: the app image has no httpx)."""

    def __init__(self, base: str, token: str | None = None, timeout: float = 30):
        from urllib.parse import urlsplit

        u = urlsplit(base)
        self.host, self.port, self.prefix, self.timeout = u.hostname, u.port or 80, u.path.rstrip("/"), timeout
        self.headers = {"Content-Type": "application/json", **({"Authorization": f"Bearer {token}"} if token else {})}
        self.conn: Any = None

    def request(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        import http.client

        for attempt in (1, 2):
            if self.conn is None:
                self.conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
            try:
                self.conn.request(method, self.prefix + path, body=None if body is None else json.dumps(body),
                                  headers=self.headers)
                r = self.conn.getresponse()
                data = r.read()
                return r.status, (json.loads(data) if data else None)
            except (OSError, http.client.HTTPException):
                self.conn.close()
                self.conn = None
                if attempt == 2:
                    raise
        raise RuntimeError("unreachable")


# ====================================================================== in-container measurement
def _events(n: int | None, rate: float, seconds: float | None, seed: int, start: datetime,
            customers: int = 2000) -> list[Any]:
    from simulator.generator import SimulationConfig, TransactionGenerator

    tag = f"BM{secrets.token_hex(3)}-"
    gen = TransactionGenerator(SimulationConfig(mode="mixed", tps=min(rate, 5000), max_events=n, duration_seconds=seconds,
                                                customer_count=customers, suspicious_pct=5, seed=seed,
                                                population_seed=seed * 7), start)
    return [e.model_copy(update={"customer_id": tag + e.customer_id, "account_id": tag + e.account_id})
            for e in gen.events()]


class Collector(threading.Thread):
    """Reads the scored topics from their current end offsets; records each event's scored record timestamp."""

    def __init__(self, bootstrap: str):
        super().__init__(daemon=True)
        from confluent_kafka import Consumer, TopicPartition

        self.c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"bench-{secrets.token_hex(4)}",
                           "enable.auto.commit": False, "auto.offset.reset": "latest"})
        parts = []
        for t in ("transactions.normal", "transactions.suspicious"):
            md = self.c.list_topics(t, timeout=10).topics[t]
            for p in md.partitions:
                hi = self.c.get_watermark_offsets(TopicPartition(t, p), timeout=10)[1]
                parts.append(TopicPartition(t, p, hi))
        self.c.assign(parts)
        self.scored: dict[str, int] = {}
        self.dupes = 0
        self.stop = threading.Event()

    def run(self) -> None:
        while not self.stop.is_set():
            for m in self.c.consume(500, 0.5):
                if m.error():
                    continue
                eid = dict(m.headers() or {}).get("event_id")
                eid = eid.decode() if isinstance(eid, bytes) else eid
                if not eid:
                    try:
                        eid = json.loads(m.value())["event"]["event_id"]
                    except (ValueError, KeyError, TypeError):
                        continue
                if eid in self.scored:
                    self.dupes += 1          # Kafka output is at-least-once: a replayed batch can re-emit
                    continue
                self.scored[eid] = m.timestamp()[1]
        self.c.close()


class LagSampler(threading.Thread):
    def __init__(self, api: str, token: str, interval: float = 3.0):
        super().__init__(daemon=True)
        self.api, self.h, self.interval = api, {"Authorization": f"Bearer {token}"}, interval
        self.samples: list[dict[str, Any]] = []
        self.stop = threading.Event()

    def run(self) -> None:
        c = Http(self.api, self.h["Authorization"].split(" ", 1)[1], timeout=10)
        while not self.stop.is_set():
            t = time.time()
            try:
                s = c.request("GET", "/infrastructure/status")[1]
                score = [q for q in (s.get("spark") or {}).get("queries", []) if q.get("name") == "score"]
                self.samples.append({"t": t, "spark_lag": score[0].get("lag_vs_raw_topic") if score else None,
                                     "consumer_lag": _consumer_lag(s)})
            except Exception:  # noqa: BLE001 - sampling only
                pass
            self.stop.wait(self.interval)


def _consumer_lag(s: dict[str, Any]) -> Any:
    for g in (s.get("kafka") or {}).get("consumer_groups", []) or []:
        if g.get("group") == "fraudstream-persistence":
            return g.get("total_lag")
    return None


def _login(api: str) -> str:
    code, body = Http(api).request("POST", "/auth/login", {"username": os.environ["BENCH_ADMIN_USER"],
                                                         "password": os.environ["BENCH_ADMIN_PASSWORD"]})
    if code != 200:
        raise SystemExit(f"login failed: HTTP {code}")
    return body["access_token"]


def _pg() -> Any:
    import psycopg

    return psycopg.connect(host=os.environ.get("BENCH_PG_HOST", "postgres"), port=int(os.environ.get("BENCH_PG_PORT", "5432")),
                           user=os.environ.get("BENCH_PG_USER", "fraudstream"), password=os.environ["BENCH_PG_PASSWORD"],
                           dbname=os.environ.get("BENCH_PG_DB", "fraudstream"), autocommit=True)


def _persisted(conn: Any, ids: list[str]) -> dict[str, tuple[datetime | None, datetime, int]]:
    rows = conn.execute("SELECT event_id, min(ingested_at), min(processed_at), count(*) FROM transactions "
                        "WHERE event_id = ANY(%s) GROUP BY event_id", (ids,)).fetchall()
    return {r[0]: (r[1], r[2], r[3]) for r in rows}


def run_level(a: argparse.Namespace, rate: float | None, api: str, token: str) -> dict[str, Any]:
    from confluent_kafka import Producer

    from streaming.kafka.client import encode, producer_config

    bootstrap = os.environ.get("BENCH_KAFKA", "kafka:29092")
    start = datetime.now(UTC)
    # Customer population scales with the rate (--customers-per-eps): with a FIXED population, per-customer velocity
    # grows with the offered load and the velocity rules flag up to 40 % of events (measured in the first Phase 12
    # sustained run), which measures an alert storm, not normal traffic. 0 = the fixed 2,000 of that first run.
    customers = max(2000, int((rate or 0) * a.customers_per_eps)) if a.customers_per_eps else 2000
    if rate is None:                                  # burst: generate N events, send as fast as possible
        events = _events(a.burst, 5000, None, a.seed, start, customers)
    else:
        events = _events(None, rate, a.duration, a.seed + int(rate), start, customers)
    ids = [e.event_id for e in events]
    offs = [(e.event_time - start).total_seconds() for e in events]
    coll = Collector(bootstrap)
    coll.start()
    lag = LagSampler(api, token)
    lag.start()
    send_ts: dict[str, int] = {}
    ack_ms: list[float] = []
    accept_ms: list[float] = []
    errors: dict[str, int] = {}
    lock = threading.Lock()

    t_begin = time.time()
    if a.source == "kafka":
        prod = Producer(producer_config(bootstrap, "fraudstream-bench"))

        def cb(err: Any, msg: Any, t_send: float) -> None:
            if err is not None:
                errors[str(err.name())] = errors.get(str(err.name()), 0) + 1
            else:
                ack_ms.append((time.perf_counter() - t_send) * 1000)

        for e, off in zip(events, offs, strict=True):
            if rate is not None:
                delay = t_begin + off - time.time()
                if delay > 0:
                    time.sleep(delay)
            now_ms = int(time.time() * 1000)
            send_ts[e.event_id] = now_ms
            t_send = time.perf_counter()
            while True:
                try:
                    prod.produce("transactions.raw", key=e.customer_id.encode(), value=encode(e.to_wire()),
                                 headers={"event_id": e.event_id, "producer": "fraudstream-bench"}, timestamp=now_ms,
                                 on_delivery=lambda err, msg, t=t_send: cb(err, msg, t))
                    break
                except BufferError:
                    prod.poll(0.1)
            prod.poll(0)
        t_calls = time.time()
        prod.flush(60)
    else:                                             # HTTP ingestion through the API
        queue = list(zip(events, offs, strict=True))
        qi = [0]

        def worker() -> None:
            c = Http(api, token)
            if True:
                while True:
                    with lock:
                        if qi[0] >= len(queue):
                            return
                        e, off = queue[qi[0]]
                        qi[0] += 1
                    if rate is not None:
                        delay = t_begin + off - time.time()
                        if delay > 0:
                            time.sleep(delay)
                    t0 = time.perf_counter()
                    try:
                        code = str(c.request("POST", "/transactions/ingest", e.to_wire())[0])
                    except OSError as exc:
                        code = exc.__class__.__name__
                    with lock:
                        if code == "202":
                            accept_ms.append((time.perf_counter() - t0) * 1000)
                        else:
                            errors[code] = errors.get(code, 0) + 1

        ws = [threading.Thread(target=worker) for _ in range(a.concurrency)]
        for w in ws:
            w.start()
        for w in ws:
            w.join()
        t_calls = time.time()
    t_sent = time.time()                              # every event acknowledged (Kafka) / answered (HTTP)

    conn = _pg()
    deadline = time.time() + a.drain_timeout
    got: dict[str, Any] = {}
    while time.time() < deadline:
        got = _persisted(conn, ids)
        if len(got) >= len(ids) - sum(errors.values()):
            break
        time.sleep(2)
    t_done = time.time()
    time.sleep(3)                                     # let the collector catch the last scored records
    coll.stop.set()
    lag.stop.set()
    coll.join(10)
    lag.join(10)
    dlq = conn.execute("SELECT count(*) FROM dead_letters WHERE event_id = ANY(%s)", (ids,)).fetchone()[0]
    conn.close()

    stream_ms, persist_ms, e2e_ms = [], [], []
    for eid, (ing, proc, _n) in got.items():
        p_ms = proc.timestamp() * 1000
        raw = ing.timestamp() * 1000 if ing else send_ts.get(eid)
        sc = coll.scored.get(eid)
        if raw is not None:
            e2e_ms.append(p_ms - raw)
        if sc is not None and raw is not None:
            stream_ms.append(sc - raw)
            persist_ms.append(p_ms - sc)
    procs = sorted(v[1].timestamp() for v in got.values())
    send_seconds = t_sent - t_begin
    conn2 = _pg()
    alerts = conn2.execute("SELECT count(*) FROM alerts a JOIN transactions t ON t.id = a.transaction_id "
                           "WHERE t.event_id = ANY(%s)", (ids,)).fetchone()[0]
    conn2.close()
    out = {
        "source": a.source, "target_rate_eps": rate, "customers": customers, "alerts": alerts,
        "alert_rate": round(alerts / len(got), 4) if got else None, "burst_events": a.burst if rate is None else None,
        "duration_target_s": a.duration if rate is not None else None, "events_generated": len(events),
        "send_seconds": round(send_seconds, 2),
        "submit_call_seconds": round(t_calls - t_begin, 2),
        "submitted_eps": round(len(events) / (t_calls - t_begin), 1) if t_calls > t_begin else None,
        "accepted": len(ack_ms) if a.source == "kafka" else len(accept_ms),
        "accepted_eps": round((len(ack_ms) if a.source == "kafka" else len(accept_ms)) / send_seconds, 1)
        if send_seconds else None,
        "errors": errors, "error_rate": round(sum(errors.values()) / len(events), 5) if events else None,
        "persisted": len(got), "missing": len(ids) - len(got), "duplicate_rows": sum(1 for v in got.values() if v[2] > 1),
        "scored_duplicates_on_kafka": coll.dupes, "dead_letters": dlq,
        "persisted_eps_overall": round(len(got) / (procs[-1] - t_begin), 1) if procs else None,
        "persisted_eps_steady": round((len(procs) - 1) / (procs[-1] - procs[0]), 1) if len(procs) > 1 else None,
        "drain_seconds_after_send": round(t_done - t_sent, 1),
        "latency_ms": {"producer_ack_ms": dist(ack_ms), "http_accept_ms": dist(accept_ms),
                       "stream_ms": dist(stream_ms, 0), "persist_ms": dist(persist_ms, 0),
                       "end_to_end_ms": dist(e2e_ms, 0)},
        "lag": {"spark_lag_max": max((s["spark_lag"] for s in lag.samples if isinstance(s["spark_lag"], int)), default=None),
                "consumer_lag_max": max((s["consumer_lag"] for s in lag.samples if isinstance(s["consumer_lag"], int)),
                                        default=None),
                "samples": lag.samples},
        "window": {"t_begin": t_begin, "t_sent": t_sent, "t_done": t_done},
    }
    return out


def in_container(a: argparse.Namespace) -> int:
    api = os.environ.get("BENCH_API", "http://api:8000/api/v1")
    token = _login(api)
    levels: list[float | None] = [None] if a.burst else [float(x) for x in a.rates.split(",")]
    results = []
    for rate in levels:
        print(f"level {rate or 'burst'} ...", flush=True)
        r = run_level(a, rate, api, token)
        lat = r["latency_ms"]
        print(f"  submitted {r['submitted_eps']}/s accepted {r['accepted_eps']}/s persisted {r['persisted']}/"
              f"{r['events_generated']} steady {r['persisted_eps_steady']}/s | e2e p50 {lat['end_to_end_ms'].get('p50')} "
              f"p95 {lat['end_to_end_ms'].get('p95')} ms | errors {r['errors']} dupes {r['duplicate_rows']}", flush=True)
        results.append(r)
        time.sleep(a.pause)
    Path(a.out).write_text(json.dumps(results, default=str), encoding="utf-8")
    return 0


# ====================================================================== host launcher
def launch(a: argparse.Namespace) -> int:
    env = read_env(Path(a.env_file))
    project = a.project
    net = env.get("FRAUDSTREAM_NETWORK", f"{project}")
    results_dir = ROOT / "benchmarks" / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    tmp = results_dir / f".stream-{secrets.token_hex(4)}.json"
    child_env = {**os.environ, "BENCH_PG_PASSWORD": env["POSTGRES_PASSWORD"],
                 "BENCH_ADMIN_USER": env.get("FRAUDSTREAM_ADMIN_USERNAME", "admin"),
                 "BENCH_ADMIN_PASSWORD": env["FRAUDSTREAM_ADMIN_PASSWORD"]}
    mounts = []
    for d in ("benchmarks", "simulator", "streaming", "backend"):
        mounts += ["-v", f"{ROOT / d}:/app/{d}:ro"]
    passthrough = [x for x in sys.argv[1:] if x not in ("--project", project, "--env-file", a.env_file)]
    cmd = ["docker", "run", "--rm", "--network", net, *mounts, "-v", f"{results_dir}:/out",
           "-e", "BENCH_PG_PASSWORD", "-e", "BENCH_ADMIN_USER", "-e", "BENCH_ADMIN_PASSWORD",
           "-e", "BENCH_RESULTS_DIR=/out", "-e", "PYTHONPATH=/app:/app/backend", "-w", "/app", "--entrypoint", "python",
           "fraudstream-app:local", "benchmarks/stream_bench.py", "--in-container", "--out", f"/out/{tmp.name}",
           *passthrough]
    before = volume_sizes(project)
    t0 = time.time()
    with DockerStats(project) as stats:
        rc = subprocess.run(cmd, env=child_env, timeout=a.timeout).returncode
    after = volume_sizes(project)
    if rc != 0 or not tmp.exists():
        print(f"FAILED: benchmark container exited {rc}")
        return 1
    levels = json.loads(tmp.read_text(encoding="utf-8"))
    tmp.unlink()
    for lv in levels:
        w = lv["window"]
        lv["resources"] = stats.summary(w["t_begin"], w["t_done"])
    payload = {"benchmark": "stream", "measured": True, "started_at": datetime.fromtimestamp(t0, UTC).isoformat(),
               "project": project, "args": {k: v for k, v in vars(a).items() if k not in ("env_file",)},
               "machine": machine_info(), "container_limits": container_limits(project),
               "volume_bytes_before": before, "volume_bytes_after": after,
               "volume_growth_bytes": {k: (after[k] - before[k]) if after.get(k) is not None and before.get(k) is not None
                                       else None for k in before},
               "levels": levels}
    p = write_result(f"stream-{a.source}-{'burst' if a.burst else 'rates'}", payload)
    print(f"result: {p}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--project", default="fraudstream-p12")
    ap.add_argument("--env-file", default=str(ROOT / ".env.isolated"))
    ap.add_argument("--source", choices=("kafka", "api"), default="kafka")
    ap.add_argument("--rates", default="10,25,50,100")
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--burst", type=int, default=0)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--customers-per-eps", type=int, default=200)
    ap.add_argument("--drain-timeout", type=float, default=600)
    ap.add_argument("--pause", type=float, default=10)
    ap.add_argument("--seed", type=int, default=1212)
    ap.add_argument("--timeout", type=int, default=7200)
    ap.add_argument("--in-container", action="store_true")
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    if a.project == "fraudstream" and not os.environ.get("BENCH_ALLOW_LIVE"):
        print("refusing to benchmark the LIVE project (writes thousands of rows); use the isolated stack")
        return 2
    return in_container(a) if a.in_container else launch(a)


if __name__ == "__main__":
    raise SystemExit(main())
