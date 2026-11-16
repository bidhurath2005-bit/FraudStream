"""Container integration tests against the RUNNING Docker Compose stack (profile ``distributed``).

    docker compose --env-file .env --profile distributed up -d --build
    python -m pytest -m containers infrastructure/tests -v

Everything is exercised through the published entry points only: the dashboard's nginx
(http://127.0.0.1:8080, which proxies /api to the API container), Kafka's host listener and
PostgreSQL's host port (for exactly-once assertions). Nothing is mocked.

Tests marked ``disruptive`` deliberately crash/stop containers of the live stack (API, persistence
consumer, Spark, Kafka, PostgreSQL) and verify recovery; they restore every service they stop.
They use the production topics and the configured database; every event they produce has a
unique ``CT<hex>-`` customer namespace and is synthetic (like all FraudStream data).

Credentials are read from the environment or from the repository's .env
(FRAUDSTREAM_ADMIN_USERNAME / FRAUDSTREAM_ADMIN_PASSWORD, POSTGRES_*, REDIS_PASSWORD).

Phase 12 — target selection (defaults = the live stack, as before):
    FS_COMPOSE_PROJECT=fraudstream-p12 FS_ENV_FILE=.env.isolated     FS_COMPOSE_FILES=docker-compose.yml,infrastructure/isolated/docker-compose.isolated.yml         python -m pytest -m containers infrastructure/tests
Disruptive tests REFUSE to run against the live project ('fraudstream') unless FS_ALLOW_DISRUPTIVE_ON_LIVE=1:
failure injection belongs on the isolated stack.
"""
from __future__ import annotations

import json
import os
import secrets
import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.containers]

ROOT = Path(__file__).resolve().parents[2]
APP_SERVICES = ("api", "persistence-consumer", "frontend", "spark")
INFRA_SERVICES = ("kafka", "postgres", "redis")
SPARK = "Spark Structured Streaming"


PROJECT = os.environ.get("FS_COMPOSE_PROJECT", "fraudstream")
ENV_FILE = ROOT / os.environ.get("FS_ENV_FILE", ".env")
COMPOSE_FILES = [f for f in os.environ.get("FS_COMPOSE_FILES", "").split(",") if f]


def _dotenv() -> dict[str, str]:
    out: dict[str, str] = {}
    p = ENV_FILE
    if not p.exists():
        return out
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        v = v.split(" #", 1)[0].strip()          # inline comments (same rule as docker compose)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


ENV = {**_dotenv(), **os.environ}
BASE = ENV.get("FS_STACK_URL", f"http://127.0.0.1:{ENV.get('FRONTEND_HOST_PORT', '8080')}")
API = f"{BASE}/api/v1"
KAFKA = f"127.0.0.1:{ENV.get('KAFKA_HOST_PORT', '9092')}"
PG_URL = (f"postgresql+psycopg://{ENV.get('POSTGRES_USER', 'fraudstream')}:{ENV.get('POSTGRES_PASSWORD', '')}"
          f"@127.0.0.1:{ENV.get('POSTGRES_HOST_PORT', '5433')}/{ENV.get('POSTGRES_DB', 'fraudstream')}")
SPARK_CKPT = ENV.get("SPARK_CHECKPOINT_DIR", "/checkpoints/v2")
REDIS_URL = f"redis://:{ENV.get('REDIS_PASSWORD', '')}@127.0.0.1:{ENV.get('REDIS_HOST_PORT', '6380')}/0"


# ------------------------------------------------------------------ docker helpers
def compose(*args: str, check: bool = True, timeout: int = 300) -> subprocess.CompletedProcess:
    files = [x for f in COMPOSE_FILES for x in ("-f", f)]
    return subprocess.run(["docker", "compose", "-p", PROJECT, "--env-file", str(ENV_FILE), *files,
                           "--profile", "distributed", *args],
                          cwd=ROOT, capture_output=True, text=True, check=check, timeout=timeout)


def docker(*args: str, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], cwd=ROOT, capture_output=True, text=True, check=check, timeout=timeout)


def cid(service: str) -> str:
    c = compose("ps", "-a", "-q", service).stdout.strip()
    assert c, f"no container for service {service}"
    return c.splitlines()[0]


def state(service: str) -> dict[str, Any]:
    return json.loads(docker("inspect", "-f", "{{json .State}}", cid(service)).stdout)


def restart_count(service: str) -> int:
    return int(docker("inspect", "-f", "{{.RestartCount}}", cid(service)).stdout.strip())


def health(service: str) -> str:
    st = state(service)
    return (st.get("Health") or {}).get("Status") or st.get("Status", "unknown")


def wait_until(cond: Callable[[], Any], timeout: float, interval: float = 2.0, what: str = "") -> Any:
    end = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < end:
        try:
            last = cond()
        except Exception as exc:  # noqa: BLE001 - keep polling through transient failures
            last = exc
        if last and not isinstance(last, Exception):
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {what or cond}: last={last!r}")


def wait_healthy(services: tuple[str, ...], timeout: float = 240) -> None:
    for s in services:
        wait_until(lambda s=s: health(s) == "healthy", timeout, 3, f"{s} healthy")


CRASH = ("import os,signal\n"
         "for p in os.listdir('/proc'):\n"
         "    if p.isdigit() and int(p) != os.getpid():\n"
         "        try: st = open(f'/proc/{p}/stat').read()\n"
         "        except OSError: continue\n"
         "        if int(st.rsplit(')', 1)[1].split()[1]) == 1: os.kill(int(p), signal.SIGKILL)\n")


def crash(service: str) -> None:
    """Simulate a process crash: SIGKILL the main process (child of tini). Unlike `docker kill`
    (a manual stop, which Docker's restart policy deliberately ignores) this exercises the
    `restart: unless-stopped` policy exactly as a real crash would."""
    docker("exec", cid(service), "python", "-c", CRASH, check=False)


# ------------------------------------------------------------------ data helpers
def make_events(n: int, seed: int) -> list[Any]:
    from simulator.generator import SimulationConfig, TransactionGenerator

    tag = f"CT{secrets.token_hex(3)}-"
    gen = TransactionGenerator(SimulationConfig(mode="mixed", tps=40, max_events=n, duration_seconds=None,
                                                customer_count=200, suspicious_pct=8, seed=seed,
                                                population_seed=seed * 7), datetime.now(timezone.utc))
    return [e.model_copy(update={"customer_id": tag + e.customer_id, "account_id": tag + e.account_id})
            for e in gen.events()]


def produce(events: list[Any]) -> None:
    from app.services.simulation import KafkaSink
    from streaming.kafka.client import Topics, make_producer

    sink = KafkaSink(make_producer(KAFKA, "container-it"), Topics("").raw)
    for ev in events:
        sink.send(ev)
    assert sink.flush(60) == 0 and sink.failed == 0, sink.last_error


_engine: Any = None


def db() -> Any:
    global _engine
    if _engine is None:
        from sqlalchemy import create_engine

        _engine = create_engine(PG_URL, pool_pre_ping=True, connect_args={"connect_timeout": 3})
    return _engine


def persisted(ids: list[str]) -> dict[str, int]:
    """event_id -> number of transaction rows (must be exactly 1 each)."""
    from sqlalchemy import text

    with db().connect() as c:
        rows = c.execute(text("SELECT event_id, count(*) FROM transactions WHERE event_id = ANY(:ids) GROUP BY event_id"),
                         {"ids": ids}).all()
    return {r[0]: r[1] for r in rows}


def assert_exactly_once(ids: list[str], timeout: float = 240) -> None:
    got = wait_until(lambda: (lambda p: p if len(p) == len(ids) else None)(persisted(ids)), timeout, 3,
                     f"{len(ids)} events persisted")
    dupes = {k: v for k, v in got.items() if v != 1}
    assert not dupes, f"duplicate transaction rows: {dupes}"
    from sqlalchemy import text

    with db().connect() as c:
        n_pred, n_pred_distinct = c.execute(text(
            "SELECT count(*), count(DISTINCT p.transaction_id) FROM predictions p JOIN transactions t "
            "ON t.id = p.transaction_id WHERE t.event_id = ANY(:ids)"), {"ids": ids}).one()
    assert n_pred == n_pred_distinct == len(ids)


# ------------------------------------------------------------------ fixtures
@pytest.fixture(autouse=True)
def _no_failure_injection_on_live(request: Any) -> None:
    if request.node.get_closest_marker("disruptive") and PROJECT == "fraudstream"             and os.environ.get("FS_ALLOW_DISRUPTIVE_ON_LIVE") != "1":
        pytest.skip("NOT EXECUTED: disruptive test against the LIVE project 'fraudstream' - run it on the isolated "
                    "stack (FS_COMPOSE_PROJECT/FS_ENV_FILE/FS_COMPOSE_FILES) or set FS_ALLOW_DISRUPTIVE_ON_LIVE=1")


@pytest.fixture(scope="session")
def stack() -> None:
    import httpx

    try:
        r = httpx.get(f"{API}/health", timeout=5)
    except httpx.HTTPError as exc:
        pytest.skip(f"NOT EXECUTED: container stack not reachable at {BASE} ({exc.__class__.__name__}); "
                    "start it with: docker compose --env-file .env --profile distributed up -d --build")
    if r.status_code != 200 or r.json().get("profile") != "distributed":
        pytest.skip(f"NOT EXECUTED: {BASE} is not the distributed stack ({r.status_code} {r.text[:200]})")
    wait_healthy(INFRA_SERVICES + APP_SERVICES)


@pytest.fixture(scope="session")
def client(stack: None) -> Iterator[Any]:
    import httpx

    user, pw = ENV.get("FRAUDSTREAM_ADMIN_USERNAME", "admin"), ENV.get("FRAUDSTREAM_ADMIN_PASSWORD")
    if not pw:
        pytest.skip("NOT EXECUTED: FRAUDSTREAM_ADMIN_PASSWORD not set (.env)")
    c = httpx.Client(base_url=API, timeout=30)
    r = c.post("/auth/login", json={"username": user, "password": pw})
    assert r.status_code == 200, r.text
    c.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    yield c
    c.close()


def processor_reported() -> str:
    import httpx

    return httpx.get(f"{API}/health", timeout=5).json().get("stream_processor", "")


# ================================================================== non-disruptive
def test_services_healthy_and_one_shot_jobs_completed(stack):
    for s in INFRA_SERVICES + APP_SERVICES:
        assert health(s) == "healthy", f"{s}: {health(s)}"
    for one_shot in ("kafka-init", "app-init"):
        st = state(one_shot)
        assert st["Status"] == "exited" and st["ExitCode"] == 0, f"{one_shot}: {st}"
    # restart policy on every long-running service
    for s in INFRA_SERVICES + APP_SERVICES:
        pol = docker("inspect", "-f", "{{.HostConfig.RestartPolicy.Name}}", cid(s)).stdout.strip()
        assert pol == "unless-stopped", f"{s} restart policy {pol}"


def test_isolation_secrets_and_persistent_volumes(stack):
    # API reachable only through nginx (not published on the host)
    assert compose("port", "api", "8000", check=False).stdout.strip() in ("", ":0")
    # no secret is baked into an image; they arrive via the container environment only
    for image in ("fraudstream-app:local", "fraudstream-frontend:local", "fraudstream-spark:local"):
        env = json.loads(docker("image", "inspect", "-f", "{{json .Config.Env}}", image).stdout)
        assert not [e for e in env if any(k in e for k in ("PASSWORD", "SECRET", "JWT"))], image
    # application containers run as non-root users
    for s in ("api", "persistence-consumer", "spark", "frontend"):
        uid = docker("exec", cid(s), "id", "-u").stdout.strip()
        assert uid != "0", f"{s} runs as root"
    # model artifacts are mounted read-only by default; state lives in named volumes
    mounts = {m["Destination"]: m for m in json.loads(docker("inspect", "-f", "{{json .Mounts}}", cid("api")).stdout)}
    if ENV.get("ARTIFACTS_MOUNT_MODE", "ro") == "ro":
        assert mounts["/app/ml/artifacts"]["RW"] is False
    # writable state volumes are usable by the non-root app user
    for dest in ("/data/uploads", "/state"):
        r = docker("exec", cid("api"), "sh", "-c", f"touch {dest}/.write-probe && rm {dest}/.write-probe", check=False)
        assert r.returncode == 0, f"{dest} not writable by the app user: {r.stderr}"
    for s, dest in (("postgres", "/var/lib/postgresql/data"), ("kafka", "/var/lib/kafka/data"),
                    ("redis", "/data"), ("spark", "/checkpoints")):
        m = {x["Destination"]: x for x in json.loads(docker("inspect", "-f", "{{json .Mounts}}", cid(s)).stdout)}
        assert m[dest]["Type"] == "volume", f"{s}:{dest} is not a named volume"


def test_dashboard_status_reflects_actual_processor_and_infrastructure(client):
    h = client.get("/health").json()
    assert h["status"] == "ok" and h["profile"] == "distributed" and h["database"] == "up" and h["model_loaded"]
    assert h["stream_processor"] == SPARK
    s = client.get("/system/status").json()
    assert s["profile"] == "distributed" and s["stream_processor"] == SPARK
    assert s["database"] == "postgresql" and s["redis"] == "up" and s["live_broker"] == "redis"
    assert s["runtime_config_publish"].get("redis") == "ok"
    infra = client.get("/infrastructure/status").json()
    assert infra["database"]["status"] == "UP" and infra["database"]["dialect"] == "postgresql"
    assert infra["redis"]["status"] == "UP"
    assert infra["kafka"]["status"] == "UP" and infra["kafka"]["brokers"] == 1
    assert all(t["exists"] for t in infra["kafka"]["topics"])
    assert infra["spark"]["status"] == "RUNNING"
    assert infra["spark"]["master"].startswith("local["), "Spark is expected in LOCAL mode inside one container"
    assert {q["name"] for q in infra["spark"]["queries"]} >= {"score", "validated-dedup"}
    assert infra["consumer"]["status"] == "UP"
    assert infra["processor"]["status"] != "UP", "fallback python processor must not be running alongside Spark"
    pers = next(g for g in infra["kafka"]["consumer_groups"] if g["group"] == "fraudstream-persistence")
    assert pers["active"], pers


def test_every_dashboard_page_and_endpoint_is_served_by_the_backend(client):
    import httpx

    # SPA routes are served by nginx (index.html), deep links included
    for path in ("/", "/login", "/live", "/investigations", "/simulation", "/streaming", "/models", "/models?tab=drift",
                 "/risk", "/risk?period=60&category=retail", "/map", "/map?risk=high&country=GB",
                 "/alerts/x", "/investigations/x", "/reports", "/reports?report=x", "/settings", "/settings?tab=scoring"):
        r = httpx.get(f"{BASE}{path}", timeout=10)
        assert r.status_code == 200 and '<div id="root">' in r.text, path
    # every GET endpoint the pages call (frontend/src) returns real backend JSON through the proxy
    endpoints = ["/health", "/system/status", "/analytics/overview", "/analytics/risk-distribution",
                 "/analytics/timeline?minutes=30&bucket_seconds=30", "/alerts?size=8",
                 "/alerts?page=1&size=20&order=desc", "/investigations?page=1&size=20", "/simulation/status",
                 "/simulation/runs?limit=10", "/risk/settings", "/infrastructure/status", "/auth/me",
                 # Phase 11A: Streaming Infrastructure + Model Intelligence pages
                 "/models", "/models?kind=anomaly", "/models/active", "/models/compare", "/models/experiments",
                 "/models/artifacts", "/models/live-inference?limit=5000", "/drift/history?limit=100", "/datasets",
                 # Phase 11B: Risk Analytics + Fraud Intelligence Map
                 "/analytics/patterns?minutes=1440", "/analytics/confusion?minutes=1440", "/analytics/hourly?minutes=1440",
                 "/analytics/merchants?minutes=1440&limit=15", "/analytics/merchants?category=retail&limit=15",
                 "/analytics/velocity?minutes=60&limit=15", "/analytics/geo?minutes=1440&min_risk=40", "/risk/rules",
                 # Phase 11C: Reports + Settings
                 "/reports/types", "/reports/generated?page=1&size=10", "/settings/drift", "/settings/preferences",
                 "/settings/simulation_defaults", "/users", "/audit?entity_type=risk_rule,system_setting&page=1&size=25"]
    for ep in endpoints:
        r = client.get(ep)
        assert r.status_code == 200, f"{ep}: {r.status_code} {r.text[:200]}"
        assert r.headers.get("x-request-id"), f"{ep}: response not produced by the FastAPI app"
        r.json()
    r = client.get("/reports/alerts.csv")
    assert r.status_code == 200 and "text/csv" in r.headers["content-type"]
    # unauthenticated requests are rejected by the backend (not by nginx)
    r = httpx.get(f"{API}/alerts", timeout=10)
    assert r.status_code == 401 and r.headers.get("x-request-id")


def test_transaction_to_investigation_workflow_through_containers(client):
    """Simulation Lab inject (API container -> Kafka) -> Spark scoring -> Kafka -> persistence
    consumer -> PostgreSQL -> SSE live feed via nginx -> alert -> investigation -> audit."""
    import httpx

    live: list[dict] = []
    stop = threading.Event()

    errors: list[str] = []

    def listen() -> None:
        try:
            _listen()
        except Exception as exc:  # noqa: BLE001 - surfaced in the assertion message
            errors.append(repr(exc))

    def _listen() -> None:
        with httpx.stream("GET", f"{API}/stream/transactions", headers={"Authorization": client.headers["Authorization"]},
                          timeout=httpx.Timeout(10, read=30)) as r:
            assert r.status_code == 200
            for line in r.iter_lines():
                if stop.is_set():
                    break
                if line.startswith("data:"):
                    live.append(json.loads(line[5:]))

    t = threading.Thread(target=listen, daemon=True)
    t.start()
    wait_until(lambda: any(m.get("type") == "status" for m in live), 20, 0.5, "SSE connected through nginx")

    r = client.post("/simulation/inject", json={"scenario": "account_takeover", "seed": secrets.randbelow(10**6)})
    assert r.status_code == 200, r.text
    injected = r.json()["events"]
    assert injected and all(e["status"] == "published_to_kafka" for e in injected)
    ids = [e["event_id"] for e in injected]
    assert_exactly_once(ids, timeout=120)

    tx_ids = {e["transaction_id"] for e in injected}
    wait_until(lambda: tx_ids & {m.get("transaction_id") for m in live}, 30, 1,
               f"injected transactions on the SSE live feed (thread alive={t.is_alive()}, "
               f"received={[(m.get('type'), m.get('transaction_id')) for m in live][-5:]}, errors={errors})")
    stop.set()

    from sqlalchemy import text

    with db().connect() as c:
        rows = c.execute(text("SELECT a.alert_uid, t.customer_id FROM alerts a JOIN transactions t ON t.id = a.transaction_id "
                              "WHERE t.event_id = ANY(:ids) ORDER BY a.risk_score DESC"), {"ids": ids}).all()
    assert rows, "the injected account-takeover pattern produced no alert"
    uid, customer = rows[0]
    assert client.get(f"/alerts?customer_id={customer}").json()["total"] >= 1

    a = client.get(f"/alerts/{uid}").json()
    assert a["alert_uid"] == uid and a["status"] == "NEW"
    assert client.patch(f"/alerts/{uid}/status", json={"status": "UNDER_REVIEW"}).status_code == 200
    assert client.post(f"/alerts/{uid}/notes", json={"body": "container IT: reviewed"}).status_code == 201
    inv = client.post("/investigations", json={"alert_uids": [uid], "title": f"Container IT case {uid[:8]}"})
    assert inv.status_code == 201, inv.text
    case = inv.json()["case_uid"]
    for body in ({"status": "IN_PROGRESS"},
                 {"status": "RESOLVED", "final_classification": "CONFIRMED_SUSPICIOUS",
                  "findings": "Synthetic account-takeover pattern verified through the container stack."},
                 {"status": "CLOSED"}):
        r = client.patch(f"/investigations/{case}/status", json=body)
        assert r.status_code == 200, r.text
    full = client.get(f"/investigations/{case}").json()
    assert full["status"] == "CLOSED" and full["final_classification"] == "CONFIRMED_SUSPICIOUS"
    actions = {x["action"] for x in full["audit"]}
    assert {"INVESTIGATION_OPENED", "INVESTIGATION_STATUS_CHANGED", "INVESTIGATION_FINDINGS"} <= actions
    alert = client.get(f"/alerts/{uid}").json()
    assert alert["status"] == "CONFIRMED_SUSPICIOUS"
    assert {"ALERT_STATUS_CHANGED", "NOTE_ADDED"} <= {x["action"] for x in alert["audit"]}


def test_model_intelligence_reports_actual_artifacts_read_only(client):
    """Model Intelligence page data: the artifact listing served by the API container is the
    bind-mounted ml/artifacts (byte-identical SHA-256 to the host files), read-only, and model
    changes are refused with 409 before anything is modified."""
    import hashlib

    art = client.get("/models/artifacts").json()
    assert art["artifacts_dir_writable"] is False and client.get("/models/active").json()["artifacts_writable"] is False
    # the directory the stack actually mounts (Phase 12: an isolated stack may mount a restored copy)
    art_dir = Path(ENV["ARTIFACTS_HOST_DIR"]) if ENV.get("ARTIFACTS_HOST_DIR") else ROOT / "ml" / "artifacts"
    host = {p.relative_to(art_dir).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in art_dir.rglob("*") if p.is_file()}
    served = {f["path"]: f["sha256"] for f in art["files"]}
    assert served == host
    assert art["registry_active"] == art["database_active"] and art["database_active"].get("supervised")
    cand = next(m for m in client.get("/models?kind=supervised").json() if m["status"] != "ACTIVE")
    r = client.post(f"/models/{cand['id']}/activate", json={"reason": "container read-only check"})
    assert r.status_code == 409 and "read-only" in r.json()["detail"]
    assert client.get("/models/active").json()["active"]["supervised"]["status"] == "ACTIVE"
    assert {f["path"]: f["sha256"] for f in client.get("/models/artifacts").json()["files"]} == host   # unchanged
    exp = client.get("/models/experiments").json()[0]
    detail = client.get(f"/models/experiments/{exp['id']}").json()
    assert detail["summary"]["dataset"]["rows"] > 0 and detail["model_versions"]
    live = client.get("/models/live-inference").json()
    assert live["models"] and all(m["p50_ms"] <= m["p95_ms"] for m in live["models"])


def test_streaming_page_reports_spark_checkpoints(client):
    infra = client.get("/infrastructure/status").json()
    assert infra["spark"]["status"] == "RUNNING" and infra["spark"]["checkpoint_root"] == SPARK_CKPT
    for q in infra["spark"]["queries"]:
        cp = q["checkpoint"]
        assert cp["location"] == f"{SPARK_CKPT}/{q['name']}" and cp["exists"], cp
        assert cp["last_committed_batch"] is not None and cp["uncommitted_batches"] in (0, 1), cp
    assert infra["kafka"]["dlq_messages"] is not None and infra["dead_letters_db"] >= 0


def test_sse_live_feed_stays_open_through_idle_periods_via_nginx(client):
    """Regression: the live feed used to END after the first 15 s keep-alive on a quiet stack
    (reproduced: stream ended at 15.1 s), which made the Live Stream page reconnect and drop
    messages, and caused the intermittent SSE failure of the workflow test."""
    import httpx

    frames: list[str] = []
    t0 = time.monotonic()
    ended_early = None
    with httpx.stream("GET", f"{API}/stream/transactions", headers={"Authorization": client.headers["Authorization"]},
                      timeout=httpx.Timeout(10, read=40)) as r:
        assert r.status_code == 200
        for line in r.iter_lines():
            if line:
                frames.append(line)
            if time.monotonic() - t0 > 35:
                break
        else:
            ended_early = round(time.monotonic() - t0, 1)
    assert ended_early is None, f"server ended the SSE stream after {ended_early}s: {frames[-5:]}"
    assert frames and '"live": "connected"' in frames[1]
    # >= 35 s open: either traffic flowed or keep-alives were sent (every 15 s when idle)
    assert any(f.startswith(": keep-alive") for f in frames) or sum(f.startswith("data:") for f in frames) > 1


def test_risk_analytics_windows_match_postgresql(client):
    """The time-window / category filters (Phase 11B) on the real PostgreSQL dialect: endpoint
    aggregates equal direct SQL over the same window (unit tests only cover SQLite)."""
    from sqlalchemy import text

    for minutes in (60, 1440, None):
        q = f"?minutes={minutes}" if minutes else ""
        where = "AND t.processed_at >= now() - make_interval(mins => :m)" if minutes else ""
        with db().connect() as c:
            n, f = c.execute(text(f"SELECT count(*), coalesce(sum(CASE WHEN p.risk_category <> 'LOW' THEN 1 ELSE 0 END), 0) "
                                  f"FROM transactions t JOIN predictions p ON p.transaction_id = t.id WHERE true {where}"),
                             {"m": minutes or 0}).one()
        # rows can arrive between the SQL and the API call while traffic flows: allow only growth
        m = client.get(f"/analytics/merchants{q}").json()
        h = client.get(f"/analytics/hourly{q}").json()
        cnt, flg = sum(x["count"] for x in m["categories"]), sum(x["flagged"] for x in m["categories"])
        assert n <= cnt <= n + 200 and f <= flg <= f + 200, (minutes, n, cnt, f, flg)
        assert len(h["hours"]) == 24 and n <= sum(x["count"] for x in h["hours"]) <= n + 200
        assert m["period"]["window_minutes"] == minutes
    allm = client.get("/analytics/merchants?limit=50").json()
    cat = allm["categories"][0]["category"]
    one = client.get(f"/analytics/merchants?category={cat}&limit=50").json()
    assert [c["category"] for c in one["categories"]] == [cat] and all(x["category"] == cat for x in one["top_flagged_merchants"])
    keys = [(-x["flagged"], -x["count"], x["merchant_id"]) for x in allm["top_flagged_merchants"]]
    assert keys == sorted(keys)
    geo = client.get("/analytics/geo?minutes=10080").json()
    assert geo["available"] and all(p["simulated"] for p in geo["points"]) and "SIMULATED" in geo["label"]
    from simulator.geography import CITIES

    centroids = {(c[0], c[1]): (c[2], c[3]) for c in CITIES}
    for p in geo["points"]:   # points are the simulator's public city centroids, never invented coordinates
        lat, lon = centroids[(p["city"], p["country"])]
        assert abs(p["lat"] - lat) < 1e-6 and abs(p["lon"] - lon) < 1e-6, p


def test_reports_match_postgresql_and_export_through_nginx(client):
    """Phase 11C reports on the real PostgreSQL dialect (unit tests run on SQLite): summary values equal
    direct SQL over the same bounds; per-day / per-hour / score-bin breakdowns add up; CSV and PDF are
    served through nginx with safe file names; the report is persisted in generated_reports."""
    from datetime import timedelta

    from sqlalchemy import text

    now = datetime.now(timezone.utc)
    start, end = now - timedelta(days=7), now + timedelta(minutes=1)
    span = {"s": start, "e": end}
    with db().connect() as c:
        n, f = c.execute(text("SELECT count(*), coalesce(sum(CASE WHEN p.risk_category <> 'LOW' THEN 1 ELSE 0 END), 0) "
                              "FROM transactions t JOIN predictions p ON p.transaction_id = t.id "
                              "WHERE t.processed_at >= :s AND t.processed_at < :e"), span).one()
        alerts = c.execute(text("SELECT count(*) FROM alerts WHERE created_at >= :s AND created_at < :e"), span).scalar()
        cases = c.execute(text("SELECT count(*) FROM investigations WHERE created_at >= :s AND created_at < :e"), span).scalar()

    def gen(kind: str, **filters: Any) -> dict[str, Any]:
        r = client.post("/reports", json={"report_type": kind, "start": start.isoformat(), "end": end.isoformat(), **filters})
        assert r.status_code == 201 and r.json()["status"] == "COMPLETED", r.text[:300]
        return r.json()

    def summ(b: dict[str, Any]) -> dict[str, Any]:
        return {m["key"]: m["value"] for m in b["content"]["summary"]}

    def sec(b: dict[str, Any], sid: str) -> dict[str, Any]:
        return next(x for x in b["content"]["sections"] if x["id"] == sid)

    fa = gen("fraud_activity")
    # rows can arrive between the SQL and the API call while traffic flows: allow only growth
    assert n <= summ(fa)["transactions_scored"] <= n + 200 and f <= summ(fa)["flagged"] <= f + 200
    assert sum(r["transactions"] for r in sec(fa, "daily")["rows"]) == summ(fa)["transactions_scored"]
    assert all(len(r["day"]) == 10 for r in sec(fa, "daily")["rows"])
    ra = gen("risk_analytics")
    assert sum(r["transactions"] for r in sec(ra, "score_distribution")["rows"]) == summ(ra)["transactions_scored"]
    assert sum(r["transactions"] for r in sec(ra, "hourly")["rows"]) == summ(ra)["transactions_scored"]
    al = gen("alert_summary", risk_level="HIGH")
    assert all(r["risk_category"] == "HIGH" for r in sec(al, "alerts")["rows"])
    assert alerts <= summ(gen("alert_summary"))["alerts"] <= alerts + 50
    assert summ(gen("investigation"))["cases"] == cases
    assert summ(gen("model_performance"))["active_model"]
    sa = gen("system_activity")
    for fmt_, ctype, magic in (("csv", "text/csv", b"\xef\xbb\xbf"), ("pdf", "application/pdf", b"%PDF-")):
        for uid in (fa["report_uid"], sa["report_uid"]):
            r = client.get(f"/reports/generated/{uid}/download?format={fmt_}")
            assert r.status_code == 200 and r.headers["content-type"].startswith(ctype) and r.content.startswith(magic)
            assert r.headers.get("x-request-id")                               # produced by the API, not nginx
            disp = r.headers["content-disposition"]
            assert disp.startswith('attachment; filename="fraudstream-') and "/" not in disp and "\\" not in disp
            if fmt_ == "pdf":
                assert b"/FontFile2" in r.content and b"DejaVuSans" in r.content   # image font embedded (Unicode)
    with db().connect() as c:
        row = c.execute(text("SELECT status, report_type, generated_by_name FROM generated_reports WHERE report_uid = :u"),
                        {"u": fa["report_uid"]}).one()
        assert tuple(row) == ("COMPLETED", "fraud_activity", ENV.get("FRAUDSTREAM_ADMIN_USERNAME", "admin"))
        assert c.execute(text("SELECT version_num FROM alembic_version")).scalar() == "0002"


def test_analyst_cannot_change_configuration_and_the_attempt_is_audited(client):
    import httpx

    user, pw = ENV.get("FRAUDSTREAM_ANALYST_USERNAME", "analyst"), ENV.get("FRAUDSTREAM_ANALYST_PASSWORD")
    if not pw:
        pytest.skip("NOT EXECUTED: FRAUDSTREAM_ANALYST_PASSWORD not set (.env)")
    with httpx.Client(base_url=API, timeout=30) as a:
        tok = a.post("/auth/login", json={"username": user, "password": pw}).json()["access_token"]
        a.headers["Authorization"] = f"Bearer {tok}"
        before = client.get("/risk/settings").json()
        r = a.put("/risk/settings", json=before["settings"])
        assert r.status_code == 403 and r.headers.get("x-request-id")
        assert a.patch("/risk/rules/NEW_LOCATION", json={"enabled": False}).status_code == 403
        assert a.post("/reports", json={"report_type": "system_activity", "start": "2026-01-01T00:00:00Z",
                                        "end": "2026-01-02T00:00:00Z"}).status_code == 403
        assert a.get("/audit").status_code == 403
    assert client.get("/risk/settings").json()["settings"] == before["settings"]
    assert next(x for x in client.get("/risk/rules").json() if x["code"] == "NEW_LOCATION")["enabled"] is True
    items = client.get("/audit?entity_type=risk_rule,system_setting&size=10").json()["items"]
    assert {(i["action"], i["actor"]) for i in items[:2]} == {("CONFIG_CHANGE_DENIED", user)}


# ================================================================== disruptive (restart / outage)
@pytest.mark.disruptive
def test_api_restart_and_recreate_recovers_behind_nginx(client):
    compose("restart", "api")
    wait_healthy(("api",), 120)
    assert client.get("/system/status").status_code == 200          # same JWT secret: token still valid
    old_ip = docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", cid("api")).stdout
    compose("up", "-d", "--no-deps", "--force-recreate", "api")       # new container (possibly new IP)
    wait_healthy(("api",), 120)
    new_ip = docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", cid("api")).stdout
    r = wait_until(lambda: (lambda r: r if r.status_code == 200 else None)(client.get("/health")), 30, 1,
                   "nginx re-resolves the recreated API")
    assert r.json()["stream_processor"] == SPARK
    print(f"api ip {old_ip.strip()} -> {new_ip.strip()}")


@pytest.mark.disruptive
def test_nginx_follows_recreated_api_to_a_different_ip(client):
    """Docker usually hands a recreated container its old IP again, so the test above may never
    exercise re-resolution. Here the old IP is occupied by a placeholder container while the API is
    recreated, which FORCES a new address; the frontend (nginx) container is not restarted."""
    fmt_ip = "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"
    network = docker("inspect", "-f", "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}", cid("api")).stdout.strip()
    old_ip = docker("inspect", "-f", fmt_ip, cid("api")).stdout.strip()
    nginx_started = docker("inspect", "-f", "{{.State.StartedAt}}", cid("frontend")).stdout.strip()
    holder = f"fs-ct-ipholder-{secrets.token_hex(3)}"
    compose("stop", "api")
    try:
        docker("run", "-d", "--rm", "--name", holder, "--network", network, "--ip", old_ip,
               "--entrypoint", "sleep", "redis:7-alpine", "600")
        compose("up", "-d", "--no-deps", "--force-recreate", "api")
        wait_healthy(("api",), 180)
        new_ip = docker("inspect", "-f", fmt_ip, cid("api")).stdout.strip()
        assert new_ip and new_ip != old_ip, f"API kept {old_ip}"
        r = wait_until(lambda: (lambda r: r if r.status_code == 200 else None)(client.get("/health")), 30, 1,
                       f"nginx re-resolves the API at its new IP {new_ip}")
        assert r.headers.get("x-request-id") and r.json()["stream_processor"] == SPARK
        for _ in range(5):                       # stable, not a lucky single request
            assert client.get("/models/active").status_code == 200
        assert docker("inspect", "-f", "{{.State.StartedAt}}", cid("frontend")).stdout.strip() == nginx_started
        print(f"api ip {old_ip} -> {new_ip} (old IP held by {holder}); nginx not restarted")
    finally:
        docker("rm", "-f", holder, check=False)
        if health("api") != "healthy":
            compose("up", "-d", "--no-deps", "api", check=False)
            wait_healthy(("api",), 180)


@pytest.mark.disruptive
def test_persistence_consumer_crash_is_restarted_without_loss_or_duplicates(client):
    before = restart_count("persistence-consumer")
    first, second = make_events(150, 101), make_events(150, 102)
    produce(first)
    crash("persistence-consumer")
    produce(second)                                         # produced while the consumer is down/restarting
    wait_until(lambda: restart_count("persistence-consumer") > before, 60, 1, "restart policy restarted the consumer")
    wait_healthy(("persistence-consumer",), 90)
    assert_exactly_once([e.event_id for e in first + second])


@pytest.mark.disruptive
def test_spark_crash_recovers_from_checkpoint_without_silent_fallback(client):
    """A crash is restarted by Docker within seconds; the dashboard may keep showing the last
    fresh Spark snapshot for up to the 30 s freshness window (documented limitation), so this
    test asserts the status comes from the NEW Spark application and never names a fallback."""
    app_before = client.get("/infrastructure/status").json()["spark"]["app_id"]
    before = restart_count("spark")
    produce(make_events(50, 201))
    crash("spark")
    during = make_events(200, 202)
    produce(during)
    seen: set[str] = set()

    def recovered() -> bool:
        seen.add(processor_reported())
        return restart_count("spark") > before and health("spark") == "healthy"
    wait_until(recovered, 300, 3, "spark restarted by policy and healthy")
    seen.add(processor_reported())
    assert not any("python-kafka" in p or p == "in-process" for p in seen), f"silent fallback: {seen}"
    assert_exactly_once([e.event_id for e in during])
    assert processor_reported() == SPARK
    spark = wait_until(lambda: (lambda sp: sp if sp.get("app_id") != app_before and sp["status"] == "RUNNING" else None)(
        client.get("/infrastructure/status").json()["spark"]), 60, 3, "status reported by the restarted Spark app")
    print(f"spark app {app_before} -> {spark['app_id']}; processor values seen during recovery: {seen}")


@pytest.mark.disruptive
def test_spark_stopped_is_reported_not_running_and_resumes(client):
    """A real (non-blip) Spark outage must be visible on the dashboard as NOT RUNNING - with no
    fallback - and events produced meanwhile are processed after restart (checkpoint)."""
    compose("stop", "spark", timeout=180)                     # graceful: job publishes a terminated snapshot
    try:
        wait_until(lambda: processor_reported().startswith("NOT RUNNING"), 40, 2, "dashboard reports NOT RUNNING")
        infra = client.get("/infrastructure/status").json()
        assert infra["spark"]["status"] in ("STOPPED", "STALE", "NOT_REPORTING")
        assert infra["profile"] == "distributed" and infra["stream_processor"].startswith("NOT RUNNING")
        events = make_events(150, 251)
        produce(events)
        time.sleep(25)
        assert processor_reported().startswith("NOT RUNNING"), "status must stay NOT RUNNING while Spark is down"
        assert not persisted([e.event_id for e in events]), "events were processed without Spark"
    finally:
        compose("start", "spark", timeout=180)
    wait_healthy(("spark",), 300)
    assert_exactly_once([e.event_id for e in events])
    assert processor_reported() == SPARK


@pytest.mark.disruptive
def test_kafka_outage_fails_explicitly_and_recovers(client):
    compose("stop", "kafka", timeout=180)
    try:
        r = client.post("/simulation/inject", json={"scenario": "large_amount"})
        assert r.status_code == 503, r.text                    # explicit failure - no local fallback
        assert "kafka" in r.json()["detail"].lower()
        infra = client.get("/infrastructure/status").json()
        assert infra["kafka"]["status"] == "DOWN" and infra["profile"] == "distributed"
        h = client.get("/health").json()
        assert h["profile"] == "distributed" and h["stream_processor"] != "in-process"
        time.sleep(20)                                          # a real outage, not a blip
    finally:
        compose("start", "kafka", timeout=180)
    wait_healthy(("kafka",), 180)
    wait_healthy(("spark", "persistence-consumer", "api"), 300)
    r = wait_until(lambda: (lambda r: r if r.status_code == 200 else None)(
        client.post("/simulation/inject", json={"scenario": "large_amount"})), 90, 5, "inject works again")
    events = make_events(150, 301)
    produce(events)
    assert_exactly_once([e.event_id for e in events] + [e["event_id"] for e in r.json()["events"]])
    assert processor_reported() == SPARK


@pytest.mark.disruptive
def test_postgres_outage_blocks_persistence_then_recovers_without_loss(client):
    import httpx
    import redis

    rc_consumer = restart_count("persistence-consumer")
    r_client = redis.Redis.from_url(REDIS_URL, socket_timeout=3)
    retries_before = json.loads(r_client.get("fraudstream:consumer:heartbeat"))["stats"]["db_retries"]
    first = make_events(100, 401)
    produce(first)
    assert_exactly_once([e.event_id for e in first])
    compose("stop", "postgres", timeout=120)
    try:
        during = make_events(200, 402)
        produce(during)                                         # Spark keeps scoring; consumer must block, not drop
        h = wait_until(lambda: (lambda r: r if r.status_code == 503 else None)(httpx.get(f"{API}/health", timeout=10)),
                       60, 2, "API reports the database outage")
        assert h.json()["database"] == "down" and h.json()["profile"] == "distributed"
        wait_until(lambda: json.loads(r_client.get("fraudstream:consumer:heartbeat"))["stats"]["db_retries"] > retries_before,
                   90, 3, "consumer retrying against the unavailable database")
        time.sleep(15)
        assert state("persistence-consumer")["Status"] == "running"
        assert health("persistence-consumer") == "healthy"      # alive and waiting, not crash-looping
    finally:
        compose("start", "postgres", timeout=120)
    wait_healthy(("postgres", "api"), 180)
    assert_exactly_once([e.event_id for e in during])
    assert restart_count("persistence-consumer") == rc_consumer


@pytest.mark.disruptive
def test_full_stack_stop_start_preserves_data_and_resumes(client):
    import httpx
    from sqlalchemy import text

    with db().connect() as c:
        tx_before, alerts_before, users_before = c.execute(text(
            "SELECT (SELECT count(*) FROM transactions), (SELECT count(*) FROM alerts), (SELECT count(*) FROM users)")).one()
    commits_before = max(int(x) for x in docker("exec", cid("spark"), "ls", f"{SPARK_CKPT}/score/commits").stdout.split()
                         if x.isdigit())
    compose("stop", timeout=300)
    _engine.dispose()
    compose("up", "-d", timeout=600)
    wait_healthy(INFRA_SERVICES + APP_SERVICES, 400)
    with db().connect() as c:
        after = c.execute(text("SELECT (SELECT count(*) FROM transactions), (SELECT count(*) FROM alerts), "
                               "(SELECT count(*) FROM users)")).one()
    assert tuple(after) == (tx_before, alerts_before, users_before)
    user, pw = ENV.get("FRAUDSTREAM_ADMIN_USERNAME", "admin"), ENV["FRAUDSTREAM_ADMIN_PASSWORD"]
    assert httpx.post(f"{API}/auth/login", json={"username": user, "password": pw}, timeout=15).status_code == 200
    events = make_events(100, 501)
    produce(events)
    assert_exactly_once([e.event_id for e in events])
    commits_after = max(int(x) for x in docker("exec", cid("spark"), "ls", f"{SPARK_CKPT}/score/commits").stdout.split()
                        if x.isdigit())
    assert commits_after > commits_before, "Spark did not resume from its checkpoint"
    assert processor_reported() == SPARK


@pytest.mark.disruptive
def test_settings_and_reports_persist_across_api_restart(client):
    """A drift-setting change (affects drift checks only, never scoring) and a generated report survive an
    API restart; the restarted API republishes the DB risk configuration to Redis for Spark. The drift
    value is restored afterwards (the restore is audited too)."""
    import redis as redis_lib

    doc = client.get("/settings/drift").json()
    original = doc["value"]["min_rows"]
    changed = original + 1 if original < doc["value"]["window_rows"] else original - 1
    r = client.put("/settings/drift", json={"value": {"min_rows": changed}, "expected_updated_at": doc["updated_at"]})
    assert r.status_code == 200 and r.json()["changed"], r.text
    rep_ = client.post("/reports", json={"report_type": "investigation", "start": "2026-01-01T00:00:00Z",
                                         "end": datetime.now(timezone.utc).isoformat()}).json()
    try:
        compose("restart", "api")
        wait_healthy(("api",), 120)
        wait_until(lambda: client.get("/health").status_code == 200, 60, 1, "api reachable through nginx")
        after = client.get("/settings/drift").json()
        assert after["value"]["min_rows"] == changed and after["last_change"]["action"] == "SETTING_UPDATED"
        stale = client.put("/settings/drift", json={"value": {"min_rows": original}, "expected_updated_at": doc["updated_at"]})
        assert stale.status_code == 409                                          # the pre-change version is stale now
        again = client.get(f"/reports/generated/{rep_['report_uid']}").json()
        assert again["status"] == "COMPLETED" and again["content"]["summary"] == rep_["content"]["summary"]
        assert client.get(f"/reports/generated/{rep_['report_uid']}/download?format=pdf").status_code == 200
        cfg = json.loads(redis_lib.Redis.from_url(REDIS_URL).get("fraudstream:runtime_config"))
        assert cfg["settings"] == client.get("/risk/settings").json()["settings"]
        assert {x["code"]: x["params"] for x in cfg["rules"]} == \
            {x["code"]: x["params"] for x in client.get("/risk/rules").json()}
    finally:
        cur = client.get("/settings/drift").json()
        client.put("/settings/drift", json={"value": {"min_rows": original}, "expected_updated_at": cur["updated_at"]})
    assert client.get("/settings/drift").json()["value"]["min_rows"] == original


# ================================================================== Phase 12: failure scenarios with timings
def _evidence(name: str, data: dict[str, Any]) -> None:
    """Timings/counts of a recovery scenario -> benchmarks/results/recovery-<name>-<utc>.json (no secrets)."""
    out = ROOT / "benchmarks" / "results"
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (out / f"recovery-{name}-{stamp}.json").write_text(json.dumps({"project": PROJECT, **data}, indent=2, default=str),
                                                       encoding="utf-8")
    print(name, json.dumps(data, default=str))


def _infra(client: Any) -> dict[str, Any]:
    return client.get("/infrastructure/status").json()


def _persistence_lag(client: Any) -> int | None:
    g = [x for x in _infra(client)["kafka"].get("consumer_groups", []) if x["group"] == "fraudstream-persistence"]
    return g[0]["total_lag"] if g else None


def _spark_lag(client: Any) -> int | None:
    q = [x for x in (_infra(client).get("spark") or {}).get("queries", []) if x.get("name") == "score"]
    return q[0].get("lag_vs_raw_topic") if q else None


class _SSE(threading.Thread):
    """Collects event ids from the live feed through nginx."""

    def __init__(self, token: str, seconds: float):
        super().__init__(daemon=True)
        self.token, self.seconds, self.ids, self.status, self.error = token, seconds, set(), None, None

    def run(self) -> None:
        import httpx

        try:
            with httpx.stream("GET", f"{API}/stream/transactions", headers={"Authorization": self.token},
                              timeout=httpx.Timeout(10, read=self.seconds + 20)) as r:
                self.status = r.status_code
                t0 = time.monotonic()
                for line in r.iter_lines():
                    if line.startswith("data:"):
                        d = json.loads(line[5:])
                        eid = (d.get("transaction") or d).get("event_id") if isinstance(d, dict) else None
                        if eid:
                            self.ids.add(eid)
                    if time.monotonic() - t0 > self.seconds:
                        break
        except Exception as exc:  # noqa: BLE001
            self.error = exc.__class__.__name__


@pytest.mark.disruptive
def test_redis_outage_is_reported_and_loses_no_persisted_data(client):
    """Redis holds heartbeats, the runtime-config copy for Spark, the rate-limit counters and the live-feed pub/sub.
    Its loss must be VISIBLE (redis down, processor state unknown - not 'not running'), must not stop persistence
    (Kafka -> Spark -> Kafka -> consumer -> PostgreSQL) and must not change the scoring configuration; after
    restart the live feed and processor reporting recover."""
    import httpx

    t: dict[str, Any] = {}
    compose("stop", "redis", timeout=120)
    t["redis_stopped"] = time.time()
    try:
        st = wait_until(lambda: (lambda s: s if s["redis"] == "down" else None)(client.get("/system/status").json()),
                        30, 2, "API reports Redis down")
        assert st["profile"] == "distributed"
        h = client.get("/health")
        assert h.status_code == 200 and h.json()["database"] == "up"          # API keeps serving PostgreSQL data
        proc = wait_until(lambda: (lambda p: p if not p.startswith("Spark") else None)(processor_reported()), 40, 2,
                          "processor no longer claimed as healthy")
        assert proc.startswith("UNKNOWN"), proc                               # not a false "NOT RUNNING"
        user, pw = ENV.get("FRAUDSTREAM_ADMIN_USERNAME", "admin"), ENV["FRAUDSTREAM_ADMIN_PASSWORD"]
        assert httpx.post(f"{API}/auth/login", json={"username": user, "password": pw}, timeout=20).status_code == 200
        assert client.get("/system/status").json()["rate_limiter"].startswith("in-process")
        during = make_events(200, 601)
        produce(during)
        assert_exactly_once([e.event_id for e in during], 240)                # persistence does not depend on Redis
        t["persisted_during_outage"] = time.time()
        assert state("persistence-consumer")["Status"] == "running" and health("persistence-consumer") == "healthy"
        assert state("spark")["Status"] == "running"
    finally:
        compose("start", "redis", timeout=120)
        t["redis_started"] = time.time()
    wait_healthy(("redis",), 120)
    wait_until(lambda: processor_reported() == SPARK, 90, 2, "Spark reported again")
    t["processor_reported_again"] = time.time()
    sse = _SSE(client.headers["Authorization"], 60)
    sse.start()
    time.sleep(3)
    after = make_events(60, 602)
    produce(after)
    assert_exactly_once([e.event_id for e in after])
    wait_until(lambda: sse.ids & {e.event_id for e in after}, 60, 1, "live feed delivers new events after Redis restart")
    t["live_feed_after_restart"] = time.time()
    sse.join(70)
    _evidence("redis", {"events_during_outage": len(during), "persisted_during_outage": len(during),
                        "seconds_to_persist_during_outage": round(t["persisted_during_outage"] - t["redis_stopped"], 1),
                        "seconds_restart_to_processor_reported": round(t["processor_reported_again"] - t["redis_started"], 1),
                        "seconds_restart_to_live_feed": round(t["live_feed_after_restart"] - t["redis_started"], 1),
                        "live_feed_events_seen_after_restart": len(sse.ids & {e.event_id for e in after})})


@pytest.mark.disruptive
def test_kafka_outage_during_continuous_production_keeps_every_acknowledged_event(client):
    """A producer keeps sending (25 ev/s) while the broker is stopped for 20 s. Every event the broker
    ACKNOWLEDGED must be persisted exactly once; consumer and Spark lag must return to 0."""
    from confluent_kafka import Producer

    from streaming.kafka.client import encode, producer_config

    events = make_events(1750, 611)                         # ~70 s at 25 ev/s
    acked: set[str] = set()
    failed: dict[str, str] = {}
    prod = Producer(producer_config(KAFKA, "container-it-continuous"))
    t: dict[str, Any] = {}

    def on_delivery(err: Any, _msg: Any, eid: str) -> None:
        if err is not None:
            failed[eid] = str(err)
        else:
            acked.add(eid)

    def produce_loop() -> None:
        t0 = time.monotonic()
        for i, e in enumerate(events):
            delay = t0 + i / 25 - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            while True:
                try:
                    prod.produce("transactions.raw", key=e.customer_id.encode(), value=encode(e.to_wire()),
                                 headers={"event_id": e.event_id}, timestamp=int(time.time() * 1000),
                                 on_delivery=lambda err, m, eid=e.event_id: on_delivery(err, m, eid))
                    break
                except BufferError:
                    prod.poll(0.2)
            prod.poll(0)
        prod.flush(120)

    th = threading.Thread(target=produce_loop)
    th.start()
    time.sleep(15)
    compose("stop", "kafka", timeout=180)
    t["kafka_stopped"] = time.time()
    time.sleep(20)
    compose("start", "kafka", timeout=180)
    t["kafka_started"] = time.time()
    th.join(300)
    wait_healthy(("kafka", "spark", "persistence-consumer", "api"), 300)
    ids = [e.event_id for e in events]
    assert acked, "nothing acknowledged"
    assert_exactly_once(sorted(acked), 300)
    t["acked_persisted"] = time.time()
    dupes = {k: v for k, v in persisted(ids).items() if v != 1}
    assert not dupes, f"duplicates: {dupes}"
    wait_until(lambda: _persistence_lag(client) == 0, 180, 3, "persistence consumer lag back to 0")
    wait_until(lambda: _spark_lag(client) == 0, 180, 3, "Spark lag back to 0")
    t["lag_zero"] = time.time()
    assert processor_reported() == SPARK
    _evidence("kafka-continuous", {
        "produced": len(events), "acknowledged": len(acked), "delivery_failed": len(failed),
        "failure_reasons": sorted(set(failed.values()))[:5], "persisted_total": len(persisted(ids)),
        "outage_seconds": round(t["kafka_started"] - t["kafka_stopped"], 1),
        "seconds_restart_to_all_acked_persisted": round(t["acked_persisted"] - t["kafka_started"], 1),
        "seconds_restart_to_zero_lag": round(t["lag_zero"] - t["kafka_started"], 1)})


@pytest.mark.disruptive
def test_spark_backlog_recovery_is_measured(client):
    """Spark stopped while 3,000 events arrive (a backlog); after start it must drain the backlog from its
    checkpoint, exactly once, and lag must return to 0. Records the drain time."""
    compose("stop", "spark", timeout=180)
    t: dict[str, Any] = {"spark_stopped": time.time()}
    try:
        backlog = make_events(3000, 621)
        produce(backlog)
        t["backlog_produced"] = time.time()
        time.sleep(5)
        assert len(persisted([e.event_id for e in backlog])) == 0, "processed while Spark was stopped"
    finally:
        compose("start", "spark", timeout=180)
        t["spark_started"] = time.time()
    ids = [e.event_id for e in backlog]
    wait_until(lambda: len(persisted(ids)) > 0, 300, 2, "first backlog event persisted")
    t["first_persisted"] = time.time()
    assert_exactly_once(ids, 600)
    t["all_persisted"] = time.time()
    wait_until(lambda: _spark_lag(client) == 0 and _persistence_lag(client) == 0, 180, 3, "lag back to 0")
    wait_healthy(("spark",), 240)
    assert processor_reported() == SPARK
    _evidence("spark-backlog", {
        "backlog_events": len(ids),
        "seconds_start_to_first_persisted": round(t["first_persisted"] - t["spark_started"], 1),
        "seconds_start_to_all_persisted": round(t["all_persisted"] - t["spark_started"], 1),
        "drain_rate_eps_after_first": round(len(ids) / max(0.1, t["all_persisted"] - t["first_persisted"]), 1)})


@pytest.mark.disruptive
def test_full_stack_recovery_time_is_measured(client):
    """Controlled stop of the whole stack and `up -d`: records the time until every service is healthy, the API
    answers, the dashboard routes are served, the live feed delivers and a new event is persisted; checks that
    database rows, Spark checkpoints and model artifacts survived."""
    import httpx
    from sqlalchemy import text

    q = ("SELECT (SELECT count(*) FROM transactions), (SELECT count(*) FROM alerts), "
         "(SELECT count(*) FROM investigations), (SELECT count(*) FROM audit_logs), "
         "(SELECT count(*) FROM generated_reports)")
    with db().connect() as c:
        before = tuple(c.execute(text(q)).one())
    commits_before = max(int(x) for x in docker("exec", cid("spark"), "ls", f"{SPARK_CKPT}/score/commits").stdout.split()
                         if x.isdigit())
    arts_before = {f["path"]: f["sha256"] for f in client.get("/models/artifacts").json()["files"]}
    t: dict[str, Any] = {"stop_begin": time.time()}
    compose("stop", timeout=300)
    t["stopped"] = time.time()
    _engine.dispose()
    compose("up", "-d", timeout=600)
    t["up_returned"] = time.time()
    wait_healthy(INFRA_SERVICES + APP_SERVICES, 400)
    t["all_healthy"] = time.time()
    user, pw = ENV.get("FRAUDSTREAM_ADMIN_USERNAME", "admin"), ENV["FRAUDSTREAM_ADMIN_PASSWORD"]
    tok = httpx.post(f"{API}/auth/login", json={"username": user, "password": pw}, timeout=20).json()["access_token"]
    client.headers["Authorization"] = f"Bearer {tok}"
    t["login_ok"] = time.time()
    for route in ("/", "/live", "/streaming", "/models", "/reports", "/settings"):
        assert httpx.get(f"{BASE}{route}", timeout=10).status_code == 200
    with db().connect() as c:
        after = tuple(c.execute(text(q)).one())
    assert after[:3] == before[:3] and after[4] == before[4] and after[3] >= before[3]   # audit grows by logins
    assert {f["path"]: f["sha256"] for f in client.get("/models/artifacts").json()["files"]} == arts_before
    wait_until(lambda: processor_reported() == SPARK, 120, 2, "Spark reported")
    t["processor_reported"] = time.time()
    sse = _SSE(client.headers["Authorization"], 60)
    sse.start()
    time.sleep(3)
    events = make_events(50, 631)
    produce(events)
    assert_exactly_once([e.event_id for e in events])
    t["new_event_persisted"] = time.time()
    wait_until(lambda: sse.ids & {e.event_id for e in events}, 60, 1, "live feed reconnected")
    t["live_feed"] = time.time()
    commits_after = max(int(x) for x in docker("exec", cid("spark"), "ls", f"{SPARK_CKPT}/score/commits").stdout.split()
                        if x.isdigit())
    assert commits_after > commits_before
    sse.join(70)
    s = t["up_returned"]
    _evidence("full-stack", {
        "stop_seconds": round(t["stopped"] - t["stop_begin"], 1),
        "seconds_up_to_all_healthy": round(t["all_healthy"] - s, 1),
        "seconds_up_to_login": round(t["login_ok"] - s, 1),
        "seconds_up_to_spark_reported": round(t["processor_reported"] - s, 1),
        "seconds_up_to_new_event_persisted": round(t["new_event_persisted"] - s, 1),
        "seconds_up_to_live_feed": round(t["live_feed"] - s, 1),
        "rows_before": before, "rows_after": after, "spark_commit_before_after": [commits_before, commits_after],
        "artifacts_identical": True})


def test_spark_container_cannot_read_the_env_file(stack):
    """Phase 12: the Spark container mounts only the source directories it needs; the repository's .env (every
    secret) is no longer visible inside it."""
    for p in ("/app/.env", "/app/.env.isolated", "/app/.env.example"):
        r = docker("exec", cid("spark"), "test", "-e", p, check=False)
        assert r.returncode != 0, f"{p} is visible inside the Spark container"
    job = docker("exec", cid("spark"), "test", "-f", "/app/streaming/spark/fraud_stream_job.py", check=False)
    assert job.returncode == 0
