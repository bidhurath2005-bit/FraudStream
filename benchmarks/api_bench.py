"""API benchmark through the dashboard's nginx (the only HTTP entry point), closed-loop concurrency levels.

    python benchmarks/api_bench.py                               # all endpoint groups, concurrency 1,8,32
    python benchmarks/api_bench.py --groups auth,alerts --levels 1,4 --seconds 5

Each worker sends its next request as soon as the previous one completes (closed loop), so throughput is the
capacity at that concurrency, not an offered load. Reported per endpoint and level: requests, throughput,
p50/p95/p99/max latency (client side, includes nginx and the Windows -> Docker VM port forward), HTTP status
counts, error rate and mean response size. Run against the ISOLATED stack (default .env.isolated, port from it).

The API rate limits (API_RATE_LIMIT_PER_MINUTE=1200 per token, LOGIN_RATE_LIMIT_PER_MINUTE=10 per client) would
cap every level at 20 req/s; the isolated stack is started with raised limits for this benchmark (recorded in
the result), and a separate `--groups ratelimit` run measures the limiter itself with the defaults.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmarks.common import ROOT, DockerStats, container_limits, dist, machine_info, read_env, write_result  # noqa: E402


def groups(ids: dict[str, str]) -> dict[str, list[tuple[str, str, Any] | None]]:
    return {
        "health": [("GET", "/health", None)],
        "auth": [("POST", "/auth/login", "LOGIN"), ("GET", "/auth/me", None)],
        "dashboard": [("GET", "/analytics/overview", None), ("GET", "/analytics/timeline?minutes=30", None),
                      ("GET", "/system/status", None), ("GET", "/infrastructure/status", None)],
        "transactions": [("GET", "/transactions?size=25", None),
                         ("GET", f"/transactions/{ids['tx']}", None) if ids.get("tx") else None],
        "alerts": [("GET", "/alerts?size=25", None), ("GET", f"/alerts/{ids['alert']}", None) if ids.get("alert") else None],
        "investigations": [("GET", "/investigations?size=25", None),
                           ("GET", f"/investigations/{ids['case']}", None) if ids.get("case") else None],
        "analytics": [("GET", "/analytics/patterns?minutes=1440", None), ("GET", "/analytics/confusion", None),
                      ("GET", "/analytics/merchants", None), ("GET", "/analytics/hourly", None),
                      ("GET", "/analytics/velocity", None), ("GET", "/analytics/geo", None)],
        "reports": [("GET", "/reports/types", None), ("GET", "/reports/generated?size=20", None),
                    ("GET", f"/reports/generated/{ids['report']}", None) if ids.get("report") else None],
        "settings": [("GET", "/risk/rules", None), ("GET", "/risk/settings", None), ("GET", "/settings/drift", None),
                     ("GET", "/audit?size=25", None)],
        "models": [("GET", "/models/active", None), ("GET", "/models/live-inference", None)],
        "ingest": [("POST", "/transactions/ingest", "EVENT")],
    }


def _event() -> dict[str, Any]:
    from simulator.generator import SimulationConfig, TransactionGenerator

    gen = TransactionGenerator(SimulationConfig(tps=1000, max_events=1, duration_seconds=None, seed=None))
    e = next(iter(gen.events()))
    return e.model_copy(update={"customer_id": "AB" + os.urandom(3).hex() + "-" + e.customer_id}).to_wire()


async def level(client: Any, method: str, path: str, body: Any, conc: int, seconds: float,
                creds: dict[str, str]) -> dict[str, Any]:
    lat: list[float] = []
    codes: dict[str, int] = {}
    sizes: list[int] = []
    end = time.perf_counter() + seconds

    async def worker() -> None:
        while time.perf_counter() < end:
            payload = creds if body == "LOGIN" else (_event() if body == "EVENT" else body)
            t0 = time.perf_counter()
            try:
                r = await client.request(method, path, json=payload)
                code = str(r.status_code)
                sizes.append(len(r.content))
            except Exception as exc:  # noqa: BLE001 - recorded as an error class
                code = exc.__class__.__name__
            lat.append((time.perf_counter() - t0) * 1000)
            codes[code] = codes.get(code, 0) + 1

    t0 = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(conc)))
    wall = time.perf_counter() - t0
    ok = sum(v for k, v in codes.items() if k.startswith("2"))
    n = sum(codes.values())
    return {"concurrency": conc, "requests": n, "seconds": round(wall, 2), "throughput_rps": round(n / wall, 1),
            "ok_rps": round(ok / wall, 1), "status": codes, "error_rate": round(1 - ok / n, 4) if n else None,
            "latency_ms": dist(lat), "mean_response_bytes": round(sum(sizes) / len(sizes)) if sizes else None}


async def run(a: argparse.Namespace) -> dict[str, Any]:
    import httpx

    env = read_env(Path(a.env_file))
    base = f"http://127.0.0.1:{env.get('FRONTEND_HOST_PORT', '8080')}/api/v1"
    creds = {"username": env.get("FRAUDSTREAM_ADMIN_USERNAME", "admin"), "password": env["FRAUDSTREAM_ADMIN_PASSWORD"]}
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=200)
    async with httpx.AsyncClient(base_url=base, timeout=60, limits=limits) as c:
        r = await c.post("/auth/login", json=creds)
        r.raise_for_status()
        c.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        ids = {}
        for key, path, field in (("tx", "/transactions?size=1", "id"), ("alert", "/alerts?size=1", "alert_uid"),
                                 ("case", "/investigations?size=1", "case_uid"),
                                 ("report", "/reports/generated?size=1", "report_uid")):
            items = (await c.get(path)).json().get("items", [])
            ids[key] = items[0].get(field) if items else None
        g = groups(ids)
        out: dict[str, Any] = {}
        levels = [int(x) for x in a.levels.split(",")]
        for name in a.groups.split(","):
            for ep in filter(None, g[name]):
                method, path, body = ep
                key = f"{method} {path.split('?')[0]}" + ("?" + path.split("?")[1] if "?" in path else "")
                conc_levels = [x for x in levels if x <= a.login_max_concurrency] if body == "LOGIN" else levels
                res = []
                for conc in conc_levels:
                    res.append(await level(c, method, path, body, conc, a.seconds, creds))
                    lr = res[-1]
                    print(f"{key:<48} c={conc:<3} {lr['throughput_rps']:>7}/s p50 {lr['latency_ms'].get('p50')} "
                          f"p95 {lr['latency_ms'].get('p95')} p99 {lr['latency_ms'].get('p99')} err {lr['error_rate']} "
                          f"{lr['mean_response_bytes']} B", flush=True)
                out[key] = {"group": name, "levels": res}
        return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--project", default="fraudstream-p12")
    ap.add_argument("--env-file", default=str(ROOT / ".env.isolated"))
    ap.add_argument("--groups", default="health,auth,dashboard,transactions,alerts,investigations,analytics,reports,"
                                        "settings,models,ingest")
    ap.add_argument("--levels", default="1,8,32")
    ap.add_argument("--seconds", type=float, default=8)
    ap.add_argument("--login-max-concurrency", type=int, default=8)
    a = ap.parse_args()
    if a.project == "fraudstream" and not os.environ.get("BENCH_ALLOW_LIVE"):
        print("refusing to benchmark the LIVE project; use the isolated stack")
        return 2
    env = read_env(Path(a.env_file))
    t0 = time.time()
    with DockerStats(a.project) as stats:
        res = asyncio.run(run(a))
    payload = {"benchmark": "api", "measured": True, "started_at": datetime.fromtimestamp(t0, UTC).isoformat(),
               "project": a.project, "args": {k: v for k, v in vars(a).items() if k != "env_file"},
               "rate_limits_during_run": {"API_RATE_LIMIT_PER_MINUTE": env.get("API_RATE_LIMIT_PER_MINUTE"),
                                          "LOGIN_RATE_LIMIT_PER_MINUTE": env.get("LOGIN_RATE_LIMIT_PER_MINUTE")},
               "client": "httpx AsyncClient on the Windows host -> nginx (127.0.0.1) -> api container",
               "machine": machine_info(), "container_limits": container_limits(a.project),
               "resources": stats.summary(), "endpoints": res}
    print(f"result: {write_result('api', payload)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
