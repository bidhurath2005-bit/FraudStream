"""Seed a FRESH test stack with synthetic traffic before the Playwright E2E suite.

    python scripts/ci/seed_traffic.py --env-file .env.ci --profile distributed|local [--min-transactions 300]

Why: Playwright runs the spec files alphabetically, and only ``workflow.spec.ts`` (the last one) generates traffic.
On a stack with an EMPTY database the earlier specs (Model Intelligence live inference, Risk Analytics, the map, the
report downloads) find nothing to show: two skip with "no scored transactions in the last 7 days on this stack" and
three fail waiting for data-dependent elements. Long-lived stacks never hit this; a CI stack always does.

What it does, only through the public API as the analyst account of the env file: start one short ``mixed``
simulation run (label ``ci-seed``), inject each suspicious scenario once, then wait until at least
``--min-transactions`` scored transactions and at least one alert are persisted. Synthetic data only; it changes no
configuration. Refuses to run against the live stack's ports.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from _ops import read_env  # noqa: E402

LIVE_PORTS = {"8080", "8081"}


def call(base: str, method: str, path: str, token: str | None = None, body: dict | None = None) -> dict:
    req = urllib.request.Request(base + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json",
                                          **({"Authorization": f"Bearer {token}"} if token else {})})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read() or b"{}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", required=True)
    ap.add_argument("--profile", choices=("distributed", "local"), required=True)
    ap.add_argument("--min-transactions", type=int, default=300)
    ap.add_argument("--timeout", type=int, default=300)
    a = ap.parse_args()
    e = read_env(Path(a.env_file))
    port = e.get("FRONTEND_HOST_PORT", "8080") if a.profile == "distributed" else e.get("FRONTEND_LOCAL_HOST_PORT", "8081")
    if port in LIVE_PORTS:
        sys.exit(f"refusing: port {port} is the live dashboard; seed only throw-away test stacks")
    base = f"http://127.0.0.1:{port}/api/v1"
    token = call(base, "POST", "/auth/login", body={"username": e.get("FRAUDSTREAM_ANALYST_USERNAME", "analyst"),
                                                     "password": e["FRAUDSTREAM_ANALYST_PASSWORD"]})["access_token"]
    try:
        call(base, "POST", "/simulation/start", token, {"mode": "mixed", "tps": 15, "duration_seconds": 30,
                                                        "suspicious_pct": 5, "label": "ci-seed"})
    except urllib.error.HTTPError as exc:
        if exc.code != 409:          # 409 = a run is already active; its traffic serves the same purpose
            raise
    scenarios = call(base, "GET", "/simulation/scenarios", token)["scenarios"]
    for i, sc in enumerate(scenarios):
        call(base, "POST", "/simulation/inject", token, {"scenario": sc, "seed": 900 + i})
    print(f"seeding {a.profile} stack on :{port}: 30 s mixed run at 15 tps + {len(scenarios)} injected scenarios")
    deadline, tx, alerts = time.time() + a.timeout, 0, 0
    while time.time() < deadline:
        tx = call(base, "GET", "/transactions?size=1", token)["total"]
        alerts = call(base, "GET", "/alerts?size=1&status=", token)["total"]
        if tx >= a.min_transactions and alerts >= 1:
            print(f"seeded: {tx} transactions, {alerts} alerts persisted")
            return 0
        time.sleep(3)
    print(f"timed out: {tx} transactions, {alerts} alerts persisted", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
