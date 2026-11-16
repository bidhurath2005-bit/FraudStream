"""Verify the DISTRIBUTED Kafka pipeline end to end through the public API.

Prerequisites: docker compose infra up; API running with FRAUDSTREAM_PROFILE=distributed;
the Kafka processor and persistence consumer running.

    python scripts/verify_kafka_pipeline.py --username <admin> --password-env DEMO_PASSWORD \
        --tps 20 --seconds 45

Steps: login -> check profile -> listen to SSE -> start simulation (Kafka sink) -> wait for it to
finish -> wait until every sent event is persisted -> report counts, consumer-group lag, DLQ,
alerts and SSE deliveries. Exits non-zero if any reconciliation check fails.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time

import httpx


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://127.0.0.1:8000/api/v1")
    ap.add_argument("--username", required=True)
    ap.add_argument("--password-env", required=True)
    ap.add_argument("--tps", type=float, default=20)
    ap.add_argument("--seconds", type=float, default=45)
    ap.add_argument("--suspicious-pct", type=float, default=8)
    ap.add_argument("--seed", type=int, default=2027)
    ap.add_argument("--timeout", type=float, default=240)
    a = ap.parse_args()
    api = a.api
    c = httpx.Client(timeout=30)
    tok = c.post(f"{api}/auth/login", json={"username": a.username, "password": os.environ[a.password_env]}).raise_for_status().json()["access_token"]
    h = {"Authorization": f"Bearer {tok}"}
    st = c.get(f"{api}/system/status", headers=h).json()
    print(f"profile: {st['profile']} | {st['profile_description']} | live broker: {st['live_broker']} | model: {st['model']['supervised']}")
    if st["profile"] != "distributed":
        print("FAIL: API is not running the distributed profile")
        return 2
    tx0 = c.get(f"{api}/transactions?size=1", headers=h).json()["total"]
    al0 = c.get(f"{api}/alerts?size=1&status=", headers=h).json()["total"]

    sse = {"transactions": 0, "status": None}

    def listen() -> None:
        try:
            with httpx.stream("GET", f"{api}/stream/transactions", headers=h, timeout=a.seconds + 60) as r:
                for line in r.iter_lines():
                    if line.startswith("data:"):
                        d = json.loads(line[5:])
                        if d.get("type") == "status":
                            sse["status"] = d
                        elif d.get("type") == "transaction":
                            sse["transactions"] += 1
        except httpx.HTTPError as exc:
            sse["error"] = f"{exc.__class__.__name__}: {exc}"
    threading.Thread(target=listen, daemon=True).start()
    time.sleep(1)

    r = c.post(f"{api}/simulation/start", headers=h, json={
        "mode": "mixed", "tps": a.tps, "duration_seconds": a.seconds, "customer_count": 20000,
        "suspicious_pct": a.suspicious_pct, "seed": a.seed, "label": "kafka verification"})
    if r.status_code != 201:
        print(f"FAIL: simulation start returned {r.status_code}: {r.text}")
        return 2
    print(f"simulation started -> sink {r.json()['sink']}")
    t0 = time.time()
    while True:
        s = c.get(f"{api}/simulation/status", headers=h).json()
        if s["status"] not in ("RUNNING", "PAUSED"):
            break
        time.sleep(2)
    sent = s["events_generated"]
    print(f"simulation {s['status']}: sent={sent} kafka={s.get('kafka')} error={s.get('error')}")
    if s["status"] != "COMPLETED" or (s.get("kafka") or {}).get("failed"):
        print("FAIL: simulation did not complete cleanly")
        return 1

    deadline = time.time() + a.timeout
    while time.time() < deadline:
        tx = c.get(f"{api}/transactions?size=1", headers=h).json()["total"] - tx0
        if tx >= sent:
            break
        time.sleep(2)
    persisted = c.get(f"{api}/transactions?size=1", headers=h).json()["total"] - tx0
    alerts = c.get(f"{api}/alerts?size=1&status=", headers=h).json()["total"] - al0
    end_to_end = time.time() - t0
    time.sleep(3)
    infra = c.get(f"{api}/infrastructure/status", headers=h).json()
    groups = {g["group"]: {"lag": g.get("total_lag"), "active": g.get("active"), "state": g.get("state")}
              for g in infra["kafka"].get("consumer_groups", [])}
    spark = infra.get("spark") or {}
    spark_lag = None
    if infra.get("stream_processor") == "Spark Structured Streaming":
        spark_lag = next((q.get("lag_vs_raw_topic") for q in spark.get("queries", []) if q.get("name") == "score"), None)
    ov = c.get(f"{api}/analytics/overview", headers=h).json()
    print(json.dumps({
        "events_sent_to_kafka": sent, "transactions_persisted": persisted, "alerts_created": alerts,
        "sse_transactions_received": sse["transactions"], "sse_broker": (sse["status"] or {}).get("broker"),
        "consumer_groups": groups, "spark_score_query_lag": spark_lag,
        "dlq_messages_total": infra["kafka"].get("dlq_messages"),
        "stream_processor": infra.get("stream_processor"),
        "processor_heartbeat": (infra.get("processor") or {}).get("status"),
        "consumer_heartbeat": (infra.get("consumer") or {}).get("status"),
        "sse_last_status": sse["status"], "sse_error": sse.get("error"),
        "detection_latency_ms_p50_p95": [ov["detection_latency_ms"]["p50"], ov["detection_latency_ms"]["p95"]],
        "wall_seconds": round(end_to_end, 1)}, indent=2))
    active_lag_ok = all((g["lag"] or 0) == 0 for g in groups.values() if g["active"])
    spark_ok = infra.get("stream_processor") != "Spark Structured Streaming" or spark_lag == 0
    processor_ok = not str(infra.get("stream_processor", "")).startswith("NOT RUNNING")
    ok = persisted == sent and active_lag_ok and spark_ok and processor_ok and sse["transactions"] > 0
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
