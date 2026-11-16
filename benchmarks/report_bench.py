"""Report-generation benchmark: all six report types x small/medium/large periods, CSV and PDF export, API
memory, and API responsiveness while reports are generated.

    python benchmarks/report_bench.py [--sizes small,medium,large] [--concurrent 4]

Sizes are PERIODS; the result records the actual rows each report covered (the DB content decides):
  small  = last 1 hour, medium = last 7 days, large = last 366 days (the maximum period).
Responsiveness: a probe thread calls GET /auth/me every 100 ms during the whole run; its latency is reported for
the idle baseline, during sequential generation and during N concurrent large generations.
API memory: `docker stats` of the api container sampled every ~1 s (peak per phase). Isolated stack only.
"""
from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmarks.common import ROOT, DockerStats, dist, machine_info, read_env, write_result  # noqa: E402

TYPES = ("fraud_activity", "risk_analytics", "alert_summary", "investigation", "model_performance", "system_activity")
SIZES = {"small": timedelta(hours=1), "medium": timedelta(days=7), "large": timedelta(days=366)}


class Probe(threading.Thread):
    def __init__(self, base: str, token: str):
        super().__init__(daemon=True)
        import httpx

        self.c = httpx.Client(base_url=base, timeout=60, headers={"Authorization": f"Bearer {token}"})
        self.samples: list[tuple[float, float, int]] = []
        self.stop = threading.Event()

    def run(self) -> None:
        while not self.stop.is_set():
            t = time.time()
            t0 = time.perf_counter()
            try:
                code = self.c.get("/auth/me").status_code
            except Exception:  # noqa: BLE001
                code = 0
            self.samples.append((t, (time.perf_counter() - t0) * 1000, code))
            self.stop.wait(max(0, 0.1 - (time.time() - t)))

    def between(self, t0: float, t1: float) -> dict[str, Any]:
        xs = [s for s in self.samples if t0 <= s[0] <= t1]
        d = dist([s[1] for s in xs])
        d["errors"] = sum(1 for s in xs if s[2] != 200)
        return d


def main() -> int:
    import httpx

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--project", default="fraudstream-p12")
    ap.add_argument("--env-file", default=str(ROOT / ".env.isolated"))
    ap.add_argument("--sizes", default="small,medium,large")
    ap.add_argument("--concurrent", type=int, default=4)
    a = ap.parse_args()
    if a.project == "fraudstream" and not os.environ.get("BENCH_ALLOW_LIVE"):
        print("refusing to benchmark the LIVE project (stores generated reports); use the isolated stack")
        return 2
    env = read_env(Path(a.env_file))
    base = f"http://127.0.0.1:{env.get('FRONTEND_HOST_PORT', '8080')}/api/v1"
    tok = httpx.post(f"{base}/auth/login", json={"username": env.get("FRAUDSTREAM_ADMIN_USERNAME", "admin"),
                                                 "password": env["FRAUDSTREAM_ADMIN_PASSWORD"]}, timeout=30)
    tok.raise_for_status()
    token = tok.json()["access_token"]
    c = httpx.Client(base_url=base, timeout=600, headers={"Authorization": f"Bearer {token}"})
    probe = Probe(base, token)
    rows: list[dict[str, Any]] = []
    phases: dict[str, tuple[float, float]] = {}
    with DockerStats(a.project, interval=1.0) as stats:
        probe.start()
        t = time.time()
        time.sleep(10)
        phases["idle_baseline"] = (t, time.time())
        t_seq = time.time()
        end = datetime.now(timezone.utc).replace(microsecond=0)
        for size in a.sizes.split(","):
            start = end - SIZES[size]
            for rt in TYPES:
                body = {"report_type": rt, "start": start.isoformat(), "end": end.isoformat()}
                t0 = time.time()
                r = c.post("/reports", json=body)
                gen_s = time.time() - t0
                rec: dict[str, Any] = {"type": rt, "size": size, "period_days": SIZES[size].total_seconds() / 86400,
                                       "status": r.status_code, "generate_s": round(gen_s, 3), "t0": t0}
                if r.status_code == 201:
                    rep = r.json()
                    content = rep.get("content") or {}
                    rec["report_status"] = rep.get("status")
                    rec["stored_rows"] = rep.get("row_count")
                    rec["section_total_rows"] = sum(s.get("total_rows") or 0 for s in content.get("sections", []))
                    rec["summary"] = {m["key"]: m["value"] for m in content.get("summary", [])
                                      if m["key"] in ("transactions", "alerts", "investigations", "events", "predictions")}
                    rec["preview_bytes"] = len(r.content)
                    for fmt in ("csv", "pdf"):
                        t1 = time.time()
                        d = c.get(f"/reports/generated/{rep['report_uid']}/download?format={fmt}")
                        rec[f"{fmt}_s"] = round(time.time() - t1, 3)
                        rec[f"{fmt}_bytes"] = len(d.content)
                        rec[f"{fmt}_status"] = d.status_code
                rec["t1"] = time.time()
                rec["api_mem_mib_max"] = max((x["mem_mib"] for x in stats.samples if x["service"] == "api"
                                              and t0 - 1 <= x["t"] <= rec["t1"] + 1), default=None)
                rows.append(rec)
                print(f"{size:<6} {rt:<18} gen {rec['generate_s']:>6}s rows {rec.get('section_total_rows')} "
                      f"csv {rec.get('csv_s')}s/{rec.get('csv_bytes')}B pdf {rec.get('pdf_s')}s/{rec.get('pdf_bytes')}B "
                      f"mem {rec['api_mem_mib_max']}", flush=True)
        phases["sequential_generation"] = (t_seq, time.time())
        # N concurrent large reports (+ PDF) while the probe keeps measuring
        t_conc = time.time()
        start = end - SIZES["large"]

        def one(rt: str) -> float:
            t0 = time.time()
            rep = c.post("/reports", json={"report_type": rt, "start": start.isoformat(), "end": end.isoformat()}).json()
            c.get(f"/reports/generated/{rep['report_uid']}/download?format=pdf")
            return time.time() - t0

        with ThreadPoolExecutor(a.concurrent) as ex:
            conc_times = list(ex.map(one, [TYPES[i % 3] for i in range(a.concurrent)]))
        phases["concurrent_large"] = (t_conc, time.time())
        time.sleep(3)
        probe.stop.set()
        probe.join(10)
    api_mem = {k: max((x["mem_mib"] for x in stats.samples if x["service"] == "api" and v[0] <= x["t"] <= v[1]),
                      default=None) for k, v in phases.items()}
    payload = {"benchmark": "reports", "measured": True, "project": a.project, "machine": machine_info(),
               "reports": rows, "concurrent": {"n": a.concurrent, "types": [TYPES[i % 3] for i in range(a.concurrent)],
                                               "each_generate_plus_pdf_s": [round(x, 2) for x in conc_times]},
               "responsiveness_probe_ms": {k: probe.between(*v) for k, v in phases.items()},
               "api_mem_mib_max_by_phase": api_mem,
               "resources_overall": stats.summary()}
    print("probe /auth/me latency:", {k: (v.get("p50"), v.get("p95"), v.get("max")) for k, v in
                                      payload["responsiveness_probe_ms"].items()})
    print(f"result: {write_result('reports', payload)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
