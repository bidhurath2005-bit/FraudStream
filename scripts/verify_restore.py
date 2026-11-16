"""Verify a RUNNING (restored) stack against a backup manifest (Phase 12).

    python scripts/verify_restore.py --backup <dir> -p fraudstream-p12 --env-file .env.isolated \
        -f docker-compose.yml -f infrastructure/isolated/docker-compose.isolated.yml --report verify.json

Checks (all must pass; exit 0 only then):
  1. PostgreSQL content: for every fingerprinted table, the rows with id <= the backup's max id are byte-identical
     (md5 of the ordered row text) — new rows added after start-up are allowed, changed/missing old rows are not.
  2. Alembic revision equals the backup's.
  3. Model artifacts: SHA-256 of every file served by GET /models/artifacts equals the manifest (the API reads the
     restored, read-only mount), and the active models were loaded.
  4. Datasets: SHA-256 of the mounted dataset files equals the manifest (computed inside the API container).
  5. Application: /health 200 (database up, model loaded, profile distributed), login, dashboard endpoints 200
     through nginx, SPA deep links served, Spark reported as the active processor.
Credentials are read from the env file and never printed.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _ops import Compose, OpsError, alembic_revision_pg, dump_json, say, table_fingerprints  # noqa: E402

ENDPOINTS = ("/system/status", "/infrastructure/status", "/analytics/overview", "/alerts?size=5", "/investigations",
             "/transactions?size=5", "/models/active", "/models/artifacts", "/risk/rules", "/risk/settings",
             "/settings/drift", "/reports/types", "/reports/generated?size=5", "/audit?size=5", "/datasets")
ROUTES = ("/", "/login", "/live", "/investigations", "/simulation", "/streaming", "/models", "/risk", "/map", "/reports",
          "/settings")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--backup", required=True)
    ap.add_argument("-p", "--project", required=True)
    ap.add_argument("--env-file", required=True)
    ap.add_argument("-f", "--file", action="append")
    ap.add_argument("--report", default="")
    ap.add_argument("--expect-processor", default="Spark Structured Streaming")
    a = ap.parse_args(argv)
    import httpx

    compose = Compose(a.project, Path(a.env_file), [Path(f) for f in a.file] if a.file else ())
    m = json.loads((Path(a.backup) / "manifest.json").read_text(encoding="utf-8"))
    checks: dict[str, Any] = {}

    def check(name: str, ok: bool, detail: Any = None) -> None:
        checks[name] = {"ok": bool(ok), "detail": detail}
        say(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f": {detail}" if detail not in (None, "") and not ok else ""))

    pgm = m["components"]["postgres"]
    conn = compose.pg_connect()
    try:
        fp_src = pgm.get("fingerprints") or {}
        fp = table_fingerprints(conn, {t: v["max_id"] for t, v in fp_src.items()})
        bad = {t: {"backup": fp_src[t], "now": fp.get(t)} for t in fp_src if fp.get(t) != fp_src[t]}
        check(f"postgres content fingerprints ({len(fp_src)} tables, rows id<=backup max)", fp_src and not bad, bad)
        check("alembic revision", alembic_revision_pg(conn) == pgm["alembic_revision"], alembic_revision_pg(conn))
        conn.rollback()
    finally:
        conn.close()

    port = compose.env.get("FRONTEND_HOST_PORT", "8080")
    base = f"http://127.0.0.1:{port}"
    api = f"{base}/api/v1"
    h = httpx.get(f"{api}/health", timeout=10)
    hj = h.json() if h.status_code == 200 else {}
    check("health 200, database up, model loaded, distributed",
          h.status_code == 200 and hj.get("database") == "up" and hj.get("model_loaded") and hj.get("profile") == "distributed",
          {k: hj.get(k) for k in ("status", "database", "model_loaded", "profile")})
    user = compose.env.get("FRAUDSTREAM_ADMIN_USERNAME", "admin")
    r = httpx.post(f"{api}/auth/login", json={"username": user, "password": compose.env["FRAUDSTREAM_ADMIN_PASSWORD"]},
                   timeout=15)
    check("admin login", r.status_code == 200, r.status_code)
    if r.status_code != 200:
        return finish(a, checks)
    c = httpx.Client(base_url=api, timeout=30, headers={"Authorization": f"Bearer {r.json()['access_token']}"})

    arts = c.get("/models/artifacts").json()
    served = {f["path"]: f["sha256"] for f in arts.get("files", [])}
    expected = {e["path"]: e["sha256"] for e in m["components"]["artifacts"]["files"]}
    diff = {p: {"backup": expected.get(p), "served": served.get(p)} for p in expected if served.get(p) != expected[p]}
    extra = sorted(set(served) - set(expected) - {"runtime_config.json"})
    check(f"model artifacts SHA-256 served by the API ({len(expected)} files)", not diff and not extra,
          {"mismatch": diff, "unexpected": extra})
    act = c.get("/models/active").json()
    loaded = act.get("loaded_in_api") or {}
    check("active models loaded by the API", bool(loaded.get("supervised")) and not act.get("model_error"), loaded)

    ds = m["components"].get("datasets")
    if ds:
        cid = compose.container("api")
        script = ("import hashlib,json,pathlib;print(json.dumps({p.name: hashlib.sha256(p.read_bytes()).hexdigest() "
                  "for p in pathlib.Path('/data/synthetic').iterdir() if p.is_file()}))")
        out = subprocess.run(["docker", "exec", cid, "python", "-c", script], capture_output=True, text=True, timeout=120)
        got = json.loads(out.stdout) if out.returncode == 0 else {}
        exp = {Path(e["path"]).name: e["sha256"] for e in ds["files"]}
        check(f"datasets SHA-256 inside the API container ({len(exp)} files)", got == exp,
              {k: (exp.get(k), got.get(k)) for k in set(exp) | set(got) if exp.get(k) != got.get(k)})

    codes = {e: c.get(e).status_code for e in ENDPOINTS}
    check(f"{len(ENDPOINTS)} dashboard endpoints 200 through nginx", all(v == 200 for v in codes.values()),
          {k: v for k, v in codes.items() if v != 200})
    pages = {p: httpx.get(base + p, timeout=10) for p in ROUTES}
    check(f"{len(ROUTES)} SPA routes served", all(x.status_code == 200 and "<div id=\"root\"" in x.text
                                                   for x in pages.values()),
          {p: x.status_code for p, x in pages.items() if x.status_code != 200})
    proc = None
    for _ in range(30):
        proc = httpx.get(f"{api}/health", timeout=10).json().get("stream_processor")
        if proc == a.expect_processor:
            break
        time.sleep(2)
    check("active stream processor", proc == a.expect_processor, proc)
    return finish(a, checks)


def finish(a: argparse.Namespace, checks: dict[str, Any]) -> int:
    ok = all(v["ok"] for v in checks.values())
    if a.report:
        dump_json(Path(a.report), {"backup": str(a.backup), "project": a.project, "checks": checks,
                                   "result": "PASS" if ok else "FAIL"})
    say(f"VERIFY {'PASS' if ok else 'FAIL'} ({sum(v['ok'] for v in checks.values())}/{len(checks)} checks)")
    return 0 if ok else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except OpsError as exc:
        say(f"FAILED: {exc}")
        raise SystemExit(1) from None
