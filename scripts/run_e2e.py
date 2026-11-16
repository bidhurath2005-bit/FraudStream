"""Run the Playwright E2E suite against a stack, taking the login accounts from an env file (Phase 12).

    python scripts/run_e2e.py --env-file .env.isolated --profile distributed          # http://127.0.0.1:<FRONTEND_HOST_PORT>
    python scripts/run_e2e.py --env-file .env.isolated --profile local                # http://127.0.0.1:<FRONTEND_LOCAL_HOST_PORT>

Passwords are handed to the test process through its environment only (never printed or written).
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _ops import ROOT, read_env  # noqa: E402

CHROME = Path("C:/Program Files/Google/Chrome/Application/chrome.exe")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", required=True)
    ap.add_argument("--profile", choices=("distributed", "local"), required=True)
    ap.add_argument("args", nargs="*", help="extra playwright arguments (e.g. a spec file)")
    a = ap.parse_args()
    e = read_env(Path(a.env_file))
    port = e.get("FRONTEND_HOST_PORT", "8080") if a.profile == "distributed" else e.get("FRONTEND_LOCAL_HOST_PORT", "8081")
    env = {**os.environ,
           "E2E_BASE_URL": f"http://127.0.0.1:{port}", "E2E_PROFILE": a.profile, "E2E_ARTIFACTS": "read-only",
           "E2E_USERNAME": e.get("FRAUDSTREAM_ANALYST_USERNAME", "analyst"), "E2E_PASSWORD": e["FRAUDSTREAM_ANALYST_PASSWORD"],
           "E2E_ADMIN_USERNAME": e.get("FRAUDSTREAM_ADMIN_USERNAME", "admin"),
           "E2E_ADMIN_PASSWORD": e["FRAUDSTREAM_ADMIN_PASSWORD"]}
    if a.profile == "distributed":
        env["E2E_EXPECT_PROCESSOR"] = "Spark Structured Streaming"
    if CHROME.exists():
        env["E2E_CHROMIUM_PATH"] = str(CHROME)
    npx = shutil.which("npx") or "npx"
    print(f"E2E {a.profile} -> {env['E2E_BASE_URL']} as {env['E2E_USERNAME']} / {env['E2E_ADMIN_USERNAME']}", flush=True)
    return subprocess.run([npx, "playwright", "test", *a.args], cwd=ROOT / "frontend", env=env).returncode


if __name__ == "__main__":
    raise SystemExit(main())
