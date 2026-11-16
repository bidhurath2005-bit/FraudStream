"""Wait until every long-running service of a compose project is healthy and every one-shot exited 0.

    python scripts/ci/wait_for_stack.py --project fraudstream-ci --env-file .env.ci --profile distributed [--timeout 900]

Read-only: it only runs ``docker compose ps``. Exits 1 (and prints the states) on timeout, when a one-shot service
exited non-zero, or when a container is restarting in a loop.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ONE_SHOT = {"kafka-init", "app-init"}


def _compose(a: argparse.Namespace, *args: str) -> str:
    cmd = ["docker", "compose", "-p", a.project, "--env-file", a.env_file, "-f", "docker-compose.yml", "-f",
           "infrastructure/isolated/docker-compose.isolated.yml", "--profile", a.profile, *args]
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()


def profile_services(a: argparse.Namespace) -> set[str]:
    return set(_compose(a, "config", "--services").split())


def states(a: argparse.Namespace) -> list[dict]:
    # `ps -a` lists every container of the project, including other profiles' stopped ones - filtered by the caller
    out = _compose(a, "ps", "-a", "--format", "json")
    if not out:
        return []
    return json.loads(out) if out.startswith("[") else [json.loads(ln) for ln in out.splitlines()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--env-file", required=True)
    ap.add_argument("--profile", default="distributed")
    ap.add_argument("--services", default="", help="comma list to wait for (default: all of the profile)")
    ap.add_argument("--timeout", type=int, default=900)
    a = ap.parse_args()
    if a.project == "fraudstream":
        sys.exit("refusing: this helper is for isolated projects only")
    wanted = {s for s in a.services.split(",") if s} or profile_services(a)
    deadline, t0 = time.time() + a.timeout, time.time()
    while True:
        rows = [r for r in states(a) if r["Service"] in wanted]
        pending, bad = [], []
        for r in rows:
            svc, state, health = r["Service"], r["State"], r.get("Health", "")
            if svc in ONE_SHOT:
                if state == "exited" and r.get("ExitCode", 0) != 0:
                    bad.append(f"{svc} exited {r.get('ExitCode')}")
                elif state != "exited":
                    pending.append(f"{svc}:{state}")
            elif state == "restarting":
                bad.append(f"{svc} is restarting")
            elif state != "running" or (health and health != "healthy"):
                pending.append(f"{svc}:{state}/{health or '-'}")
        if rows and not pending and not bad:
            print(f"all {len(rows)} services ready after {time.time() - t0:.0f} s")
            return 0
        if bad or time.time() > deadline:
            print("NOT READY:", *(bad or pending), sep="\n  ")
            return 1
        time.sleep(5)


if __name__ == "__main__":
    raise SystemExit(main())
