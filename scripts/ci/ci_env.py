"""Create the env file for a throw-away CI / regression stack, and export integration-test variables.

    python scripts/ci/ci_env.py create --out .env.ci [--project fraudstream-ci] [--e2e]
    python scripts/ci/ci_env.py export --env-file .env.ci [--project fraudstream-ci] [--github-env]

``create`` starts from ``scripts/isolated_env.py`` (fresh RANDOM secrets generated on the machine that runs it, host
ports 18080/19092/15433/16380/14040, never the live ones) and only changes:
  * FRAUDSTREAM_NETWORK -> the project name (the base compose file pins the network name; see the isolated override),
  * the login account names -> ci-admin / ci-analyst,
  * with ``--e2e``: LOGIN_RATE_LIMIT_PER_MINUTE=100 in THIS test env file only. The Playwright suite logs in more than
    10 times per minute; the production default (10/min, docker-compose.yml / .env.example) is NOT changed.

``export --github-env`` appends the variables the Kafka/Spark/container tests read to the file named by $GITHUB_ENV;
on GitHub Actions it first registers every secret with ``::add-mask::`` so the runner never prints it. Locally, point
GITHUB_ENV at a private file and load it (CONTRIBUTING.md section 5). Without ``--github-env`` only the variable NAMES
are printed (values stay in the env file).
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from _ops import read_env  # noqa: E402

SECRET_KEYS = ("JWT_SECRET", "POSTGRES_PASSWORD", "REDIS_PASSWORD", "FRAUDSTREAM_ADMIN_PASSWORD",
               "FRAUDSTREAM_ANALYST_PASSWORD")


def create(out: Path, project: str, e2e: bool) -> int:
    if project == "fraudstream":
        print("refusing: 'fraudstream' is the live project name", file=sys.stderr)
        return 2
    if out.exists():
        print(f"{out} exists - not modified (delete it yourself if you want new secrets)")
        return 0
    subprocess.run([sys.executable, str(ROOT / "scripts" / "isolated_env.py"), "--out", str(out)], check=True,
                   stdout=subprocess.DEVNULL)
    overrides = {"FRAUDSTREAM_NETWORK": project, "FRAUDSTREAM_ADMIN_USERNAME": "ci-admin",
                 "FRAUDSTREAM_ANALYST_USERNAME": "ci-analyst"}
    if e2e:
        overrides["LOGIN_RATE_LIMIT_PER_MINUTE"] = "100"
    lines = []
    for line in out.read_text(encoding="utf-8").splitlines():
        key = line.split("=", 1)[0].strip()
        lines.append(f"{key}={overrides.pop(key)}" if key in overrides else line)
    lines += [f"{k}={v}" for k, v in overrides.items()]
    lines[0] = f"# Throw-away CI/regression stack (project {project}). Random secrets; never commit or reuse."
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {out} for project {project} (secrets not shown)")
    return 0


def export(env_file: Path, project: str, github_env: bool) -> int:
    e = read_env(env_file)
    pg, rd = e["POSTGRES_PASSWORD"], e["REDIS_PASSWORD"]
    user, pg_port = e.get("POSTGRES_USER", "fraudstream"), e.get("POSTGRES_HOST_PORT", "5433")
    values = {
        "KAFKA_BOOTSTRAP_SERVERS": f"127.0.0.1:{e.get('KAFKA_HOST_PORT', '9092')}",
        "IT_POSTGRES_ADMIN_URL": f"postgresql+psycopg://{user}:{pg}@127.0.0.1:{pg_port}/postgres",
        "REDIS_URL": f"redis://:{rd}@127.0.0.1:{e.get('REDIS_HOST_PORT', '6380')}/0",
        "SPARK_IT_REDIS_URL": f"redis://:{rd}@redis:6379/15",
        "SPARK_IT_NETWORK": e.get("FRAUDSTREAM_NETWORK", project),
        "SPARK_IT_COMPOSE_PROJECT": project,
        "FS_COMPOSE_PROJECT": project,
        "FS_ENV_FILE": env_file.as_posix(),
        "FS_COMPOSE_FILES": "docker-compose.yml,infrastructure/isolated/docker-compose.isolated.yml",
    }
    if github_env:
        if os.environ.get("GITHUB_ACTIONS") == "true":     # mask commands only mean something on GitHub
            for k in SECRET_KEYS:
                if e.get(k):
                    print(f"::add-mask::{e[k]}")
        with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as fh:
            for k, v in values.items():
                fh.write(f"{k}={v}\n")
        print("exported: " + ", ".join(values))
    else:
        print("variables (values are in the env file): " + ", ".join(values))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("create")
    c.add_argument("--out", default=".env.ci")
    c.add_argument("--project", default="fraudstream-ci")
    c.add_argument("--e2e", action="store_true")
    x = sub.add_parser("export")
    x.add_argument("--env-file", default=".env.ci")
    x.add_argument("--project", default="fraudstream-ci")
    x.add_argument("--github-env", action="store_true")
    a = ap.parse_args()
    if a.cmd == "create":
        return create(ROOT / a.out, a.project, a.e2e)
    return export(Path(a.env_file), a.project, a.github_env)


if __name__ == "__main__":
    raise SystemExit(main())
