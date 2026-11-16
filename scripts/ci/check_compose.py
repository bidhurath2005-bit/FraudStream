"""Validate the Docker Compose configuration without starting or changing anything.

    python scripts/ci/check_compose.py [--env-file .env.example]

For each combination (default services, every profile, and the isolated override) it runs
``docker compose ... config`` (parse + interpolation + schema validation; no container, network or volume is touched)
and then checks that every file the configuration refers to exists in the repository: build contexts, Dockerfiles,
bind-mount sources, and the files baked into images by COPY. The project name is always ``fraudstream-config-check``,
so nothing can be confused with a running stack.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "fraudstream-config-check"
ISOLATED = "infrastructure/isolated/docker-compose.isolated.yml"
COMBOS = {
    "default (no profile)": ([], []),
    "profile distributed": (["distributed"], []),
    "profile local": (["local"], []),
    "profile spark": (["spark"], []),
    "profile python-processor": (["python-processor"], []),
    "distributed + isolated override": (["distributed"], [ISOLATED]),
    "local + isolated override": (["local"], [ISOLATED]),
}
EXPECTED = {
    "default (no profile)": {"kafka", "kafka-init", "postgres", "redis"},
    "profile distributed": {"kafka", "kafka-init", "postgres", "redis", "spark", "app-init", "api",
                            "persistence-consumer", "frontend"},
    "profile local": {"kafka", "kafka-init", "postgres", "redis", "api-local", "frontend-local"},
}
# Bind sources that legitimately do not exist in a fresh checkout (gitignored data, created/regenerated at run time)
OPTIONAL_BIND_SOURCES = {"data/synthetic"}


def compose(env_file: str, profiles: list[str], extra: list[str], *args: str) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", "-p", PROJECT, "--env-file", env_file, "-f", "docker-compose.yml"]
    for f in extra:
        cmd += ["-f", f]
    for p in profiles:
        cmd += ["--profile", p]
    return subprocess.run([*cmd, *args], cwd=ROOT, capture_output=True, text=True, timeout=120)


def dockerfile_copies(dockerfile: Path, context: Path) -> list[Path]:
    """Sources of COPY instructions (not multi-stage --from copies)."""
    out = []
    for line in dockerfile.read_text(encoding="utf-8").splitlines():
        m = re.match(r"\s*COPY\s+(?!--from)(?:--\S+\s+)*(.+)$", line)
        if m:
            parts = m.group(1).split()
            out += [context / p for p in parts[:-1] if not p.startswith("$")]
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default=".env.example")
    a = ap.parse_args()
    problems: list[str] = []
    checked: set[Path] = set()
    for name, (profiles, extra) in COMBOS.items():
        r = compose(a.env_file, profiles, extra, "config", "--quiet")
        if r.returncode != 0:
            problems.append(f"{name}: docker compose config failed:\n{r.stderr.strip()}")
            continue
        r = compose(a.env_file, profiles, extra, "config", "--format", "json")
        cfg = json.loads(r.stdout)
        services = set(cfg.get("services", {}))
        if name in EXPECTED and services != EXPECTED[name]:
            problems.append(f"{name}: services {sorted(services)} != expected {sorted(EXPECTED[name])}")
        for svc, spec in cfg["services"].items():
            build = spec.get("build")
            if build:
                ctx = Path(build["context"])
                df = ctx / build.get("dockerfile", "Dockerfile")
                for p in (ctx, df):
                    if not p.exists():
                        problems.append(f"{name}/{svc}: build path missing: {p}")
                if df.exists() and df not in checked:
                    checked.add(df)
                    for src in dockerfile_copies(df, ctx):
                        if not src.exists():
                            problems.append(f"{df.relative_to(ROOT)}: COPY source missing: {src}")
            for v in spec.get("volumes", []):
                if v.get("type") == "bind":
                    src = Path(v["source"])
                    rel = src.relative_to(ROOT).as_posix() if src.is_relative_to(ROOT) else str(src)
                    if not src.exists() and rel not in OPTIONAL_BIND_SOURCES:
                        problems.append(f"{name}/{svc}: bind-mount source missing: {rel}")
                    if rel in (".", "") or Path(rel).name.startswith(".env"):
                        problems.append(f"{name}/{svc}: bind mount exposes {rel or 'the repository root'}")
            for port in spec.get("ports", []):
                if port.get("host_ip") not in ("127.0.0.1",):
                    problems.append(f"{name}/{svc}: port {port.get('published')} not bound to 127.0.0.1")
        print(f"OK  {name}: {len(services)} services")
    if not (ROOT / "frontend" / "nginx" / "default.conf.template").exists():
        problems.append("frontend/nginx/default.conf.template missing")
    if problems:
        print("PROBLEMS:", *problems, sep="\n  ")
        return 1
    print(f"compose configuration valid; {len(checked)} Dockerfiles' COPY sources present")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
