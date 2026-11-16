"""Shared helpers for the Phase 12 benchmarks.

Results are JSON files in benchmarks/results/ (one per run) with the environment they were measured in.
Credentials come from the selected env file (default .env.isolated) or from the environment and are never
written into results or printed.
"""
from __future__ import annotations

import json
import math
import os
import platform
import subprocess
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RESULTS = Path(os.environ.get("BENCH_RESULTS_DIR", ROOT / "benchmarks" / "results"))


def read_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.split(" #", 1)[0].strip().strip('"').strip("'")
    return out


def pctl(values: list[float], p: float) -> float | None:
    """Nearest-rank percentile (p in 0..100). None for an empty list (never 0)."""
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, math.ceil(p / 100 * len(s)) - 1))
    return s[k]


def dist(values: list[float], digits: int = 1) -> dict[str, Any]:
    """Distribution summary; p99 is reported only with >= 100 samples (otherwise it is just the max)."""
    n = len(values)
    if not n:
        return {"n": 0}
    r = lambda x: None if x is None else round(x, digits)  # noqa: E731
    out = {"n": n, "min": r(min(values)), "mean": r(sum(values) / n), "p50": r(pctl(values, 50)),
           "p95": r(pctl(values, 95)), "max": r(max(values))}
    out["p99"] = r(pctl(values, 99)) if n >= 100 else None
    return out


def write_result(name: str, payload: dict[str, Any]) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    p = RESULTS / f"{stamp}-{name}.json"
    p.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return p


def machine_info() -> dict[str, Any]:
    info: dict[str, Any] = {"python": platform.python_version(), "os": platform.platform(),
                            "logical_cpus": os.cpu_count()}
    try:
        import psutil

        info["host_ram_gb"] = round(psutil.virtual_memory().total / 2**30, 1)
    except ImportError:
        pass
    try:
        d = json.loads(subprocess.run(["docker", "info", "--format", "{{json .}}"], capture_output=True, text=True,
                                      timeout=30).stdout)
        info["docker"] = {"server": d.get("ServerVersion"), "vm_cpus": d.get("NCPU"),
                          "vm_mem_gib": round(d.get("MemTotal", 0) / 2**30, 2), "kernel": d.get("KernelVersion"),
                          "os": d.get("OperatingSystem")}
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return info


def _mib(s: str) -> float:
    s = s.strip()
    units = {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "kB": 1 / 1000 / 1.048576, "MB": 1 / 1.048576,
             "GB": 1000 / 1.048576, "B": 1 / 2**20}
    for u in sorted(units, key=len, reverse=True):
        if s.endswith(u):
            return float(s[: -len(u)]) * units[u]
    return float(s or 0)


class DockerStats:
    """Samples `docker stats` for the containers of one compose project in a background thread."""

    def __init__(self, project: str, interval: float = 2.0):
        self.project, self.interval = project, interval
        self.samples: list[dict[str, Any]] = []
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            t = time.time()
            try:
                out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"], capture_output=True,
                                     text=True, timeout=30).stdout
                for line in out.splitlines():
                    d = json.loads(line)
                    name = d.get("Name", "")
                    if not name.startswith(self.project + "-"):
                        continue
                    used = d.get("MemUsage", "0B / 0B").split("/")[0]
                    self.samples.append({"t": t, "service": name[len(self.project) + 1:].rsplit("-", 1)[0],
                                         "cpu_pct": float(d.get("CPUPerc", "0%").rstrip("%") or 0),
                                         "mem_mib": round(_mib(used), 1)})
            except (OSError, ValueError, subprocess.SubprocessError):
                pass
            self._stop.wait(max(0.0, self.interval - (time.time() - t)))

    def __enter__(self) -> DockerStats:
        self._t.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._stop.set()
        self._t.join(timeout=40)

    def summary(self, t0: float | None = None, t1: float | None = None) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for s in {x["service"] for x in self.samples}:
            xs = [x for x in self.samples if x["service"] == s and (t0 is None or x["t"] >= t0)
                  and (t1 is None or x["t"] <= t1)]
            if xs:
                cpu = [x["cpu_pct"] for x in xs]
                mem = [x["mem_mib"] for x in xs]
                out[s] = {"samples": len(xs), "cpu_pct_mean": round(sum(cpu) / len(cpu), 1), "cpu_pct_max": max(cpu),
                          "mem_mib_mean": round(sum(mem) / len(mem), 1), "mem_mib_max": max(mem)}
        return out


def volume_sizes(project: str, volumes: tuple[str, ...] = ("postgres-data", "kafka-data", "redis-data",
                                                           "spark-checkpoints")) -> dict[str, int | None]:
    """Bytes used by each named volume (du inside a throwaway container, volume mounted read-only)."""
    out: dict[str, int | None] = {}
    for v in volumes:
        r = subprocess.run(["docker", "run", "--rm", "--user", "0", "--entrypoint", "du", "-v", f"{project}_{v}:/v:ro",
                            "fraudstream-app:local", "-sb", "/v"], capture_output=True, text=True, timeout=300)
        try:
            out[v] = int(r.stdout.split()[0])
        except (IndexError, ValueError):
            out[v] = None
    return out


def container_limits(project: str) -> dict[str, Any]:
    ids = subprocess.run(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={project}"],
                         capture_output=True, text=True, timeout=30).stdout.split()
    out = {}
    for i in ids:
        d = json.loads(subprocess.run(["docker", "inspect", i], capture_output=True, text=True, timeout=30).stdout)[0]
        svc = d["Config"]["Labels"].get("com.docker.compose.service")
        out[svc] = {"mem_limit_mib": round(d["HostConfig"]["Memory"] / 2**20) if d["HostConfig"]["Memory"] else None,
                    "nano_cpus": d["HostConfig"]["NanoCpus"] or None}
    return out
