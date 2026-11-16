"""Shared helpers for the operational scripts (backup, restore, isolated stack). Standard library only,
except psycopg, which is only imported where a PostgreSQL connection is needed.

Secrets are read from the selected env file and passed to libraries in memory only: they are never
printed, logged, written into a backup or put on a command line.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
LIVE_PROJECT = "fraudstream"
APP_IMAGE = "fraudstream-app:local"


class OpsError(RuntimeError):
    """A step failed; the message never contains a secret."""


def read_env(path: Path) -> dict[str, str]:
    """Parse a compose-style env file (same inline-comment rule as docker compose / the tests)."""
    out: dict[str, str] = {}
    if not path.exists():
        raise OpsError(f"env file not found: {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        v = v.split(" #", 1)[0].strip()
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def sha256_file(p: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with p.open("rb") as fh:
        while b := fh.read(chunk):
            h.update(b)
    return h.hexdigest()


def run(cmd: Sequence[str], *, check: bool = True, timeout: int = 600, stdin: Any = None, stdout: Any = None,
        text: bool = True) -> subprocess.CompletedProcess:
    r = subprocess.run(list(cmd), cwd=ROOT, stdin=stdin, stdout=stdout if stdout is not None else subprocess.PIPE,
                       stderr=subprocess.PIPE, text=text, timeout=timeout, check=False)
    if check and r.returncode != 0:
        err = r.stderr if isinstance(r.stderr, str) else (r.stderr or b"").decode("utf-8", "replace")
        raise OpsError(f"command failed ({r.returncode}): {' '.join(cmd[:6])} …: {err.strip()[-800:]}")
    return r


class Compose:
    """`docker compose` bound to one project, env file and set of compose files."""

    def __init__(self, project: str, env_file: Path, files: Sequence[Path] = (), profiles: Sequence[str] = ("distributed",)):
        self.project = project
        self.env_file = env_file
        self.files = list(files) or [ROOT / "docker-compose.yml"]
        self.profiles = list(profiles)
        self.env = read_env(env_file)

    def base(self) -> list[str]:
        cmd = ["docker", "compose", "-p", self.project, "--env-file", str(self.env_file)]
        for f in self.files:
            cmd += ["-f", str(f)]
        for p in self.profiles:
            cmd += ["--profile", p]
        return cmd

    def __call__(self, *args: str, **kw: Any) -> subprocess.CompletedProcess:
        return run([*self.base(), *args], **kw)

    def container(self, service: str, *, running_only: bool = True) -> str | None:
        args = ["ps", "-q", service] if running_only else ["ps", "-a", "-q", service]
        out = self(*args, check=False).stdout.strip()
        return out.splitlines()[0] if out else None

    def volume(self, name: str) -> str:
        return f"{self.project}_{name}"

    @property
    def pg(self) -> dict[str, str]:
        e = self.env
        if not e.get("POSTGRES_PASSWORD"):
            raise OpsError(f"POSTGRES_PASSWORD missing in {self.env_file.name}")
        return {"user": e.get("POSTGRES_USER", "fraudstream"), "password": e["POSTGRES_PASSWORD"],
                "db": e.get("POSTGRES_DB", "fraudstream"), "port": e.get("POSTGRES_HOST_PORT", "5433")}

    def pg_connect(self, autocommit: bool = False) -> Any:
        import psycopg

        p = self.pg
        return psycopg.connect(host="127.0.0.1", port=int(p["port"]), user=p["user"], password=p["password"],
                               dbname=p["db"], connect_timeout=5, autocommit=autocommit)


def volume_exists(name: str) -> bool:
    return run(["docker", "volume", "inspect", name], check=False).returncode == 0


def volume_is_empty(name: str) -> bool:
    r = run(["docker", "run", "--rm", "--user", "0", "--entrypoint", "sh", "-v", f"{name}:/v:ro", APP_IMAGE, "-c",
             "ls -A /v | head -1"], check=True)
    return r.stdout.strip() == ""


def refuse_live(project: str, what: str) -> None:
    if project == LIVE_PROJECT:
        raise OpsError(f"refusing to {what} the LIVE project '{LIVE_PROJECT}'. Use an isolated project (-p).")


def table_counts(conn: Any) -> dict[str, int]:
    """Exact row count of every base table in the public schema (inside the caller's transaction)."""
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
                    "AND table_type = 'BASE TABLE' ORDER BY table_name")
        tables = [r[0] for r in cur.fetchall()]
        out = {}
        for t in tables:
            cur.execute(f'SELECT count(*) FROM public."{t}"')  # names come from the catalog, not from input
            out[t] = int(cur.fetchone()[0])
    return out


# Tables whose existing rows the application never rewrites in normal operation (append-only or edited only by
# explicit user actions). Their content fingerprint over `id <= max_id` must survive backup -> restore -> start-up.
# users / refresh_tokens / customers are excluded: logins and new traffic legitimately update existing rows.
FINGERPRINT_TABLES = ("transactions", "predictions", "alerts", "investigations", "investigation_notes", "audit_logs",
                      "generated_reports", "drift_reports", "dead_letters", "model_versions", "model_experiments",
                      "risk_rules", "system_settings", "datasets", "merchants", "simulation_runs")


def table_fingerprints(conn: Any, max_ids: dict[str, int] | None = None) -> dict[str, dict[str, Any]]:
    """md5 over every row (text form, ordered by id) with id <= max_id. Without ``max_ids`` the current max id of
    each table is used (backup time); with it, the same id range is fingerprinted again (restore time)."""
    out: dict[str, dict[str, Any]] = {}
    with conn.cursor() as cur:
        for t in FINGERPRINT_TABLES:
            cur.execute("SELECT to_regclass(%s)", (f"public.{t}",))
            if cur.fetchone()[0] is None:
                continue
            cur.execute("SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_index i JOIN pg_attribute a "
                        "ON a.attrelid = i.indrelid "
                        "AND a.attnum = ANY(i.indkey) WHERE i.indrelid = %s::regclass AND i.indisprimary "
                        "ORDER BY a.attnum", (f"public.{t}",))
            pk_rows = cur.fetchall()
            pk = [r[0] for r in pk_rows]
            order = ", ".join(f'x."{c}"' for c in pk) or "x::text"
            if pk == ["id"] and pk_rows[0][1] in ("integer", "bigint"):            # append-mostly table: fingerprint the id range that existed at backup time
                if max_ids is None:
                    cur.execute(f'SELECT coalesce(max(id), 0) FROM public."{t}"')
                    mx: int | None = int(cur.fetchone()[0])
                elif t in max_ids:
                    mx = max_ids[t]
                else:
                    continue
                cur.execute(f'SELECT count(*), md5(coalesce(string_agg(x::text, %s ORDER BY {order}), %s)) '
                            f'FROM public."{t}" x WHERE x.id <= %s', ("\x1e", "", mx))
            else:                       # small keyed table (e.g. system_settings): whole table
                if max_ids is not None and t not in max_ids:
                    continue
                mx = None
                cur.execute(f'SELECT count(*), md5(coalesce(string_agg(x::text, %s ORDER BY {order}), %s)) '
                            f'FROM public."{t}" x', ("\x1e", ""))
            n, h = cur.fetchone()
            out[t] = {"max_id": mx, "rows": int(n), "md5": h, "key": pk}
    return out


def alembic_revision_pg(conn: Any) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.alembic_version')")
        if cur.fetchone()[0] is None:
            return None
        cur.execute("SELECT version_num FROM alembic_version")
        row = cur.fetchone()
        return row[0] if row else None


def dump_json(p: Path, obj: Any) -> None:
    p.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def say(msg: str) -> None:
    print(msg, flush=True)


def fail(msg: str, code: int = 1) -> None:
    print(f"FAILED: {msg}", file=sys.stderr, flush=True)
    raise SystemExit(code)


def docker_path(p: Path) -> str:
    """Host path usable in `docker run -v` on Windows (Docker Desktop) and POSIX."""
    return str(p.resolve())


def env_flag(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")
