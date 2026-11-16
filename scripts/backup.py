"""FraudStream backup (Phase 12).

    python scripts/backup.py                                   # live project, default components
    python scripts/backup.py -p fraudstream-p12 --env-file .env.isolated -f docker-compose.yml \
        -f infrastructure/isolated/docker-compose.isolated.yml --components all --allow-running-streaming
    python scripts/backup.py --verify <backup-dir>             # re-check an existing backup

Components (default: postgres,sqlite,artifacts,datasets,uploads):
  postgres   pg_dump -Fc inside an EXPORTED SNAPSHOT; the per-table row counts in the manifest are taken in the
             same snapshot, so a restore can be compared exactly even while the stack is writing.
  sqlite     local-profile database (volume local-data): SQLite online-backup API + PRAGMA integrity_check.
  artifacts  ml/artifacts (model files, registry) — copied with SHA-256; runtime_config.json is derived and skipped.
  datasets   data/synthetic — copied with SHA-256.
  uploads    volume app-data (uploaded datasets) — tar.gz.
  state      volume app-state (derived runtime_config.json; re-creatable) — tar.gz.
  redis      volume redis-data (AOF: runtime config copy, heartbeats, rate-limit counters; re-creatable) — tar.gz.
  kafka      volume kafka-data — tar.gz. Consistent only while Kafka is STOPPED.
  spark      volume spark-checkpoints — tar.gz. Consistent only while Spark is STOPPED; back up kafka and spark
             together (checkpoint offsets refer to the Kafka log).

Guarantees: the backup is written to `<id>.partial/` and renamed to `<id>/` only after every component succeeded
and every checksum was written; a failure renames it to `<id>.FAILED/` (with FAILED.txt) and exits non-zero.
Nothing is ever overwritten. `.env` (secrets) is NEVER copied: it is re-creatable with scripts/new_env.py (the
login password hashes are in the database, so accounts keep working; only issued tokens become invalid).
The backup itself is SENSITIVE (the database contains password hashes and audit history): keep it out of the
repository and of any public package. Default destination: ../FraudStream-backups (outside the repository).
"""
from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
import sys
import tarfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _ops import (  # noqa: E402
    APP_IMAGE,
    ROOT,
    Compose,
    OpsError,
    alembic_revision_pg,
    docker_path,
    dump_json,
    run,
    say,
    sha256_file,
    table_counts,
    table_fingerprints,
    volume_exists,
)

FORMAT = "fraudstream-backup/1"
DEFAULT_COMPONENTS = ("postgres", "sqlite", "artifacts", "datasets", "uploads")
ALL_COMPONENTS = ("postgres", "sqlite", "artifacts", "datasets", "uploads", "state", "redis", "kafka", "spark")
VOLUME_OF = {"uploads": "app-data", "state": "app-state", "redis": "redis-data", "kafka": "kafka-data",
             "spark": "spark-checkpoints"}
SERVICE_OF = {"kafka": "kafka", "spark": "spark", "redis": "redis"}
SKIP_FILES = {"runtime_config.json", "runtime_config.json.tmp"}

CLASSIFICATION = {
    "postgres": "REQUIRED for recovery; SENSITIVE (password hashes, refresh-token hashes, audit trail)",
    "sqlite": "REQUIRED for the local profile; SENSITIVE (same tables as PostgreSQL)",
    "artifacts": "REQUIRED (trained models; re-training would give different models)",
    "datasets": "REQUIRED for reproducibility (re-creatable with the simulator seed, but checksums would differ)",
    "uploads": "OPTIONAL (uploaded public datasets; re-uploadable)",
    "state": "RE-CREATABLE (runtime_config.json is regenerated from the database at API start-up)",
    "redis": "RE-CREATABLE (runtime config re-published at API start-up; heartbeats/rate limits are transient)",
    "kafka": "OPTIONAL (unprocessed events in flight; processed events are in PostgreSQL)",
    "spark": "OPTIONAL (checkpoints; without them Spark starts a new checkpoint — see SPARK_STARTING_OFFSETS)",
}
NOT_INCLUDED = [
    ".env — secrets are never backed up here. Re-create with `python scripts/new_env.py` (new random values) or "
    "restore it from your own secret store. Login password hashes live in the database, so accounts keep working.",
    "Docker images — rebuilt from the repository (`docker compose build`).",
    "Source code — in the repository / project archive, not in data backups.",
]

SQLITE_SNIPPET = r"""
import glob, json, os, shutil, sqlite3, sys
src, out, copy_first = sys.argv[1], sys.argv[2], sys.argv[3] == "1"
if not os.path.exists(src):
    print(json.dumps({"missing": True})); sys.exit(0)
if copy_first:  # read-only volume of a stopped container: copy db + -wal/-shm, then back up the copy
    os.makedirs("/tmp/fsbk", exist_ok=True)
    for f in glob.glob(src + "*"):
        shutil.copy2(f, "/tmp/fsbk")
    src = os.path.join("/tmp/fsbk", os.path.basename(src))
s = sqlite3.connect(src); d = sqlite3.connect(out); s.backup(d); d.close(); s.close()
c = sqlite3.connect(out)
ic = c.execute("PRAGMA integrity_check").fetchone()[0]
tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
counts = {t: c.execute('SELECT count(*) FROM "%s"' % t.replace('"', '""')).fetchone()[0] for t in tables}
rev = c.execute("SELECT version_num FROM alembic_version").fetchone()[0] if "alembic_version" in tables else None
c.close()
print(json.dumps({"integrity_check": ic, "table_counts": counts, "alembic_revision": rev}))
"""


class Backup:
    def __init__(self, a: argparse.Namespace):
        self.a = a
        self.compose = Compose(a.project, Path(a.env_file), [Path(f) for f in a.file] if a.file else (),
                               profiles=("distributed", "local", "spark"))
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        self.id = f"{stamp}-{a.project}" + (f"-{a.label}" if a.label else "")
        self.dest_root = Path(a.dest).resolve()
        self.final = self.dest_root / self.id
        self.work = self.dest_root / f"{self.id}.partial"
        self.manifest: dict[str, Any] = {
            "format": FORMAT, "backup_id": self.id, "status": "IN_PROGRESS",
            "created_at": datetime.now(UTC).isoformat(), "source_project": a.project,
            "tool": {"python": platform.python_version(), "platform": platform.system()},
            "components": {}, "classification": {}, "not_included": NOT_INCLUDED,
        }

    # ------------------------------------------------------------------ helpers
    def _file_entry(self, p: Path) -> dict[str, Any]:
        return {"file": p.relative_to(self.work).as_posix(), "bytes": p.stat().st_size, "sha256": sha256_file(p)}

    # ------------------------------------------------------------------ components
    def postgres(self) -> dict[str, Any]:
        if not self.compose.container("postgres"):
            raise OpsError("postgres service is not running")
        pg = self.compose.pg
        out = self.work / "postgres" / f"{pg['db']}.dump"
        out.parent.mkdir(parents=True)
        conn = self.compose.pg_connect()
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                cur.execute("SELECT pg_export_snapshot(), current_setting('server_version')")
                snapshot, server_version = cur.fetchone()
            counts = table_counts(conn)               # same snapshot as the dump below
            fingerprints = table_fingerprints(conn)
            revision = alembic_revision_pg(conn)
            t0 = time.perf_counter()
            with out.open("wb") as fh:
                r = subprocess.run([*self.compose.base(), "exec", "-T", "postgres", "pg_dump", "-Fc", "--no-password",
                                    f"--snapshot={snapshot}", "-U", pg["user"], "-d", pg["db"]],
                                   cwd=ROOT, stdout=fh, stderr=subprocess.PIPE, timeout=1800, check=False)
            if r.returncode != 0:
                raise OpsError(f"pg_dump failed: {r.stderr.decode('utf-8', 'replace').strip()[-500:]}")
            seconds = round(time.perf_counter() - t0, 2)
        finally:
            conn.rollback()
            conn.close()
        if out.stat().st_size == 0:
            raise OpsError("pg_dump produced an empty file")
        # integrity: the archive must be readable by pg_restore (throwaway container, backup mounted read-only)
        lst = run(["docker", "run", "--rm", "-v", f"{docker_path(out.parent)}:/b:ro", "postgres:16-alpine",
                   "pg_restore", "--list", f"/b/{out.name}"]).stdout
        data_entries = sum(1 for line in lst.splitlines() if " TABLE DATA " in line)
        tables_with_data = {line.split(" TABLE DATA public ")[1].split()[0]
                            for line in lst.splitlines() if " TABLE DATA public " in line}
        missing = sorted(set(counts) - tables_with_data)
        if missing:
            raise OpsError(f"dump has no TABLE DATA entry for {missing}")
        ver = run([*self.compose.base(), "exec", "-T", "postgres", "pg_dump", "--version"]).stdout.strip()
        return {**self._file_entry(out), "format": "pg_dump custom (-Fc)", "pg_dump_version": ver,
                "server_version": server_version, "consistency": "single exported snapshot (REPEATABLE READ)",
                "dump_seconds": seconds, "table_counts": counts, "alembic_revision": revision,
                "pg_restore_list_table_data_entries": data_entries, "fingerprints": fingerprints}

    def sqlite(self) -> dict[str, Any]:
        vol = self.compose.volume("local-data")
        if not volume_exists(vol):
            return {"skipped": f"volume {vol} does not exist (local profile never started)"}
        out_dir = self.work / "sqlite"
        out_dir.mkdir(parents=True)
        out = out_dir / "fraudstream-local.db"
        running = self.compose.container("api-local")
        if running:   # consistent online backup inside the running container
            r = subprocess.run(["docker", "exec", "-i", running, "python", "-", "/data/db/fraudstream-local.db",
                                "/tmp/fs-backup.db", "0"], input=SQLITE_SNIPPET, capture_output=True, text=True,
                               timeout=600, check=False)
            if r.returncode != 0:
                raise OpsError(f"sqlite backup failed: {r.stderr.strip()[-500:]}")
            run(["docker", "cp", f"{running}:/tmp/fs-backup.db", str(out)])
            run(["docker", "exec", running, "rm", "-f", "/tmp/fs-backup.db"], check=False)
            mode = "online backup API inside the running api-local container"
        else:
            r = subprocess.run(["docker", "run", "--rm", "-i", "--entrypoint", "python", "-v", f"{vol}:/src:ro",
                                "-v", f"{docker_path(out_dir)}:/out", APP_IMAGE, "-", "/src/fraudstream-local.db",
                                "/out/fraudstream-local.db", "1"], input=SQLITE_SNIPPET, capture_output=True,
                               text=True, timeout=600, check=False)
            if r.returncode != 0:
                raise OpsError(f"sqlite backup failed: {r.stderr.strip()[-500:]}")
            mode = "backup API on a copy of the stopped container's files (volume mounted read-only)"
        info = json.loads(r.stdout.strip().splitlines()[-1])
        if info.get("missing"):
            shutil.rmtree(out_dir)
            return {"skipped": "no fraudstream-local.db in the volume"}
        if info["integrity_check"] != "ok":
            raise OpsError(f"sqlite integrity_check: {info['integrity_check']}")
        return {**self._file_entry(out), "mode": mode, **info}

    def files(self, name: str, src: Path) -> dict[str, Any]:
        if not src.is_dir():
            raise OpsError(f"{name}: source directory {src} not found")
        dst_root = self.work / "files" / name
        entries = []
        for p in sorted(src.rglob("*")):
            if not p.is_file() or p.name in SKIP_FILES or "__pycache__" in p.parts:
                continue
            rel = p.relative_to(src)
            dst = dst_root / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, dst)
            src_sha, dst_sha = sha256_file(p), sha256_file(dst)
            if src_sha != dst_sha:
                raise OpsError(f"{name}: copy of {rel} differs from the source")
            entries.append({"path": rel.as_posix(), "bytes": dst.stat().st_size, "sha256": dst_sha,
                            "file": dst.relative_to(self.work).as_posix()})
        if not entries:
            raise OpsError(f"{name}: no files in {src}")
        return {"source": str(src), "file_count": len(entries), "bytes": sum(e["bytes"] for e in entries),
                "files": entries, "skipped_derived": sorted(SKIP_FILES)}

    def volume(self, comp: str) -> dict[str, Any]:
        vol = self.compose.volume(VOLUME_OF[comp])
        if not volume_exists(vol):
            raise OpsError(f"volume {vol} does not exist")
        svc = SERVICE_OF.get(comp)
        live = bool(svc and self.compose.container(svc))
        if comp in ("kafka", "spark") and live and not self.a.allow_running_streaming:
            raise OpsError(f"{comp}: service is running - a volume copy would be crash-consistent at best. Stop it "
                           f"first or pass --allow-running-streaming (recorded in the manifest).")
        out_dir = self.work / "volumes"
        out_dir.mkdir(parents=True, exist_ok=True)
        name = f"{VOLUME_OF[comp]}.tar.gz"
        run(["docker", "run", "--rm", "--user", "0", "--entrypoint", "tar", "-v", f"{vol}:/v:ro",
             "-v", f"{docker_path(out_dir)}:/out", APP_IMAGE, "--numeric-owner", "-czf", f"/out/{name}", "-C", "/v", "."],
            timeout=3600)
        p = out_dir / name
        with tarfile.open(p, "r:gz") as tf:           # integrity: archive must be fully readable
            members = tf.getmembers()
        n_files = sum(1 for m in members if m.isfile())
        return {**self._file_entry(p), "volume": vol, "file_count": n_files,
                "consistency": ("crash-consistent (service running)" if live else "consistent (service stopped)")
                if svc else "consistent (no writer service)"}

    def config(self) -> None:
        cfg = self.work / "config"
        cfg.mkdir()
        for rel in ("docker-compose.yml", ".env.example", "infrastructure/kafka/create_topics.sh"):
            shutil.copy2(ROOT / rel, cfg / Path(rel).name)
        keys = [line.split("=", 1)[0].strip() for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.strip().startswith("#") and "=" in line]
        self.manifest["required_env_keys"] = keys
        self.manifest["config_files"] = [self._file_entry(p) for p in sorted(cfg.iterdir())]

    # ------------------------------------------------------------------ driver
    def run(self) -> int:
        comps = ALL_COMPONENTS if self.a.components == "all" else tuple(c.strip() for c in self.a.components.split(","))
        bad = sorted(set(comps) - set(ALL_COMPONENTS))
        if bad:
            raise OpsError(f"unknown components {bad}; choose from {list(ALL_COMPONENTS)}")
        if ("kafka" in comps) != ("spark" in comps):
            say("WARNING: kafka and spark are backed up separately; restoring only one of them gives "
                "checkpoint offsets that do not match the Kafka log.")
        self.dest_root.mkdir(parents=True, exist_ok=True)
        for p in (self.final, self.work, self.dest_root / f"{self.id}.FAILED"):
            if p.exists():
                raise OpsError(f"{p} already exists - refusing to overwrite")
        self.work.mkdir()
        say(f"backup {self.id} -> {self.final}")
        t_start = time.perf_counter()
        try:
            for c in comps:
                t0 = time.perf_counter()
                if c == "postgres":
                    res = self.postgres()
                elif c == "sqlite":
                    res = self.sqlite()
                elif c == "artifacts":
                    res = self.files("artifacts", Path(self.a.artifacts_dir))
                elif c == "datasets":
                    res = self.files("datasets", Path(self.a.datasets_dir))
                else:
                    res = self.volume(c)
                res["seconds"] = round(time.perf_counter() - t0, 2)
                self.manifest["components"][c] = res
                self.manifest["classification"][c] = CLASSIFICATION[c]
                say(f"  OK  {c:<9} {res.get('skipped') or self._summary(res)}")
            self.config()
            self.manifest["status"] = "COMPLETE"
            self.manifest["completed_at"] = datetime.now(UTC).isoformat()
            self.manifest["duration_seconds"] = round(time.perf_counter() - t_start, 2)
            dump_json(self.work / "manifest.json", self.manifest)
            write_sums(self.work)
            problems = verify_dir(self.work)
            if problems:
                raise OpsError("post-write verification failed: " + "; ".join(problems[:5]))
            self.work.rename(self.final)
        except BaseException as exc:
            failed = self.dest_root / f"{self.id}.FAILED"
            try:
                (self.work / "FAILED.txt").write_text(f"{datetime.now(UTC).isoformat()} {exc}\n", encoding="utf-8")
                self.work.rename(failed)
            except OSError:
                pass
            say(f"FAILED: {exc}\nThe incomplete backup was renamed to {failed} - it must not be used for a restore.")
            return 1
        say(f"COMPLETE: {self.final} ({self.manifest['duration_seconds']} s) - verified "
            f"{len((self.final / 'SHA256SUMS').read_text().splitlines())} checksums")
        return 0

    @staticmethod
    def _summary(res: dict[str, Any]) -> str:
        bits = []
        if "bytes" in res:
            bits.append(f"{res['bytes'] / 1e6:.2f} MB")
        if "file_count" in res:
            bits.append(f"{res['file_count']} files")
        if "table_counts" in res:
            tc = res["table_counts"]
            bits.append(", ".join(f"{k}={tc[k]}" for k in ("transactions", "alerts", "investigations",
                                                           "generated_reports", "audit_log") if k in tc))
        if "alembic_revision" in res:
            bits.append(f"alembic {res['alembic_revision']}")
        return " | ".join(bits)


def write_sums(d: Path) -> None:
    lines = [f"{sha256_file(p)}  {p.relative_to(d).as_posix()}" for p in sorted(d.rglob("*"))
             if p.is_file() and p.name not in ("SHA256SUMS", "FAILED.txt")]
    (d / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")


def verify_dir(d: Path) -> list[str]:
    """Every file listed in SHA256SUMS exists with that hash, no unlisted file exists, manifest is COMPLETE
    and its per-file hashes agree."""
    problems: list[str] = []
    sums = d / "SHA256SUMS"
    if not sums.exists():
        return ["SHA256SUMS missing"]
    listed = {}
    for line in sums.read_text(encoding="utf-8").splitlines():
        if line.strip():
            h, rel = line.split("  ", 1)
            listed[rel] = h
    for rel, h in listed.items():
        p = d / rel
        if not p.exists():
            problems.append(f"missing {rel}")
        elif sha256_file(p) != h:
            problems.append(f"checksum mismatch {rel}")
    actual = {p.relative_to(d).as_posix() for p in d.rglob("*") if p.is_file() and p.name != "SHA256SUMS"}
    extra = sorted(actual - set(listed))
    if extra:
        problems.append(f"unlisted files {extra[:5]}")
    try:
        m = json.loads((d / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return [*problems, f"manifest unreadable ({exc.__class__.__name__})"]
    if m.get("format") != FORMAT or m.get("status") != "COMPLETE":
        problems.append(f"manifest status {m.get('status')!r} / format {m.get('format')!r}")
    for name, comp in m.get("components", {}).items():
        for e in ([comp] if "file" in comp else []) + comp.get("files", []):
            if listed.get(e["file"]) != e["sha256"]:
                problems.append(f"{name}: manifest hash differs for {e['file']}")
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-p", "--project", default="fraudstream")
    ap.add_argument("--env-file", default=str(ROOT / ".env"))
    ap.add_argument("-f", "--file", action="append", help="compose file(s); default docker-compose.yml")
    ap.add_argument("--dest", default=str(ROOT.parent / "FraudStream-backups"))
    ap.add_argument("--components", default=",".join(DEFAULT_COMPONENTS), help="comma list or 'all'")
    ap.add_argument("--artifacts-dir", default=str(ROOT / "ml" / "artifacts"))
    ap.add_argument("--datasets-dir", default=str(ROOT / "data" / "synthetic"))
    ap.add_argument("--label", default="", help="suffix for the backup id (letters, digits, -)")
    ap.add_argument("--allow-running-streaming", action="store_true")
    ap.add_argument("--verify", metavar="DIR", help="verify an existing backup and exit")
    a = ap.parse_args(argv)
    if a.verify:
        problems = verify_dir(Path(a.verify))
        if problems:
            say("VERIFY FAILED:\n  " + "\n  ".join(problems))
            return 1
        n = len((Path(a.verify) / "SHA256SUMS").read_text().splitlines())
        say(f"VERIFY OK: {a.verify} ({n} files, manifest COMPLETE)")
        return 0
    if a.label and not all(c.isalnum() or c == "-" for c in a.label):
        ap.error("--label: letters, digits and '-' only")
    try:
        return Backup(a).run()
    except OpsError as exc:
        say(f"FAILED: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
