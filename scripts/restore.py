"""FraudStream restore (Phase 12). Restores a backup made by scripts/backup.py and VERIFIES it against the
backup manifest. It never overwrites anything: every target (database, volume, directory) must be empty.

    # isolated project (recommended; see docs/phase12/BACKUP_AND_RESTORE.md for the full sequence)
    python scripts/restore.py --backup <dir> -p fraudstream-p12 --env-file .env.isolated \
        -f docker-compose.yml -f infrastructure/isolated/docker-compose.isolated.yml \
        --components postgres,sqlite,uploads --files-to <empty dir> --report restore-report.json

Components: postgres, sqlite, artifacts, datasets, uploads, state, redis, kafka, spark (default: everything in the
backup except state/redis, which are re-creatable). `artifacts`/`datasets` need --files-to (an empty directory;
point ARTIFACTS_HOST_DIR / DATASETS_HOST_DIR at it to run the stack on the restored copy).

Safety: the LIVE project ('fraudstream') is refused unless --allow-live-project is given — and even then the target
database and volumes must be empty (disaster recovery onto fresh volumes). PostgreSQL is restored in a single
transaction (all or nothing). Exit code 0 only when every restored item matches the manifest.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _ops import (  # noqa: E402
    APP_IMAGE,
    LIVE_PROJECT,
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
    volume_is_empty,
)
from backup import SERVICE_OF, VOLUME_OF, verify_dir  # noqa: E402

SQLITE_RESTORE = r"""
import json, os, shutil, sqlite3, sys
src, dst = sys.argv[1], sys.argv[2]
if os.path.exists(dst):
    print(json.dumps({"error": "target database file already exists"})); sys.exit(3)
shutil.copy2(src, dst + ".restoring")
os.replace(dst + ".restoring", dst)
c = sqlite3.connect(dst)
ic = c.execute("PRAGMA integrity_check").fetchone()[0]
tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
counts = {t: c.execute('SELECT count(*) FROM "%s"' % t.replace('"', '""')).fetchone()[0] for t in tables}
rev = c.execute("SELECT version_num FROM alembic_version").fetchone()[0] if "alembic_version" in tables else None
c.close()
print(json.dumps({"integrity_check": ic, "table_counts": counts, "alembic_revision": rev}))
"""


def wait_pg(compose: Compose, timeout: float = 120) -> None:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            compose.pg_connect(autocommit=True).close()
            return
        except Exception:  # noqa: BLE001 - starting up
            time.sleep(2)
    raise OpsError("target PostgreSQL did not accept connections")


class Restore:
    def __init__(self, a: argparse.Namespace):
        self.a = a
        self.backup = Path(a.backup).resolve()
        self.compose = Compose(a.project, Path(a.env_file), [Path(f) for f in a.file] if a.file else (),
                               profiles=("distributed", "local", "spark"))
        self.manifest: dict[str, Any] = {}
        self.report: dict[str, Any] = {"backup_id": None, "target_project": a.project,
                                       "started_at": datetime.now(UTC).isoformat(), "components": {}}

    def check_backup(self) -> None:
        problems = verify_dir(self.backup)
        if problems:
            raise OpsError("backup verification failed (refusing to restore): " + "; ".join(problems[:5]))
        self.manifest = json.loads((self.backup / "manifest.json").read_text(encoding="utf-8"))
        self.report["backup_id"] = self.manifest["backup_id"]
        say(f"backup {self.manifest['backup_id']} verified ({len((self.backup / 'SHA256SUMS').read_text().splitlines())} checksums)")

    # ------------------------------------------------------------------ components
    def postgres(self, m: dict[str, Any]) -> dict[str, Any]:
        self.compose("up", "-d", "--no-deps", "postgres", timeout=300)
        wait_pg(self.compose)
        conn = self.compose.pg_connect(autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")
                if cur.fetchone()[0]:
                    raise OpsError("target database is not empty - refusing to restore over existing data")
        finally:
            conn.close()
        pg = self.compose.pg
        t0 = time.perf_counter()
        with (self.backup / m["file"]).open("rb") as fh:
            r = subprocess.run([*self.compose.base(), "exec", "-T", "postgres", "pg_restore", "--no-owner",
                                f"--role={pg['user']}", "--exit-on-error", "--single-transaction", "--no-password",
                                "-U", pg["user"], "-d", pg["db"]], cwd=ROOT, stdin=fh, capture_output=True,
                               timeout=3600, check=False)
        if r.returncode != 0:
            raise OpsError(f"pg_restore failed (nothing committed): {r.stderr.decode('utf-8', 'replace').strip()[-600:]}")
        seconds = round(time.perf_counter() - t0, 2)
        conn = self.compose.pg_connect()
        try:
            counts = table_counts(conn)
            revision = alembic_revision_pg(conn)
            fp_src = m.get("fingerprints") or {}
            fp = table_fingerprints(conn, {t: v["max_id"] for t, v in fp_src.items()})
            conn.rollback()
            with conn.cursor() as cur:
                conn.autocommit = True
                cur.execute("ANALYZE")
        finally:
            conn.close()
        diff = {t: {"backup": m["table_counts"].get(t), "restored": counts.get(t)}
                for t in sorted(set(counts) | set(m["table_counts"])) if counts.get(t) != m["table_counts"].get(t)}
        fp_diff = sorted(t for t in fp_src if fp.get(t) != fp_src[t])
        return {"restore_seconds": seconds, "table_counts": counts, "alembic_revision": revision,
                "fingerprints_compared": len(fp_src), "fingerprint_mismatches": fp_diff,
                "alembic_matches": revision == m.get("alembic_revision"), "count_mismatches": diff,
                "ok": not diff and not fp_diff and revision == m.get("alembic_revision")}

    def sqlite(self, m: dict[str, Any]) -> dict[str, Any]:
        self.compose("create", "--no-recreate", "api-local", timeout=300)     # creates the compose volume
        vol = self.compose.volume("local-data")
        r = subprocess.run(["docker", "run", "--rm", "-i", "--entrypoint", "python", "-v", f"{vol}:/data/db",
                            "-v", f"{docker_path((self.backup / m['file']).parent)}:/b:ro", APP_IMAGE, "-",
                            f"/b/{Path(m['file']).name}", "/data/db/fraudstream-local.db"],
                           input=SQLITE_RESTORE, capture_output=True, text=True, timeout=600, check=False)
        out = json.loads(r.stdout.strip().splitlines()[-1]) if r.stdout.strip() else {}
        if r.returncode != 0 or "error" in out:
            raise OpsError(f"sqlite restore failed: {out.get('error') or r.stderr.strip()[-400:]}")
        diff = {t: {"backup": m["table_counts"].get(t), "restored": out["table_counts"].get(t)}
                for t in sorted(set(out["table_counts"]) | set(m["table_counts"]))
                if out["table_counts"].get(t) != m["table_counts"].get(t)}
        return {**out, "count_mismatches": diff,
                "ok": out["integrity_check"] == "ok" and not diff and out["alembic_revision"] == m.get("alembic_revision")}

    def files(self, name: str, m: dict[str, Any]) -> dict[str, Any]:
        if not self.a.files_to:
            raise OpsError(f"{name}: --files-to <empty directory> is required")
        dst_root = Path(self.a.files_to).resolve() / name
        if dst_root.exists() and any(dst_root.iterdir()):
            raise OpsError(f"{name}: {dst_root} is not empty - refusing to overwrite")
        bad = []
        for e in m["files"]:
            dst = dst_root / e["path"]
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.backup / e["file"], dst)
            if sha256_file(dst) != e["sha256"]:
                bad.append(e["path"])
        return {"target": str(dst_root), "file_count": len(m["files"]), "checksum_mismatches": bad, "ok": not bad}

    def volume(self, comp: str, m: dict[str, Any]) -> dict[str, Any]:
        svc = {"uploads": "api", "state": "api", "redis": "redis", "kafka": "kafka", "spark": "spark"}[comp]
        if SERVICE_OF.get(comp) and self.compose.container(SERVICE_OF[comp]):
            raise OpsError(f"{comp}: target service is running - stop it before restoring its volume")
        self.compose("create", "--no-recreate", svc, timeout=300)  # creates the compose volume
        vol = self.compose.volume(VOLUME_OF[comp])
        if not volume_exists(vol):
            raise OpsError(f"{comp}: volume {vol} was not created")
        if not volume_is_empty(vol):
            raise OpsError(f"{comp}: volume {vol} is not empty - refusing to overwrite")
        src = self.backup / m["file"]
        run(["docker", "run", "--rm", "--user", "0", "--entrypoint", "tar", "-v", f"{vol}:/v",
             "-v", f"{docker_path(src.parent)}:/b:ro", APP_IMAGE, "--numeric-owner", "-xzf", f"/b/{src.name}", "-C", "/v"],
            timeout=3600)
        n = run(["docker", "run", "--rm", "--user", "0", "--entrypoint", "sh", "-v", f"{vol}:/v:ro", APP_IMAGE, "-c",
                 "find /v -type f | wc -l"]).stdout.strip()
        ok = int(n) == m["file_count"]
        return {"volume": vol, "file_count": int(n), "expected_file_count": m["file_count"], "ok": ok}

    # ------------------------------------------------------------------ driver
    def run(self) -> int:
        if self.a.project == LIVE_PROJECT and not self.a.allow_live_project:
            raise OpsError(f"refusing to restore into the LIVE project '{LIVE_PROJECT}'. Restore into an isolated "
                           "project; for disaster recovery onto fresh volumes pass --allow-live-project.")
        self.check_backup()
        available = self.manifest["components"]
        if self.a.components:
            comps = [c.strip() for c in self.a.components.split(",")]
        else:
            comps = [c for c in available if c not in ("state", "redis") and "skipped" not in available[c]
                     and (c not in ("artifacts", "datasets") or self.a.files_to)]
        missing = [c for c in comps if c not in available or "skipped" in available[c]]
        if missing:
            raise OpsError(f"backup does not contain {missing}")
        all_ok = True
        for c in comps:
            m = available[c]
            t0 = time.perf_counter()
            try:
                if c == "postgres":
                    res = self.postgres(m)
                elif c == "sqlite":
                    res = self.sqlite(m)
                elif c in ("artifacts", "datasets"):
                    res = self.files(c, m)
                else:
                    res = self.volume(c, m)
            except OpsError as exc:
                res = {"ok": False, "error": str(exc)}
            res["seconds"] = round(time.perf_counter() - t0, 2)
            self.report["components"][c] = res
            all_ok &= bool(res.get("ok"))
            say(f"  {'OK  ' if res.get('ok') else 'FAIL'} {c:<9} " + (res.get("error") or self._summary(res)))
        self.report["completed_at"] = datetime.now(UTC).isoformat()
        self.report["result"] = "PASS" if all_ok else "FAIL"
        if self.a.report:
            dump_json(Path(self.a.report), self.report)
        say(f"RESTORE {self.report['result']}" + (f" (report: {self.a.report})" if self.a.report else ""))
        return 0 if all_ok else 1

    @staticmethod
    def _summary(res: dict[str, Any]) -> str:
        if "table_counts" in res:
            tc = res["table_counts"]
            return (", ".join(f"{k}={tc.get(k)}" for k in ("transactions", "alerts", "investigations", "generated_reports",
                                                          "audit_logs")) +
                    f" | alembic {res['alembic_revision']} | count mismatches {len(res['count_mismatches'])}"
                    + (f" | fingerprints {res['fingerprints_compared'] - len(res['fingerprint_mismatches'])}"
                       f"/{res['fingerprints_compared']} identical" if "fingerprints_compared" in res else ""))
        if "checksum_mismatches" in res:
            return f"{res['file_count']} files, checksum mismatches {len(res['checksum_mismatches'])}"
        return f"{res.get('file_count')} files (expected {res.get('expected_file_count')})"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--backup", required=True)
    ap.add_argument("-p", "--project", required=True)
    ap.add_argument("--env-file", required=True)
    ap.add_argument("-f", "--file", action="append")
    ap.add_argument("--components", default="")
    ap.add_argument("--files-to", default="")
    ap.add_argument("--report", default="")
    ap.add_argument("--allow-live-project", action="store_true")
    a = ap.parse_args(argv)
    try:
        return Restore(a).run()
    except OpsError as exc:
        say(f"FAILED: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
