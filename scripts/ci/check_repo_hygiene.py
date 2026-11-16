"""Fail if files that must never be published are part of the repository.

    python scripts/ci/check_repo_hygiene.py

Checks the files Git would publish (``git ls-files --cached --others --exclude-standard``: tracked + untracked but
not ignored). It needs a Git work tree - in CI that is the checkout.

Fails on:
  * env files other than .env.example; .dev-credentials*; private keys / certificates;
  * databases and dumps (*.db, *.sqlite*, *.dump, *.backup, pg_dump custom archives), backup/restore directories;
  * Argon2 password hashes or PEM private-key blocks inside any text file;
  * .env.example containing anything but placeholder secrets;
  * gitignore rules that keep secrets/data out being removed.
Prints file names and the rule only - never file contents.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FORBIDDEN_NAME = [
    (re.compile(r"(^|/)\.env($|\.(?!example$)[^/]+$)"), "env file (secrets)"),
    (re.compile(r"(^|/)\.dev-credentials"), "development credentials"),
    (re.compile(r"\.(db|sqlite|sqlite3|db-wal|db-shm|dump|backup|bak)$", re.I), "database / dump / backup file"),
    (re.compile(r"\.(pem|key|p12|pfx|jks)$", re.I), "key / certificate"),
    (re.compile(r"(^|/)(backups|FraudStream-backups|p12-restore)/"), "backup / restore directory"),
    (re.compile(r"\.(FAILED|partial)/"), "partial / failed backup"),
]
CONTENT = [
    (re.compile(rb"\$argon2(id|i|d)\$v=\d+\$m=\d+"), "Argon2 password hash"),
    (re.compile(rb"-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"), "PEM private key"),
    (re.compile(rb"PGDMP"), "PostgreSQL custom-format dump header"),
]
REQUIRED_IGNORES = [".env", ".env.*", "!.env.example", ".dev-credentials.txt", "data/", "*.db", "backups/",
                    "FraudStream-backups/", "p12-restore/"]
PLACEHOLDER_KEYS = ("JWT_SECRET", "POSTGRES_PASSWORD", "REDIS_PASSWORD", "FRAUDSTREAM_ADMIN_PASSWORD",
                    "FRAUDSTREAM_ANALYST_PASSWORD")


def published_files() -> list[str]:
    r = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT,
                       capture_output=True)
    if r.returncode != 0:
        sys.exit("not a Git work tree: run this in the repository checkout (it lists what Git would publish)")
    return sorted({p for p in r.stdout.decode().split("\0") if p})


def main() -> int:
    problems: list[str] = []
    files = published_files()
    for rel in files:
        for rx, why in FORBIDDEN_NAME:
            if rx.search(rel):
                problems.append(f"{rel}: {why}")
        p = ROOT / rel
        if p.is_file() and p.stat().st_size < 20_000_000:
            data = p.read_bytes()
            for rx, why in CONTENT:
                # PGDMP only counts as a dump header at the start of a file
                if rx.pattern == rb"PGDMP" and not data.startswith(b"PGDMP"):
                    continue
                if rx.search(data):
                    problems.append(f"{rel}: contains {why}")
    ignore = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    for rule in REQUIRED_IGNORES:
        if rule not in [ln.strip() for ln in ignore]:
            problems.append(f".gitignore: rule '{rule}' missing")
    example = ROOT / ".env.example"
    for line in example.read_text(encoding="utf-8").splitlines():
        k, _, v = line.partition("=")
        v = v.split("#")[0].strip()
        if k.strip() in PLACEHOLDER_KEYS and not v.startswith("change-me"):
            problems.append(f".env.example: {k.strip()} is not a 'change-me' placeholder")
    print(f"checked {len(files)} publishable files")
    if problems:
        print("PROBLEMS:", *problems, sep="\n  ")
        return 1
    print("no env files, credentials, databases, dumps, backups, password hashes or private keys")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
