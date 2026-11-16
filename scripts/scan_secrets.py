"""Scan the repository (or any directory / ZIP) for secret VALUES and secret-bearing files (Phase 12).

    python scripts/scan_secrets.py                     # repository, secrets from .env and .env.isolated
    python scripts/scan_secrets.py --path some.zip     # a package to be published

Checks: (1) the literal values of every secret key (JWT_SECRET, *_PASSWORD) from the given env files, and the
URL-encoded forms, appear in no file other than the env files themselves; (2) no file named .env / .env.* (except
.env.example), *.dump, *.db, backup manifests, private keys or .dev-credentials.txt is present (packaging check
with --strict); (3) private-key headers. Prints file paths and key NAMES only, never a value. Exit 1 on findings.
"""
from __future__ import annotations

import argparse
import fnmatch
import sys
import urllib.parse
import zipfile
from collections.abc import Iterator
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _ops import ROOT, read_env  # noqa: E402

SECRET_KEYS = ("JWT_SECRET", "POSTGRES_PASSWORD", "REDIS_PASSWORD", "FRAUDSTREAM_ADMIN_PASSWORD",
               "FRAUDSTREAM_ANALYST_PASSWORD")
SKIP_DIRS = {".venv", "node_modules", ".git", "__pycache__", ".pytest_cache", ".ruff_cache"}
SENSITIVE_NAMES = (".env", ".env.*", "*.dump", "*.db", "*.db-wal", "*.db-shm", ".dev-credentials.txt", "*.pem", "*.key",
                   "SHA256SUMS")
ALLOWED_NAMES = (".env.example",)
_DASH = b"-" * 5   # built at runtime so this file does not match itself
KEY_HEADERS = tuple(_DASH + b"BEGIN " + k + b"PRIVATE KEY" for k in (b"RSA ", b"", b"OPENSSH ", b"EC "))


def files(root: Path) -> Iterator[tuple[str, bytes]]:
    if root.suffix.lower() == ".zip":
        with zipfile.ZipFile(root) as z:
            for n in z.namelist():
                if not n.endswith("/"):
                    yield n, z.read(n)
        return
    for p in root.rglob("*"):
        if p.is_file() and not (set(p.relative_to(root).parts) & SKIP_DIRS):
            try:
                if p.stat().st_size > 50_000_000:
                    continue
                yield p.relative_to(root).as_posix(), p.read_bytes()
            except OSError:
                continue


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--path", default=str(ROOT))
    ap.add_argument("--env-file", action="append", help="default: .env and .env.isolated (if present)")
    ap.add_argument("--strict", action="store_true", help="also fail on secret-bearing FILE NAMES (packaging check)")
    a = ap.parse_args()
    env_files = [Path(f) for f in a.env_file] if a.env_file else [p for p in (ROOT / ".env", ROOT / ".env.isolated")
                                                                    if p.exists()]
    needles: dict[bytes, str] = {}
    for ef in env_files:
        env = read_env(ef)
        for k in SECRET_KEYS:
            v = env.get(k, "")
            if len(v) >= 8 and not v.startswith("change-me"):
                needles[v.encode()] = f"{ef.name}:{k}"
                enc = urllib.parse.quote(v, safe="")
                if enc != v:
                    needles[enc.encode()] = f"{ef.name}:{k} (url-encoded)"
    root = Path(a.path)
    own = {p.resolve() for p in env_files}
    findings: list[str] = []
    scanned = 0
    for rel, data in files(root):
        scanned += 1
        name = rel.rsplit("/", 1)[-1]
        if root.suffix.lower() != ".zip" and (root / rel).resolve() in own:
            continue
        if any(fnmatch.fnmatch(name, pat) for pat in SENSITIVE_NAMES) and name not in ALLOWED_NAMES:
            if a.strict:
                findings.append(f"sensitive file present: {rel}")
            if name == ".env" or name.startswith(".env."):
                continue                              # env files hold secrets by design; everything else is scanned
        for needle, label in needles.items():
            if needle in data:
                findings.append(f"secret value of {label} found in {rel}")
        for h in KEY_HEADERS:
            if h in data:
                findings.append(f"private key header in {rel}")
    print(f"scanned {scanned} files under {root} for {len(needles)} secret values from "
          f"{[p.name for p in env_files]}")
    if findings:
        print("FINDINGS:\n  " + "\n  ".join(findings))
        return 1
    print("OK: no secret values found" + (" and no secret-bearing files" if a.strict else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
