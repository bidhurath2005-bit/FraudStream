"""Create .env from .env.example with freshly generated random secrets.

  python scripts/new_env.py               create .env (refuses to overwrite an existing one)
  python scripts/new_env.py --add-missing append keys that exist in .env.example but not in .env;
                                          secrets among them get fresh random values. Existing
                                          values are NEVER changed.
  python scripts/new_env.py --force       regenerate .env from scratch (replaces ALL secrets)
"""
from __future__ import annotations

import secrets
import string
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECRET_KEYS = {"JWT_SECRET", "POSTGRES_PASSWORD", "REDIS_PASSWORD",
               "FRAUDSTREAM_ADMIN_PASSWORD", "FRAUDSTREAM_ANALYST_PASSWORD"}


def login_password() -> str:
    """Satisfies the API password policy (>= 12 chars, >= 3 character classes)."""
    alphabet = string.ascii_letters + string.digits + "-_.!@%"
    while True:
        pw = "".join(secrets.choice(alphabet) for _ in range(22))
        if (any(c.islower() for c in pw) and any(c.isupper() for c in pw) and any(c.isdigit() for c in pw)
                and any(not c.isalnum() for c in pw)):
            return pw


def secret_for(key: str) -> str:
    if key == "JWT_SECRET":
        return secrets.token_urlsafe(48)
    if key in ("POSTGRES_PASSWORD", "REDIS_PASSWORD"):
        return secrets.token_urlsafe(24)       # URL-safe: embedded in DATABASE_URL / REDIS_URL
    return login_password()


def _key(line: str) -> str | None:
    s = line.strip()
    if not s or s.startswith("#") or "=" not in s:
        return None
    return s.split("=", 1)[0].strip()


def add_missing(target: Path) -> int:
    existing = {k for k in map(_key, target.read_text(encoding="utf-8").splitlines()) if k}
    added = []
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        k = _key(line)
        if k and k not in existing:
            added.append(f"{k}={secret_for(k)}" if k in SECRET_KEYS else line)
    if not added:
        print(".env already has every key from .env.example - not modified")
        return 0
    with target.open("a", encoding="utf-8") as fh:
        fh.write("\n# --- added by scripts/new_env.py --add-missing ---\n" + "\n".join(added) + "\n")
    print(f"appended {len(added)} missing key(s) to {target}: {', '.join(a.split('=', 1)[0] for a in added)}")
    return 0


def main() -> int:
    target = ROOT / ".env"
    if target.exists() and "--add-missing" in sys.argv:
        return add_missing(target)
    if target.exists() and "--force" not in sys.argv:
        print(".env already exists - not modified (use --add-missing to append new keys, --force to regenerate)")
        return 0
    values = {k: secret_for(k) for k in SECRET_KEYS}
    out = []
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        key = _key(line)
        if key in SECRET_KEYS:
            line = f"{key}={values[key]}"
        elif line.startswith("# DATABASE_URL=postgresql"):
            line = f"# DATABASE_URL=postgresql+psycopg://fraudstream:{values['POSTGRES_PASSWORD']}@127.0.0.1:5433/fraudstream"
        elif line.startswith("# REDIS_URL="):
            line = f"# REDIS_URL=redis://:{values['REDIS_PASSWORD']}@127.0.0.1:6380/0"
        out.append(line)
    target.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"wrote {target} with random secrets")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
