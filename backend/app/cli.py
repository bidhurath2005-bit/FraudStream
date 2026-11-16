"""Admin CLI.

  python -m app.cli migrate
  python -m app.cli create-user --username alice --role ANALYST   (prompts for password)
  python -m app.cli create-user --username ops --role ADMIN --password-env OPS_PASSWORD
  python -m app.cli dev-bootstrap        (development only: creates 'admin' and 'analyst'
                                          with RANDOM passwords written to .dev-credentials.txt)
  python -m app.cli prune-reports --older-than-days 90 [--keep-latest 20] [--type T] [--apply]
                                         (report retention; DRY RUN unless --apply; audited)
"""
from __future__ import annotations

import argparse
import getpass
import os
import secrets
import string
import sys

from sqlalchemy import select

from app.core.config import ROOT_DIR, get_settings
from app.database.migrate import upgrade
from app.database.session import SessionLocal, init_engine
from app.models.entities import Role, User
from app.security.auth import hash_password


def _ensure_roles(db) -> None:  # type: ignore[no-untyped-def]
    for name in ("ADMIN", "ANALYST"):
        if db.scalar(select(Role).where(Role.name == name)) is None:
            db.add(Role(name=name))
    db.commit()


def _create(username: str, role: str, password: str, full_name: str = "") -> bool:
    db = SessionLocal()
    try:
        _ensure_roles(db)
        if db.scalar(select(User).where(User.username == username)):
            print(f"user '{username}' already exists - unchanged")
            return False
        role_id = db.scalar(select(Role.id).where(Role.name == role))
        db.add(User(username=username, full_name=full_name, password_hash=hash_password(password), role_id=role_id))
        db.commit()
        return True
    finally:
        db.close()


def _random_password() -> str:
    alphabet = string.ascii_letters + string.digits + "-_!@#%"
    while True:
        pw = "".join(secrets.choice(alphabet) for _ in range(20))
        if any(c.islower() for c in pw) and any(c.isupper() for c in pw) and any(c.isdigit() for c in pw):
            return pw


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.cli")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate")
    cu = sub.add_parser("create-user")
    cu.add_argument("--username", required=True)
    cu.add_argument("--role", choices=["ADMIN", "ANALYST"], required=True)
    cu.add_argument("--full-name", default="")
    cu.add_argument("--password-env", help="read the password from this environment variable")
    sub.add_parser("dev-bootstrap")
    pr = sub.add_parser("prune-reports", help="report retention (dry run unless --apply)")
    pr.add_argument("--older-than-days", type=int, required=True)
    pr.add_argument("--keep-latest", type=int, default=20, help="newest reports kept per type regardless of age")
    pr.add_argument("--type", default=None, help="only this report type")
    pr.add_argument("--apply", action="store_true", help="actually delete (default: report what would be deleted)")
    a = ap.parse_args(argv)
    s = get_settings()
    init_engine(s.database_url)
    upgrade(s.database_url)
    if a.cmd == "migrate":
        print("migrations applied")
        return 0
    if a.cmd == "create-user":
        if a.password_env:
            pw = os.environ.get(a.password_env)
            if not pw:
                print(f"environment variable {a.password_env} is empty", file=sys.stderr)
                return 2
        else:
            pw = getpass.getpass("Password: ")
            if pw != getpass.getpass("Repeat: "):
                print("passwords do not match", file=sys.stderr)
                return 2
        try:
            created = _create(a.username, a.role, pw, a.full_name)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        print("created" if created else "unchanged")
        return 0
    if a.cmd == "prune-reports":
        import json

        from app.services.report_retention import RetentionRule, prune
        from app.services.reports import REPORT_TYPES

        if a.type and a.type not in REPORT_TYPES:
            print(f"unknown report type; choose from {sorted(REPORT_TYPES)}", file=sys.stderr)
            return 2
        rule = RetentionRule(a.older_than_days, a.keep_latest, a.type)
        try:
            rule.validate()
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        db = SessionLocal()
        try:
            out = prune(db, rule, apply=a.apply, actor="cli:prune-reports")
        finally:
            db.close()
        print(json.dumps(out, indent=2))
        print(("DELETED " if a.apply else "DRY RUN - would delete ") + f"{out['eligible']} of {out['stored_reports']} "
              "stored report(s)" + ("" if a.apply else "; re-run with --apply to delete"))
        return 0
    if a.cmd == "dev-bootstrap":
        if s.env != "development":
            print("dev-bootstrap is only allowed with FRAUDSTREAM_ENV=development", file=sys.stderr)
            return 2
        lines = []
        for username, role in (("admin", "ADMIN"), ("analyst", "ANALYST")):
            pw = _random_password()
            if _create(username, role, pw, f"Demo {role.title()}"):
                lines.append(f"{username} / {pw}  ({role})")
        if lines:
            path = ROOT_DIR / ".dev-credentials.txt"
            with path.open("a", encoding="utf-8") as fh:
                fh.write("# DEVELOPMENT-ONLY demo accounts (random passwords). Never use in production.\n")
                fh.write("\n".join(lines) + "\n")
            print("Development accounts created; credentials written to .dev-credentials.txt:")
            print("\n".join("  " + ln for ln in lines))
        else:
            print("demo accounts already exist; see .dev-credentials.txt")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
