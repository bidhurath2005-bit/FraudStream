"""Programmatic Alembic upgrade (used by scripts and optional auto-migration on startup)."""
from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

BACKEND_DIR = Path(__file__).resolve().parents[2]


def alembic_config(database_url: str) -> Config:
    cfg = Config(str(BACKEND_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(BACKEND_DIR / "migrations"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


def upgrade(database_url: str, revision: str = "head") -> None:
    command.upgrade(alembic_config(database_url), revision)


def main() -> None:
    from app.core.config import get_settings

    url = get_settings().database_url
    upgrade(url)
    print(f"migrations applied: {url.split('@')[-1]}")


if __name__ == "__main__":
    main()
