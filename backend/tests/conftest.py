"""Backend test fixtures.

* A fresh SQLite database per test session in pytest's tmp dir (migrated with Alembic).
* A COPY of ml/artifacts in tmp (activation/rollback write registry.json; the project's
  real artifacts are never modified).
* Real models, real feature engineering, real risk engine — nothing is mocked.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
REAL_ARTIFACTS = ROOT / "ml" / "artifacts"
_TMP = Path(tempfile.mkdtemp(prefix="fraudstream-test-"))
os.environ.update({
    "FRAUDSTREAM_ENV": "test", "FRAUDSTREAM_PROFILE": "local",
    "DATABASE_URL": f"sqlite:///{(_TMP / 'test.db').as_posix()}",
    "JWT_SECRET": "test-secret-" + "x" * 40,
    "FRAUDSTREAM_ARTIFACTS_DIR": str(_TMP / "artifacts"), "FRAUDSTREAM_DATA_DIR": str(_TMP / "data"),
    "LOGIN_RATE_LIMIT_PER_MINUTE": "1000", "API_RATE_LIMIT_PER_MINUTE": "100000",
})
for k in ("REDIS_URL", "KAFKA_BOOTSTRAP_SERVERS"):
    os.environ.pop(k, None)

if not (REAL_ARTIFACTS / "registry.json").exists():
    raise RuntimeError("backend tests need trained models: run `python -m ml.training.train` first")
shutil.copytree(REAL_ARTIFACTS, _TMP / "artifacts")

from fastapi.testclient import TestClient  # noqa: E402

from app.core.config import get_settings  # noqa: E402

get_settings.cache_clear()
from app.cli import _create  # noqa: E402
from app.main import create_app  # noqa: E402

PASSWORDS = {"admin": "Admin-Pass-12345", "analyst": "Analyst-Pass-12345", "analyst2": "Analyst2-Pass-12345"}


@pytest.fixture(scope="session")
def client() -> Iterator[TestClient]:
    app = create_app(get_settings())
    with TestClient(app) as c:
        _create("admin", "ADMIN", PASSWORDS["admin"])
        _create("analyst", "ANALYST", PASSWORDS["analyst"])
        _create("analyst2", "ANALYST", PASSWORDS["analyst2"])
        yield c
    shutil.rmtree(_TMP, ignore_errors=True)  # only the per-session temp dir created above


def _login(client: TestClient, user: str) -> dict[str, str]:
    r = client.post("/api/v1/auth/login", json={"username": user, "password": PASSWORDS[user]})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


@pytest.fixture(scope="session")
def admin_h(client) -> dict[str, str]:
    return _login(client, "admin")


@pytest.fixture(scope="session")
def analyst_h(client) -> dict[str, str]:
    return _login(client, "analyst")


@pytest.fixture(scope="session")
def analyst2_h(client) -> dict[str, str]:
    return _login(client, "analyst2")


@pytest.fixture
def make_events():
    """Generate synthetic events for a fresh customer population (unique per call)."""
    from simulator.generator import SimulationConfig, TransactionGenerator

    counter = {"n": 0}

    def _make(scenario: str | None = None, n_normal: int = 0, seed: int = 0):
        counter["n"] += 1
        cfg = SimulationConfig(mode="normal", tps=1.0, duration_seconds=None, max_events=n_normal or None,
                               customer_count=30, seed=1000 + seed * 10 + counter["n"])
        # history ends roughly "now" so injected scenario events (stamped now) are in order
        gen = TransactionGenerator(cfg, datetime.now(timezone.utc) - timedelta(seconds=n_normal * 1.2 + 5))
        evs = list(gen.events()) if n_normal else []
        if scenario:
            evs += gen.inject(scenario, customer_id=evs[-1].customer_id if evs else None)
        return evs
    return _make
