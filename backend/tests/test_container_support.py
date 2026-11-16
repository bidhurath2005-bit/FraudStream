"""Phase 10 container support: read-only artifacts guard, runtime-config location,
.env secret tooling and the container health-check script (no Docker needed)."""
from __future__ import annotations

import importlib.util
import json
import os
import time
from pathlib import Path

from app.core.config import Settings
from app.core.runtime import get_runtime
from app.models.entities import ModelVersion

API = "/api/v1"
ROOT = Path(__file__).resolve().parents[2]


def _load(rel: str):  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location(Path(rel).stem, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_model_changes_refused_explicitly_when_artifacts_read_only(client, admin_h, monkeypatch):
    from app.database.session import SessionLocal

    models = client.get(f"{API}/models?kind=supervised", headers=admin_h).json()
    other = next(m for m in models if m["status"] != "ACTIVE")
    real_access = os.access
    art = str(get_runtime().settings.artifacts_dir)
    monkeypatch.setattr(os, "access", lambda p, mode: False if str(p) == art and mode == os.W_OK else real_access(p, mode))
    for method, path, body in (("post", f"/models/{other['id']}/activate", {"reason": "ro mount check"}),
                               ("post", "/models/rollback", {"kind": "supervised", "reason": "ro mount check"}),
                               ("post", "/models/train", {"quick": True})):
        r = getattr(client, method)(f"{API}{path}", headers=admin_h, json=body)
        assert r.status_code == 409, (path, r.status_code, r.text)
        if path == "/models/rollback" and "no previously active" in r.json()["detail"]:
            continue                                    # refused earlier for a legitimate reason (fresh DB)
        assert "read-only" in r.json()["detail"], (path, r.text)
    db = SessionLocal()
    try:      # nothing was changed half-way: the candidate is still not active
        assert db.get(ModelVersion, other["id"]).status != "ACTIVE"
    finally:
        db.close()


def test_runtime_config_dir_defaults_to_artifacts_and_can_be_redirected(tmp_path, monkeypatch):
    monkeypatch.delenv("RUNTIME_CONFIG_DIR", raising=False)
    s = Settings(FRAUDSTREAM_ARTIFACTS_DIR=str(tmp_path / "a"), _env_file=None)
    assert s.runtime_config_path_dir == tmp_path / "a"
    s = Settings(FRAUDSTREAM_ARTIFACTS_DIR=str(tmp_path / "a"), RUNTIME_CONFIG_DIR=str(tmp_path / "state"), _env_file=None)
    assert s.runtime_config_path_dir == tmp_path / "state"


def test_new_env_add_missing_never_changes_existing_values(tmp_path, monkeypatch):
    mod = _load("scripts/new_env.py")
    (tmp_path / ".env.example").write_text("JWT_SECRET=x\nPOSTGRES_PASSWORD=x\nFOO=bar   # c\n"
                                           "FRAUDSTREAM_ADMIN_PASSWORD=change-me\n", encoding="utf-8")
    (tmp_path / ".env").write_text("JWT_SECRET=keep-me\nPOSTGRES_PASSWORD=keep-pg\n", encoding="utf-8")
    monkeypatch.setattr(mod, "ROOT", tmp_path)
    monkeypatch.setattr("sys.argv", ["new_env.py", "--add-missing"])
    assert mod.main() == 0
    env = dict(line.split("=", 1) for line in (tmp_path / ".env").read_text().splitlines() if line and not line.startswith("#"))
    assert env["JWT_SECRET"] == "keep-me" and env["POSTGRES_PASSWORD"] == "keep-pg"
    assert env["FOO"].startswith("bar")
    pw = env["FRAUDSTREAM_ADMIN_PASSWORD"]
    assert pw != "change-me" and len(pw) >= 12
    from app.security.auth import validate_password_strength

    validate_password_strength(pw)                       # accepted by the API's password policy
    assert mod.main() == 0                               # second run: nothing appended
    assert (tmp_path / ".env").read_text().count("FRAUDSTREAM_ADMIN_PASSWORD") == 1


def test_container_healthcheck_script(tmp_path, monkeypatch):
    hc = _load("infrastructure/docker/healthcheck.py")
    hb = tmp_path / "hb.json"
    assert hc.main(["hc", "file", str(hb), "30"]) == 1                 # no heartbeat yet
    hb.write_text("{}")
    assert hc.main(["hc", "file", str(hb), "30"]) == 0
    old = time.time() - 120
    os.utime(hb, (old, old))
    assert hc.main(["hc", "file", str(hb), "30"]) == 1                 # stale
    sp = tmp_path / "spark.json"
    sp.write_text(json.dumps({"queries": [{"name": "score", "active": True, "exception": None}]}))
    assert hc.main(["hc", "spark", str(sp), "45"]) == 0
    sp.write_text(json.dumps({"queries": [{"name": "score", "active": False, "exception": "boom"}]}))
    assert hc.main(["hc", "spark", str(sp), "45"]) == 1                # failed query -> unhealthy
    sp.write_text(json.dumps({"queries": []}))
    assert hc.main(["hc", "spark", str(sp), "45"]) == 1
    assert hc.main(["hc", "http", "http://127.0.0.1:9/nothing"]) == 1  # unreachable
