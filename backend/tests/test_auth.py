from __future__ import annotations

import time

import jwt

from app.core.config import get_settings
from app.core.runtime import get_runtime
from app.security.auth import hash_password, verify_password
from app.security.rate_limit import RateLimiter

API = "/api/v1"


def test_health_is_public_and_minimal(client):
    r = client.get(f"{API}/health")
    assert r.status_code == 200
    body = r.json()
    assert body["database"] == "up" and body["model_loaded"] is True
    assert "jwt" not in r.text.lower() and "database_url" not in r.text.lower()


def test_unauthenticated_requests_are_rejected(client):
    for path in ("/transactions", "/alerts", "/models", "/analytics/overview", "/system/status"):
        assert client.get(API + path).status_code == 401, path


def test_login_success_and_me(client, analyst_h):
    r = client.get(f"{API}/auth/me", headers=analyst_h)
    assert r.status_code == 200 and r.json()["role"] == "ANALYST"


def test_login_wrong_password_generic_error(client):
    r1 = client.post(f"{API}/auth/login", json={"username": "analyst", "password": "nope-nope-nope"})
    r2 = client.post(f"{API}/auth/login", json={"username": "ghost", "password": "nope-nope-nope"})
    assert r1.status_code == r2.status_code == 401
    assert r1.json() == r2.json()


def test_invalid_and_expired_tokens(client):
    assert client.get(f"{API}/auth/me", headers={"Authorization": "Bearer garbage"}).status_code == 401
    s = get_settings()
    expired = jwt.encode({"sub": "1", "type": "access", "exp": int(time.time()) - 10}, s.jwt_secret, algorithm="HS256")
    r = client.get(f"{API}/auth/me", headers={"Authorization": f"Bearer {expired}"})
    assert r.status_code == 401 and r.json()["detail"] == "token expired"
    forged = jwt.encode({"sub": "1", "type": "access", "exp": int(time.time()) + 100}, "wrong-secret" * 4, algorithm="HS256")
    assert client.get(f"{API}/auth/me", headers={"Authorization": f"Bearer {forged}"}).status_code == 401


def test_analyst_cannot_use_admin_endpoints(client, analyst_h):
    checks = [
        ("post", "/users", {"username": "x1", "password": "Abcdefgh1234!", "role": "ADMIN"}),
        ("patch", "/risk/rules/LARGE_AMOUNT", {"enabled": False}),
        ("put", "/risk/settings", {"medium_threshold": 40, "high_threshold": 70, "model_weight": 0.9,
                                   "anomaly_weight": 0.3, "anomaly_min_percentile": 0.97, "block_simulation_enabled": True}),
        ("post", "/models/train", {"quick": True}),
        ("post", "/models/1/activate", {"reason": "because"}),
        ("post", "/models/rollback", {"kind": "supervised", "reason": "because"}),
        ("get", "/audit", None),
    ]
    for method, path, body in checks:
        r = getattr(client, method)(API + path, headers=analyst_h, **({"json": body} if body else {}))
        assert r.status_code == 403, (path, r.status_code)


def test_admin_user_management_and_password_policy(client, admin_h):
    weak = client.post(f"{API}/users", headers=admin_h, json={"username": "weak1", "password": "aaaaaaaaaaaa", "role": "ANALYST"})
    assert weak.status_code == 422
    r = client.post(f"{API}/users", headers=admin_h, json={"username": "newanalyst", "password": "Str0ng-Passw0rd!", "role": "ANALYST"})
    assert r.status_code == 201
    uid = r.json()["id"]
    assert client.post(f"{API}/users", headers=admin_h, json={"username": "newanalyst", "password": "Str0ng-Passw0rd!",
                                                              "role": "ANALYST"}).status_code == 409
    assert client.patch(f"{API}/users/{uid}", headers=admin_h, json={"is_active": False}).json()["is_active"] is False
    login = client.post(f"{API}/auth/login", json={"username": "newanalyst", "password": "Str0ng-Passw0rd!"})
    assert login.status_code == 401  # deactivated
    me = client.get(f"{API}/auth/me", headers=admin_h).json()
    assert client.patch(f"{API}/users/{me['id']}", headers=admin_h, json={"role": "ANALYST"}).status_code == 409


def test_refresh_rotation_and_reuse_detection(client):
    from tests.conftest import PASSWORDS

    r = client.post(f"{API}/auth/login", json={"username": "analyst2", "password": PASSWORDS["analyst2"]})
    first = r.cookies.get("fs_refresh") or client.cookies.get("fs_refresh")
    assert first
    r2 = client.post(f"{API}/auth/refresh")
    assert r2.status_code == 200 and r2.json()["access_token"]
    second = client.cookies.get("fs_refresh")
    assert second and second != first
    client.cookies.set("fs_refresh", first, path="/api/v1/auth")
    assert client.post(f"{API}/auth/refresh").status_code == 401        # old token replayed
    client.cookies.set("fs_refresh", second, path="/api/v1/auth")
    assert client.post(f"{API}/auth/refresh").status_code == 401        # whole family revoked
    client.cookies.clear()


def test_login_rate_limit(client):
    rt = get_runtime()
    old, old_limiter = rt.settings.login_rate_limit_per_minute, rt.limiter
    rt.settings.login_rate_limit_per_minute = 3
    rt.limiter = RateLimiter()          # isolated counter for this test
    remaining = 60 - time.time() % 60   # fixed 1-minute windows: don't straddle a window boundary
    if remaining < 10:
        time.sleep(remaining + 0.2)
    try:
        codes = [client.post(f"{API}/auth/login", json={"username": "nobody", "password": "x"}).status_code for _ in range(5)]
    finally:
        rt.settings.login_rate_limit_per_minute, rt.limiter = old, old_limiter
    assert codes[:3] == [401, 401, 401] and codes[3] == 429


def test_password_hashing_and_limiter_unit():
    h = hash_password("Correct-Horse-9")
    assert h.startswith("$argon2id$") and verify_password("Correct-Horse-9", h) and not verify_password("x", h)
    rl = RateLimiter()
    assert [rl.hit("k", 2)[0] for _ in range(3)] == [True, True, False]


def test_validation_errors_are_safe(client, analyst_h):
    r = client.get(f"{API}/transactions?page=0", headers=analyst_h)
    assert r.status_code == 422 and "Traceback" not in r.text
    r = client.get(f"{API}/transactions?q=' OR 1=1 --", headers=analyst_h)   # parameterised: no injection
    assert r.status_code == 200
