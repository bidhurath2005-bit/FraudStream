"""Phase 15: access-token validation against hostile tokens (regression guard for the PyJWT 2.10.1 -> 2.15.x upgrade).

Tokens are assembled by hand (base64url + HMAC-SHA256) so the checks do not depend on how the installed PyJWT
*encodes*; only the server-side *decode* path in ``app.security.auth`` is exercised. A correctly assembled token
is accepted (positive control), so every rejection below is caused by the specific defect injected.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

import jwt
import pytest
from app.core.config import get_settings
from app.security.auth import create_access_token, decode_access_token

API = "/api/v1"


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _token(payload: dict, header: dict | None = None, secret: str | None = None, digest=hashlib.sha256) -> str:
    header = header or {"alg": "HS256", "typ": "JWT"}
    signing_input = f"{_b64(json.dumps(header).encode())}.{_b64(json.dumps(payload).encode())}"
    key = (secret if secret is not None else get_settings().jwt_secret).encode()
    sig = hmac.new(key, signing_input.encode(), digest).digest()
    return f"{signing_input}.{_b64(sig)}"


def _claims(uid: int, **over) -> dict:
    now = int(time.time())
    return {"sub": str(uid), "username": "analyst", "role": "ANALYST", "type": "access", "iat": now,
            "exp": now + 300, "jti": "p15", **over}


def _me(client, token: str):
    return client.get(f"{API}/auth/me", headers={"Authorization": f"Bearer {token}"})


@pytest.fixture(scope="module")
def analyst_id(client, analyst_h) -> int:
    return client.get(f"{API}/auth/me", headers=analyst_h).json()["id"]


def test_hand_built_token_is_accepted_positive_control(client, analyst_id):
    r = _me(client, _token(_claims(analyst_id)))
    assert r.status_code == 200 and r.json()["id"] == analyst_id


def test_issued_token_header_and_lifetime(client, analyst_id):
    from app.database.session import SessionLocal
    from app.models.entities import User

    with SessionLocal() as db:
        token, ttl = create_access_token(db.get(User, analyst_id))
    header = jwt.get_unverified_header(token)
    payload = decode_access_token(token)
    assert header["alg"] == "HS256"
    assert ttl == get_settings().access_token_minutes * 60 == payload["exp"] - payload["iat"]
    assert payload["type"] == "access" and payload["sub"] == str(analyst_id)


@pytest.mark.parametrize("case", ["alg_none", "alg_hs512", "alg_hs384", "wrong_secret", "empty_secret"])
def test_algorithm_and_key_confusion_is_rejected(client, analyst_id, case):
    claims = _claims(analyst_id)
    if case == "alg_none":
        tok = f"{_b64(json.dumps({'alg': 'none', 'typ': 'JWT'}).encode())}.{_b64(json.dumps(claims).encode())}."
    elif case == "alg_hs512":   # right secret, algorithm not in the allow-list
        tok = _token(claims, {"alg": "HS512", "typ": "JWT"}, digest=hashlib.sha512)
    elif case == "alg_hs384":
        tok = _token(claims, {"alg": "HS384", "typ": "JWT"}, digest=hashlib.sha384)
    elif case == "wrong_secret":
        tok = _token(claims, secret="attacker-chosen-secret-" + "y" * 30)
    else:
        tok = _token(claims, secret="")
    r = _me(client, tok)
    assert r.status_code == 401 and r.json()["detail"] == "invalid token", (case, r.text)


def test_tampered_payload_with_original_signature_is_rejected(client, analyst_id):
    good = _token(_claims(analyst_id))
    h, _, s = good.split(".")
    forged_payload = _b64(json.dumps(_claims(analyst_id, role="ADMIN", sub="1")).encode())
    assert _me(client, f"{h}.{forged_payload}.{s}").status_code == 401


@pytest.mark.parametrize("drop", ["exp", "sub"])
def test_required_claims_are_enforced(client, analyst_id, drop):
    claims = _claims(analyst_id)
    del claims[drop]
    assert _me(client, _token(claims)).status_code == 401


def test_expired_token_reports_expiry(client, analyst_id):
    r = _me(client, _token(_claims(analyst_id, exp=int(time.time()) - 5)))
    assert r.status_code == 401 and r.json()["detail"] == "token expired"


def test_non_access_token_type_is_rejected(client, analyst_id):
    r = _me(client, _token(_claims(analyst_id, type="refresh")))
    assert r.status_code == 401 and r.json()["detail"] == "invalid token type"


def test_unknown_critical_header_is_rejected(client, analyst_id):
    """RFC 7515 4.1.11 (GHSA-752w-5fwx-jx9f, fixed in PyJWT 2.12.0): a correctly signed token whose `crit` header
    lists an extension the verifier does not understand MUST be rejected."""
    tok = _token(_claims(analyst_id), {"alg": "HS256", "typ": "JWT", "crit": ["x-fs-ext"], "x-fs-ext": True})
    assert _me(client, tok).status_code == 401


def test_signature_outside_the_base64url_alphabet_is_rejected(client, analyst_id):
    """GHSA-hxm8-2xgr-2p9m (fixed in PyJWT 2.14.0): appended non-alphabet characters must not yield a second,
    differently serialised but still valid token."""
    good = _token(_claims(analyst_id))
    assert _me(client, good).status_code == 200
    assert _me(client, good + "!!!!").status_code == 401


def test_token_for_a_missing_user_is_rejected(client):
    assert _me(client, _token(_claims(999_999))).status_code == 401


def test_role_claim_is_not_trusted_for_authorisation(client, analyst_id):
    """A validly signed token claiming role=ADMIN for an analyst account still gets 403: the role comes from the DB."""
    tok = _token(_claims(analyst_id, role="ADMIN"))
    assert _me(client, tok).json()["role"] == "ANALYST"
    assert client.get(f"{API}/audit", headers={"Authorization": f"Bearer {tok}"}).status_code == 403


@pytest.mark.parametrize("value", ["Bearer", "Basic abc", "Bearer a.b", "Bearer " + "a" * 5000, "Bearer ..."])
def test_malformed_authorization_headers_are_401_not_500(client, value):
    r = client.get(f"{API}/auth/me", headers={"Authorization": value})
    assert r.status_code == 401 and "Traceback" not in r.text
