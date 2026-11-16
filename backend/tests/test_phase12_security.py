"""Phase 12 security hardening: mandatory configuration versions, bounded numeric parameters (no 5xx for
hostile values on any GET route), SQL-injection strings handled as data, path traversal, unexpected JSON
fields, CSV formula injection in the legacy alerts export, and the API rate-limit key for invalid tokens."""

from __future__ import annotations

import csv
import io
import itertools
import json

from fastapi.testclient import TestClient
from sqlalchemy import func, select
from starlette.requests import Request

from app.database.session import SessionLocal
from app.models.entities import AuditLog, Transaction

API = "/api/v1"
HOSTILE = ["-1", "0", "-999999999", "99999999999999999999", "abc", "1e309", "' OR 1=1--", "../../etc/passwd", "",
           "2026-13-45T99:99", "%00", "<script>"]


def _max_audit() -> int:
    with SessionLocal() as db:
        return db.scalar(select(func.max(AuditLog.id))) or 0


def _audits_since(mark: int) -> list[AuditLog]:
    with SessionLocal() as db:
        return list(db.scalars(select(AuditLog).where(AuditLog.id > mark).order_by(AuditLog.id)))


def _rule(client, h, code="LARGE_AMOUNT"):
    return next(r for r in client.get(f"{API}/risk/rules", headers=h).json() if r["code"] == code)


# ------------------------------------------------------------------ optimistic concurrency is mandatory
def test_configuration_updates_without_a_version_are_rejected_428_and_audited(client, admin_h):
    rule = _rule(client, admin_h)
    risk = client.get(f"{API}/risk/settings", headers=admin_h).json()
    drift = client.get(f"{API}/settings/drift", headers=admin_h).json()
    cases = [
        ("patch", f"/risk/rules/{rule['code']}", {"enabled": not rule["enabled"]}, "RULE_UPDATE_REJECTED"),
        ("put", "/risk/settings", {**risk["settings"], "high_threshold": risk["settings"]["high_threshold"] - 1},
         "RISK_SETTINGS_REJECTED"),
        ("put", "/settings/drift", {"value": {"min_rows": drift["value"]["min_rows"] + 1}}, "SETTING_UPDATE_REJECTED"),
    ]
    for method, path, body, action in cases:
        mark = _max_audit()
        r = getattr(client, method)(f"{API}{path}", headers=admin_h, json=body)
        assert r.status_code == 428, (path, r.text)
        assert "expected_updated_at is required" in r.json()["detail"]
        rej = _audits_since(mark)
        assert [a.action for a in rej] == [action] and rej[0].details["status_code"] == 428
    # nothing changed
    assert _rule(client, admin_h)["enabled"] == rule["enabled"] and _rule(client, admin_h)["updated_at"] == rule["updated_at"]
    assert client.get(f"{API}/risk/settings", headers=admin_h).json()["settings"] == risk["settings"]
    assert client.get(f"{API}/settings/drift", headers=admin_h).json()["value"] == drift["value"]


def test_invalid_values_are_still_reported_422_before_the_version_check(client, admin_h):
    rule = _rule(client, admin_h)
    assert client.patch(f"{API}/risk/rules/{rule['code']}", headers=admin_h,
                        json={"params": {"ratio": 0.5}}).status_code == 422
    assert client.put(f"{API}/settings/drift", headers=admin_h,
                      json={"value": {"moderate": 0.3, "significant": 0.2}}).status_code == 422


def test_null_version_is_only_accepted_for_a_setting_that_was_never_saved(client, admin_h):
    rule = _rule(client, admin_h)
    # the rule exists (has an updated_at): claiming "never saved" is stale -> 409, nothing changes
    r = client.patch(f"{API}/risk/rules/{rule['code']}", headers=admin_h,
                     json={"enabled": not rule["enabled"], "expected_updated_at": None})
    assert r.status_code == 409 and _rule(client, admin_h)["enabled"] == rule["enabled"]
    ok = client.patch(f"{API}/risk/rules/{rule['code']}", headers=admin_h,
                      json={"weight": rule["weight"], "expected_updated_at": rule["updated_at"]})
    assert ok.status_code == 200 and ok.json()["changed_fields"] == []


# ------------------------------------------------------------------ input validation
def test_no_get_route_answers_5xx_for_hostile_parameter_values(client, admin_h):
    """Every GET route in the OpenAPI schema, every query and path parameter, a set of hostile values
    (negative, zero, huge, non-numeric, SQL, traversal, empty, invalid dates). Validation must answer 4xx."""
    c = TestClient(client.app, raise_server_exceptions=False)
    failures = []
    for path, ops in client.app.openapi()["paths"].items():
        if "get" not in ops or "/stream/" in path:
            continue
        params = ops["get"].get("parameters", [])
        qp = [p["name"] for p in params if p["in"] == "query"]
        pp = [p["name"] for p in params if p["in"] == "path"]
        for pv in (["x"] if not pp else ["-1", "..%2F..%2Fetc%2Fpasswd", "' OR 1=1--", "999999999999999999999"]):
            url = path
            for n in pp:
                url = url.replace("{" + n + "}", pv)
            if not qp:
                r = c.get(url, headers=admin_h)
                if r.status_code >= 500:
                    failures.append((url, r.status_code))
            for q, v in itertools.product(qp, HOSTILE):
                r = c.get(url, params={q: v}, headers=admin_h)
                if r.status_code >= 500:
                    failures.append((url, q, v, r.status_code))
    assert not failures, failures[:20]


def test_negative_and_zero_limits_are_rejected_instead_of_meaning_unlimited(client, admin_h):
    for url in ("/drift/history?limit=-1", "/drift/history?limit=0", "/simulation/runs?limit=-1",
                "/analytics/risk-distribution?minutes=0", "/analytics/risk-distribution?minutes=-5",
                "/transactions?page=100001", "/alerts?size=100000", "/transactions/0", "/models/compare?ids=1,99999999999"):
        assert client.get(f"{API}{url}", headers=admin_h).status_code == 422, url


def test_sql_injection_strings_are_treated_as_data(client, admin_h):
    with SessionLocal() as db:
        n_before = db.scalar(select(func.count(Transaction.id)))
    for url in ("/transactions?customer_id=' OR '1'='1", "/transactions?customer_id=x'; DROP TABLE transactions;--",
                "/alerts?customer_id=' OR 1=1--", "/reports/generated?report_type=x' OR '1'='1"):
        r = client.get(f"{API}{url}", headers=admin_h)
        assert r.status_code in (200, 422), (url, r.status_code)
        if r.status_code == 200:
            assert r.json().get("total", 0) == 0, url
    with SessionLocal() as db:
        assert db.scalar(select(func.count(Transaction.id))) == n_before


def test_path_traversal_attempts_find_nothing(client, admin_h):
    for url in ("/reports/generated/..%2F..%2Fetc%2Fpasswd", "/reports/generated/../../etc/passwd/download",
                "/settings/..%2F..%2Fetc", "/risk/rules/..%2F..%2Fx"):
        r = client.get(f"{API}{url}", headers=admin_h)
        assert r.status_code in (404, 405, 422), (url, r.status_code)
    rep = client.post(f"{API}/reports", headers=admin_h, json={"report_type": "investigation",
                                                               "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"})
    uid = rep.json()["report_uid"]
    r = client.get(f"{API}/reports/generated/{uid}/download", headers=admin_h,
                   params={"format": "csv", "section": "../../../../etc/passwd"})
    assert r.status_code in (404, 422)


def test_unexpected_json_fields_are_rejected(client, admin_h):
    assert client.post(f"{API}/reports", headers=admin_h, json={"report_type": "investigation", "title": "x",
                                                                 "start": "2026-01-01T00:00:00Z",
                                                                 "end": "2026-01-02T00:00:00Z"}).status_code == 422
    rule = _rule(client, admin_h)
    assert client.patch(f"{API}/risk/rules/{rule['code']}", headers=admin_h,
                        json={"weight": rule["weight"], "is_admin": True, "expected_updated_at": rule["updated_at"]}
                        ).status_code == 422


# ------------------------------------------------------------------ CSV formula injection (legacy export)
def test_alerts_csv_neutralises_formulas_from_ingested_text(client, admin_h, make_events):
    evil = '=HYPERLINK("http://example.invalid","x")'
    evs = [e.model_copy(update={"customer_id": evil, "account_id": evil}) for e in make_events("velocity_burst", n_normal=3)]
    resp = [client.post(f"{API}/transactions/ingest", headers=admin_h, json=e.to_wire()).json() for e in evs]
    assert any(r.get("alert_uid") for r in resp), "scenario raised no alert"
    body = client.get(f"{API}/reports/alerts.csv", headers=admin_h).content.decode("utf-8")
    rows = list(csv.reader(io.StringIO(body)))
    hits = [r for r in rows if "HYPERLINK" in "".join(r)]
    assert hits
    for r in rows[1:]:
        for cell in r:
            assert not cell.startswith(("=", "+", "@", "\t", "\r")), cell
    assert all(any(cell == "'" + evil for cell in r) for r in hits)


# ------------------------------------------------------------------ rate-limit key
def _req(auth: str | None, host: str = "203.0.113.7") -> Request:
    headers = [(b"authorization", auth.encode())] if auth else []
    return Request({"type": "http", "method": "GET", "path": "/api/v1/alerts", "headers": headers,
                    "client": (host, 1234), "query_string": b""})


def test_invalid_bearer_tokens_fall_back_to_the_client_address_bucket(client, admin_h):
    from app.main import _rate_limit_key

    valid = admin_h["Authorization"]
    assert _rate_limit_key(_req(valid)).startswith("t:")
    k1 = _rate_limit_key(_req("Bearer " + "a" * 40))
    k2 = _rate_limit_key(_req("Bearer " + "b" * 40))
    k3 = _rate_limit_key(_req("Basic Zm9vOmJhcg=="))
    assert k1 == k2 == k3 == _rate_limit_key(_req(None)) == "ip:203.0.113.7"


def test_no_secret_values_in_system_information_endpoints(client, admin_h):
    from app.core.config import get_settings

    secret = get_settings().jwt_secret
    for url in ("/system/status", "/infrastructure/status", "/health", "/models/active", "/settings"):
        text = client.get(f"{API}{url}", headers=admin_h).text
        assert secret not in text and "password" not in text.lower(), url
    assert "password_hash" not in json.dumps(client.get(f"{API}/users", headers=admin_h).json())


# ------------------------------------------------------------------ PDF Unicode fallback (Phase 12)
def test_font_runs_send_only_glyphs_missing_from_the_base_font_to_the_fallback():
    from app.services.reports import font_runs

    base = frozenset(map(ord, "ABC abc-:"))
    cjk, hangul = ("F0", frozenset(map(ord, "中文테"))), ("F1", frozenset(map(ord, "한국테")))
    # first fallback that has the character wins ("테" is in both -> F0); Hangul missing from F0 -> F1
    assert font_runs("abc 中文-테한국x", base, [cjk, hangul]) == [
        (None, "abc "), ("F0", "中文"), (None, "-"), ("F0", "테"), ("F1", "한국"), (None, "x")]
    assert font_runs("中", base, None) == [(None, "中")]           # no fallback font: unchanged behaviour
    assert font_runs("Ж", base, [cjk]) == [(None, "Ж")]           # in no font: stays in the base font (missing glyph)


def test_pdf_embeds_the_fallback_font_for_cjk_when_one_is_available(client, admin_h):
    import os
    import re

    import pytest

    from app.services import reports as rep

    rep._fonts()
    if not rep._fallback_fonts():
        pytest.skip("no CJK fallback font on this host (the app image installs fonts-droid-fallback + fonts-nanum; set "
                    "FRAUDSTREAM_PDF_FALLBACK_FONT to a TrueType CJK font to run this here)")
    content = {"report_uid": "00000000-0000-0000-0000-0000000000cf", "title": "Investigation report",
               "generated_at": "2026-11-15T12:00:00+00:00", "generated_by": "t", "profile": "local",
               "period": {"start": "2026-11-01T00:00:00+00:00", "end": "2026-11-02T00:00:00+00:00", "basis": "b",
                          "label": "l"}, "filters": {}, "summary": [],
               "sections": [{"id": "s", "title": "S", "columns": [{"key": "t", "label": "T", "format": "longtext"}],
                             "rows": [{"t": "调查 日本語 한국어"}], "total_rows": 1, "truncated": False}],
               "limitations": [], "data_sources": []}
    pdf = rep.render_pdf(content)
    fonts = set(re.findall(rb"/BaseFont /[A-Z]{6}\+([A-Za-z0-9-]+)", pdf))
    assert len(fonts) >= 3, fonts            # base regular + bold + the embedded fallback subset(s)
    if not os.environ.get("FRAUDSTREAM_PDF_FALLBACK_FONT"):   # the app image: Droid for CJK, Nanum for Hangul
        assert any(b"Droid" in f for f in fonts) and any(b"Nanum" in f for f in fonts), fonts
