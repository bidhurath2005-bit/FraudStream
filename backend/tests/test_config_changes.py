"""Phase 11C configuration changes: server-side validation with per-parameter bounds, optimistic
concurrency, only real changes audited, rejected and denied attempts audited, previous values kept on
failure, persistence (a freshly built engine from the database sees the change), drift-setting bounds."""

from __future__ import annotations

from app.core.runtime import get_runtime
from app.database.session import SessionLocal
from app.models.entities import AuditLog
from app.services.runtime_config import build_engine
from sqlalchemy import select

API = "/api/v1"
RULE = "IMPOSSIBLE_TRAVEL"


def _audit(entity_type: str, entity_id: str, since_id: int = 0) -> list[AuditLog]:
    with SessionLocal() as db:
        return list(
            db.scalars(
                select(AuditLog)
                .where(AuditLog.entity_type == entity_type, AuditLog.entity_id == entity_id, AuditLog.id > since_id)
                .order_by(AuditLog.id)
            )
        )


def _max_audit_id() -> int:
    with SessionLocal() as db:
        return db.scalar(select(AuditLog.id).order_by(AuditLog.id.desc()).limit(1)) or 0


def _rule(client, h, code=RULE):
    return next(r for r in client.get(f"{API}/risk/rules", headers=h).json() if r["code"] == code)


def test_rule_listing_exposes_bounds_last_change_and_editability(client, analyst_h):
    rules = client.get(f"{API}/risk/rules", headers=analyst_h).json()
    assert len(rules) == 10 and all(r["editable"] for r in rules)
    vel = next(r for r in rules if r["code"] == "HIGH_VELOCITY_1M")
    assert vel["param_bounds"]["count"] == {"integer": True, "min": 1, "min_inclusive": True, "max": 1000}
    assert next(r for r in rules if r["code"] == "NEW_LOCATION")["param_bounds"] == {}


def test_invalid_rule_values_are_rejected_audited_and_change_nothing(client, admin_h):
    before = _rule(client, admin_h)
    mark = _max_audit_id()
    bad_bodies = [
        '{"params": {"kmh": 0}}',
        '{"params": {"kmh": -5}}',
        '{"params": {"kmh": 1e999}}',
        '{"params": {"kmh": 30000}}',
        '{"params": {"bogus": 1}}',
        '{"weight": 1.5}',
        '{"weight": "heavy"}',
        '{"enabled": "maybe"}',
        '{"unknown": true}',
    ]
    for raw in bad_bodies:
        r = client.patch(f"{API}/risk/rules/{RULE}", headers={**admin_h, "Content-Type": "application/json"}, content=raw)
        assert r.status_code == 422, (raw, r.text)
    for raw in ('{"params": {"count": 0}}', '{"params": {"count": 2.5}}'):
        assert (
            client.patch(
                f"{API}/risk/rules/HIGH_VELOCITY_1M", headers={**admin_h, "Content-Type": "application/json"}, content=raw
            ).status_code
            == 422
        )
    assert (
        client.patch(f"{API}/risk/rules/LARGE_AMOUNT", headers=admin_h, json={"params": {"ratio": 0.5}}).status_code == 422
    )
    after = _rule(client, admin_h)
    assert {k: after[k] for k in ("params", "weight", "enabled", "updated_at")} == {
        k: before[k] for k in ("params", "weight", "enabled", "updated_at")
    }
    rejected = [a for a in _audit("risk_rule", RULE, mark)]
    assert len(rejected) == len(bad_bodies) and {a.action for a in rejected} == {"RULE_UPDATE_REJECTED"}
    assert all(a.actor_name == "admin" and a.details["reason"] and a.details["status_code"] == 422 for a in rejected)
    assert "must be > 0" in rejected[0].details["reason"] and "finite" in rejected[2].details["reason"]


def test_valid_rule_change_audits_only_changed_fields_and_reaches_the_engine(client, admin_h):
    before = _rule(client, admin_h)
    mark = _max_audit_id()
    r = client.patch(
        f"{API}/risk/rules/{RULE}",
        headers=admin_h,
        json={"params": {"kmh": 950}, "weight": before["weight"], "expected_updated_at": before["updated_at"]},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["params"]["kmh"] == 950.0 and body["changed_fields"] == ["params"]
    assert body["last_change"]["by"] == "admin" and body["propagation"]["file"] in ("ok", "error")
    rows = _audit("risk_rule", RULE, mark)
    assert [(a.action, a.field) for a in rows] == [("RULE_UPDATED", "params")]  # weight unchanged -> not audited
    assert '"kmh": 900.0' in rows[0].old_value and '"kmh": 950.0' in rows[0].new_value
    assert next(x for x in get_runtime().engine.rules if x.code == RULE).params["kmh"] == 950.0
    with SessionLocal() as db:  # what a restart would load
        assert next(x for x in build_engine(db).rules if x.code == RULE).params["kmh"] == 950.0
    # a no-op save is not an audited change
    mark2 = _max_audit_id()
    same = client.patch(f"{API}/risk/rules/{RULE}", headers=admin_h,
                        json={"params": {"kmh": 950}, "expected_updated_at": _rule(client, admin_h)["updated_at"]})
    assert same.status_code == 200 and same.json()["changed_fields"] == [] and _audit("risk_rule", RULE, mark2) == []
    # restore the default
    assert (
        client.patch(f"{API}/risk/rules/{RULE}", headers=admin_h,
                     json={"params": {"kmh": 900}, "expected_updated_at": _rule(client, admin_h)["updated_at"]}).json()["params"]["kmh"]
        == 900.0
    )


def test_stale_rule_edit_is_rejected_with_409(client, admin_h):
    loaded = _rule(client, admin_h)
    assert (
        client.patch(
            f"{API}/risk/rules/{RULE}", headers=admin_h, json={"enabled": False, "expected_updated_at": loaded["updated_at"]}
        ).status_code
        == 200
    )
    stale = client.patch(
        f"{API}/risk/rules/{RULE}", headers=admin_h, json={"weight": 0.2, "expected_updated_at": loaded["updated_at"]}
    )  # second editor, old copy
    assert stale.status_code == 409 and "changed by someone else" in stale.json()["detail"]
    now = _rule(client, admin_h)
    assert now["weight"] == loaded["weight"] and now["enabled"] is False
    assert _audit("risk_rule", RULE)[-1].action == "RULE_UPDATE_REJECTED"
    assert (
        client.patch(
            f"{API}/risk/rules/{RULE}", headers=admin_h, json={"enabled": True, "expected_updated_at": now["updated_at"]}
        ).status_code
        == 200
    )


def test_risk_settings_validation_concurrency_audit_and_persistence(client, admin_h, analyst_h):
    st = client.get(f"{API}/risk/settings", headers=analyst_h).json()
    assert st["editable_by"] == "ADMIN" and st["updated_at"] and "risk engine" in st["used_by"]
    s, engine_before = st["settings"], get_runtime().engine.settings
    mark = _max_audit_id()
    for bad in (
        {**s, "medium_threshold": 80, "high_threshold": 70},
        {**s, "model_weight": 1.5},
        {**s, "extra": 1},
        {k: v for k, v in s.items() if k != "model_weight"},
        {**s, "block_simulation_enabled": "yes"},
    ):
        r = client.put(f"{API}/risk/settings", headers=admin_h, json=bad)
        assert r.status_code == 422, (bad, r.text)
    assert client.get(f"{API}/risk/settings", headers=admin_h).json()["settings"] == s  # previous config kept
    assert get_runtime().engine.settings == engine_before
    rej = _audit("system_setting", "risk", mark)
    assert len(rej) == 5 and {a.action for a in rej} == {"RISK_SETTINGS_REJECTED"}
    assert "medium_threshold < high_threshold" in rej[0].details["reason"]
    # identical save: not a change
    same = client.put(f"{API}/risk/settings", headers=admin_h, json={**s, "expected_updated_at": st["updated_at"]})
    assert (
        same.status_code == 200
        and same.json()["changed"] is False
        and _audit("system_setting", "risk", mark)[-1].action == "RISK_SETTINGS_REJECTED"
    )
    ok = client.put(
        f"{API}/risk/settings", headers=admin_h, json={**s, "high_threshold": 72.5, "expected_updated_at": st["updated_at"]}
    )
    assert ok.status_code == 200 and ok.json()["changed"] and ok.json()["settings"]["high_threshold"] == 72.5
    assert ok.json()["last_change"]["by"] == "admin" and ok.json()["updated_at"] != st["updated_at"]
    upd = _audit("system_setting", "risk", mark)[-1]
    assert upd.action == "RISK_SETTINGS_UPDATED" and upd.details["changed"] == ["high_threshold"]
    assert '"high_threshold": 72.5' in upd.new_value and f'"high_threshold": {s["high_threshold"]}' in upd.old_value
    assert get_runtime().engine.settings.high_threshold == 72.5
    with SessionLocal() as db:
        assert build_engine(db).settings.high_threshold == 72.5
    stale = client.put(f"{API}/risk/settings", headers=admin_h, json={**s, "expected_updated_at": st["updated_at"]})
    assert stale.status_code == 409 and get_runtime().engine.settings.high_threshold == 72.5
    restored = client.put(f"{API}/risk/settings", headers=admin_h,
                          json={**s, "expected_updated_at": client.get(f"{API}/risk/settings", headers=admin_h).json()["updated_at"]})
    assert restored.status_code == 200 and restored.json()["settings"] == s


def test_analyst_config_changes_are_denied_and_audited(client, analyst_h):
    mark = _max_audit_id()
    st = client.get(f"{API}/risk/settings", headers=analyst_h).json()["settings"]
    attempts = [
        ("patch", f"/risk/rules/{RULE}", {"enabled": False}, ("risk_rule", RULE)),
        ("put", "/risk/settings", st, ("system_setting", "risk")),
        ("put", "/settings/drift", {"value": {"min_rows": 10}}, ("system_setting", "drift")),
    ]
    for method, path, body, (et, eid) in attempts:
        r = getattr(client, method)(API + path, headers=analyst_h, json=body)
        assert r.status_code == 403 and r.json()["detail"] == "administrator role required"
        denied = _audit(et, eid, mark)
        assert denied and denied[-1].action == "CONFIG_CHANGE_DENIED" and denied[-1].actor_name == "analyst"
        assert denied[-1].details["role"] == "ANALYST"
    assert _rule(client, analyst_h)["enabled"] is True


def test_drift_settings_bounds_types_and_metadata(client, admin_h, analyst_h):
    meta = client.get(f"{API}/settings/drift", headers=analyst_h).json()
    assert "drift check" in meta["used_by"] and meta["bounds"]["window_rows"] == {"min": 50, "max": 100000}
    assert client.get(f"{API}/settings/preferences", headers=analyst_h).json()["used_by"] is None
    assert client.get(f"{API}/settings/nope", headers=analyst_h).status_code == 404
    for value in (
        {"window_rows": -5},
        {"window_rows": True},
        {"min_rows": 5000, "window_rows": 1000},
        {"moderate": 0.5},
        {"significant": 50.0},
        {"moderate": "0.1"},
        {"min_rows": 0},
    ):
        r = client.put(f"{API}/settings/drift", headers=admin_h, json={"value": value})
        assert r.status_code == 422, (value, r.text)
    before = client.get(f"{API}/settings/drift", headers=admin_h).json()
    stale = client.put(
        f"{API}/settings/drift",
        headers=admin_h,
        json={"value": {"min_rows": 150}, "expected_updated_at": "2000-01-01T00:00:00+00:00"},
    )
    assert stale.status_code == 409
    ok = client.put(
        f"{API}/settings/drift",
        headers=admin_h,
        json={"value": {"min_rows": 150}, "expected_updated_at": before["updated_at"]},
    )
    assert ok.status_code == 200 and ok.json()["drift"]["min_rows"] == 150 and ok.json()["changed"]
    assert client.get(f"{API}/settings/drift", headers=admin_h).json()["last_change"]["by"] == "admin"
    assert (
        client.put(
            f"{API}/settings/drift", headers=admin_h,
            json={"value": {"min_rows": before["value"]["min_rows"]},
                  "expected_updated_at": client.get(f"{API}/settings/drift", headers=admin_h).json()["updated_at"]},
        ).status_code
        == 200
    )


def test_settings_audit_trail_can_be_queried_for_several_entity_types(client, admin_h):
    r = client.get(f"{API}/audit?entity_type=risk_rule,system_setting&size=200", headers=admin_h).json()
    types = {a["entity_type"] for a in r["items"]}
    assert types == {"risk_rule", "system_setting"} and r["total"] >= len(r["items"])
    assert {"RULE_UPDATED", "RISK_SETTINGS_UPDATED", "CONFIG_CHANGE_DENIED"} <= {a["action"] for a in r["items"]}
