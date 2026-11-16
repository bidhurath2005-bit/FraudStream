"""Phase 11C reports: every report type against direct database counts, server-side filters, period
bounds, RBAC, persistence/history, CSV escaping (commas, quotes, newlines, Unicode, formula guard), PDF
export (embedded TrueType font), empty periods, large/truncated reports, failures, and the migration."""

from __future__ import annotations

import csv
import io
import re
import time
from datetime import datetime, timedelta, timezone

import pytest
from app.database.session import SessionLocal
from app.models.entities import Alert, AuditLog, Investigation, ModelVersion, Prediction, Transaction
from app.services import reports as rep
from sqlalchemy import case, create_engine, func, inspect, select, text

API = "/api/v1"
UNICODE_TITLE = '=HYPERLINK("x") Zürich, “quoted” 日本 — case'
FINDINGS = 'Line one, with a comma and "double quotes".\nLine two: Ünïcödé — São Paulo, Kraków, 東京 ☃\nLine three'


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _window(hours: float = 24) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    return now - timedelta(hours=hours), now + timedelta(minutes=1)


def _gen(client, h, report_type, start, end, **filters):
    r = client.post(
        f"{API}/reports", headers=h, json={"report_type": report_type, "start": _iso(start), "end": _iso(end), **filters}
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == "COMPLETED", body
    return body


def _summary(body) -> dict:
    return {m["key"]: m["value"] for m in body["content"]["summary"]}


def _section(body, sid) -> dict:
    return next(s for s in body["content"]["sections"] if s["id"] == sid)


def _ingest_alerts(client, h, make_events, scenario="account_takeover", seed=0):
    uids = []
    for i in range(4):
        evs = make_events(scenario, n_normal=12, seed=seed + i)
        for ev in evs:
            r = client.post(f"{API}/transactions/ingest", headers=h, json=ev.to_wire()).json()
            if r.get("alert_uid"):
                uids.append((ev.customer_id, r["alert_uid"]))
        if uids:
            return uids
    raise AssertionError("no alert produced")


_DATA: dict[str, str] = {}


@pytest.fixture
def data(client, analyst_h, make_events):
    """Real pipeline traffic + one resolved investigation with Unicode/CSV-hostile text (created once)."""
    if _DATA:
        return _DATA
    alerts = _ingest_alerts(client, analyst_h, make_events, seed=500)
    cust, uid = alerts[-1]
    same = [u for c, u in alerts if c == cust]
    inv = client.post(f"{API}/investigations", headers=analyst_h, json={"alert_uids": same, "title": UNICODE_TITLE}).json()
    case_uid = inv["case_uid"]
    assert (
        client.patch(
            f"{API}/investigations/{case_uid}/status", headers=analyst_h, json={"status": "IN_PROGRESS"}
        ).status_code
        == 200
    )
    r = client.patch(
        f"{API}/investigations/{case_uid}/status",
        headers=analyst_h,
        json={"status": "RESOLVED", "final_classification": "CONFIRMED_SUSPICIOUS", "findings": FINDINGS},
    )
    assert r.status_code == 200, r.text
    _DATA["case_uid"] = case_uid
    return _DATA


def test_catalogue_lists_only_backed_types_and_admin_only_system_activity(client, analyst_h, admin_h):
    t = client.get(f"{API}/reports/types", headers=analyst_h).json()
    types = {x["key"]: x for x in t["types"]}
    assert set(types) == {
        "fraud_activity",
        "risk_analytics",
        "alert_summary",
        "investigation",
        "model_performance",
        "system_activity",
    }
    assert types["system_activity"]["admin_only"] and not types["system_activity"]["available"]
    assert [f["key"] for f in types["alert_summary"]["filters"]] == ["risk_level", "alert_status"]
    assert types["risk_analytics"]["filters"] == [] and t["persisted"] is True
    adm = {x["key"]: x for x in client.get(f"{API}/reports/types", headers=admin_h).json()["types"]}
    assert adm["system_activity"]["available"]


def test_each_report_type_matches_direct_database_queries(client, analyst_h, admin_h, data):
    start, end = _window(24)
    with SessionLocal() as db:
        in_win = (Transaction.processed_at >= start, Transaction.processed_at < end)
        n, flagged = db.execute(
            select(func.count(), func.sum(case((Prediction.risk_category != "LOW", 1), else_=0)))
            .select_from(Transaction)
            .join(Prediction)
            .where(*in_win)
        ).one()
        alerts = db.scalar(select(func.count(Alert.id)).where(Alert.created_at >= start, Alert.created_at < end))
        cases = db.scalar(
            select(func.count(Investigation.id)).where(Investigation.created_at >= start, Investigation.created_at < end)
        )
        models = db.scalar(select(func.count(ModelVersion.id)))
    assert n > 0 and alerts > 0 and cases > 0
    fa = _summary(_gen(client, analyst_h, "fraud_activity", start, end))
    assert (fa["transactions_scored"], fa["flagged"]) == (n, int(flagged))
    assert fa["flag_rate"] == pytest.approx(int(flagged) / n)
    ra = _gen(client, analyst_h, "risk_analytics", start, end)
    assert _summary(ra)["transactions_scored"] == n
    assert sum(r["transactions"] for r in _section(ra, "score_distribution")["rows"]) == n
    assert sum(r["transactions"] for r in _section(ra, "confusion")["rows"]) <= n  # unlabelled rows are not in the matrix
    al = _gen(client, analyst_h, "alert_summary", start, end)
    assert _summary(al)["alerts"] == alerts and _section(al, "alerts")["total_rows"] == alerts
    assert sum(r["alerts"] for r in _section(al, "by_status")["rows"]) == alerts
    inv = _gen(client, analyst_h, "investigation", start, end)
    assert _summary(inv)["cases"] == cases
    row = next(r for r in _section(inv, "cases")["rows"] if r["case_uid"] == data["case_uid"])
    assert row["title"] == UNICODE_TITLE and row["findings"] == FINDINGS and row["classification"] == "CONFIRMED_SUSPICIOUS"
    mp = _gen(client, analyst_h, "model_performance", start, end)
    assert len(_section(mp, "registry")["rows"]) == models
    assert _summary(mp)["active_model"] is not None
    with SessionLocal() as db:
        events = db.scalar(select(func.count(AuditLog.id)).where(AuditLog.created_at >= start, AuditLog.created_at < end))
    sa = _gen(client, admin_h, "system_activity", start, end)
    # generated after the count above; the report's own REPORT_GENERATED row is written after it was computed
    assert _summary(sa)["events"] == events
    assert _summary(sa)["reports"] >= 5


def test_period_bounds_are_applied_on_the_server(client, analyst_h, data):
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        ids = list(db.scalars(select(Transaction.id).order_by(Transaction.id.desc()).limit(7)))
        db.execute(
            Transaction.__table__.update().where(Transaction.id.in_(ids)).values(processed_at=now - timedelta(days=40))
        )
        db.commit()
    s, e = now - timedelta(days=41), now - timedelta(days=39)
    with SessionLocal() as db:
        expected = db.scalar(
            select(func.count())
            .select_from(Transaction)
            .join(Prediction)
            .where(Transaction.processed_at >= s, Transaction.processed_at < e)
        )
    assert expected >= 7
    body = _gen(client, analyst_h, "fraud_activity", s, e)
    assert _summary(body)["transactions_scored"] == expected
    assert body["content"]["period"]["start"].startswith(s.strftime("%Y-%m-%dT%H:%M"))
    assert "end exclusive" in body["content"]["period"]["label"]
    # [s, e) is end-exclusive: the 7 rows stamped exactly at now-40d drop out when the period ends there
    assert (
        _summary(_gen(client, analyst_h, "fraud_activity", s, now - timedelta(days=40)))["transactions_scored"]
        == expected - 7
    )


def test_filters_are_server_side_and_unsupported_filters_are_rejected(client, analyst_h, data):
    start, end = _window(24)
    with SessionLocal() as db:
        high = db.scalar(
            select(func.count(Alert.id)).where(
                Alert.created_at >= start, Alert.created_at < end, Alert.risk_category == "HIGH"
            )
        )
    body = _gen(client, analyst_h, "alert_summary", start, end, risk_level="HIGH")
    assert body["filters"] == {"risk_level": "HIGH"} and _summary(body)["alerts"] == high
    assert all(r["risk_category"] == "HIGH" for r in _section(body, "alerts")["rows"])
    st = _gen(client, analyst_h, "alert_summary", start, end, alert_status="NEW")
    assert all(r["status"] == "NEW" for r in _section(st, "alerts")["rows"])
    low = _gen(client, analyst_h, "fraud_activity", start, end, risk_level="LOW")
    assert _summary(low)["flagged"] == 0 and _summary(low)["transactions_scored"] > 0
    res = _gen(client, analyst_h, "investigation", start, end, investigation_status="RESOLVED")
    assert all(r["status"] == "RESOLVED" for r in _section(res, "cases")["rows"]) and _summary(res)["cases"] >= 1
    bad = [
        {"report_type": "risk_analytics", "risk_level": "HIGH"},  # filter the type cannot apply
        {"report_type": "investigation", "alert_status": "NEW"},
        {"report_type": "alert_summary", "risk_level": "SEVERE"},  # unknown value
        {"report_type": "nonexistent"},
    ]
    for b in bad:
        r = client.post(f"{API}/reports", headers=analyst_h, json={"start": _iso(start), "end": _iso(end), **b})
        assert r.status_code == 422, (b, r.text)
    for s, e in ((end, start), (start, start), (start - timedelta(days=400), end)):
        assert (
            client.post(
                f"{API}/reports", headers=analyst_h, json={"report_type": "risk_analytics", "start": _iso(s), "end": _iso(e)}
            ).status_code
            == 422
        )
    naive = client.post(
        f"{API}/reports",
        headers=analyst_h,
        json={"report_type": "risk_analytics", "start": "2026-01-01T00:00:00", "end": "2026-01-02T00:00:00"},
    )
    assert naive.status_code == 422 and "timezone" in naive.json()["detail"]


def test_system_activity_is_admin_only_everywhere(client, analyst_h, admin_h):
    start, end = _window(1)
    assert (
        client.post(
            f"{API}/reports",
            headers=analyst_h,
            json={"report_type": "system_activity", "start": _iso(start), "end": _iso(end)},
        ).status_code
        == 403
    )
    uid = _gen(client, admin_h, "system_activity", start, end)["report_uid"]
    assert client.get(f"{API}/reports/generated/{uid}", headers=analyst_h).status_code == 403
    assert client.get(f"{API}/reports/generated/{uid}/download?format=csv", headers=analyst_h).status_code == 403
    listed = client.get(f"{API}/reports/generated?size=100", headers=analyst_h).json()["items"]
    assert uid not in {r["report_uid"] for r in listed}
    assert uid in {r["report_uid"] for r in client.get(f"{API}/reports/generated?size=100", headers=admin_h).json()["items"]}
    events = _section(client.get(f"{API}/reports/generated/{uid}", headers=admin_h).json(), "events")["rows"]
    assert not any(re.search(r"password|secret|token", (e["details"] or "") + (e["new_value"] or ""), re.I) for e in events)


def test_csv_export_escapes_commas_quotes_newlines_unicode_and_formulas(client, analyst_h, data):
    start, end = _window(24)
    uid = _gen(client, analyst_h, "investigation", start, end)["report_uid"]
    r = client.get(f"{API}/reports/generated/{uid}/download?format=csv&section=cases", headers=analyst_h)
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/csv")
    cd = r.headers["content-disposition"]
    assert re.fullmatch(r'attachment; filename="fraudstream-investigation-\d{8}-\d{8}-cases-[0-9a-f]{8}\.csv"', cd), cd
    assert r.content.startswith(b"\xef\xbb\xbf")  # UTF-8 BOM so spreadsheets detect UTF-8
    rows = list(csv.reader(io.StringIO(r.content.decode("utf-8-sig"))))
    head = rows[0]
    row = next(x for x in rows[1:] if x[head.index("Case id")] == data["case_uid"])
    assert row[head.index("Title")] == "'" + UNICODE_TITLE  # formula guard, otherwise byte-identical
    assert row[head.index("Findings")] == FINDINGS  # commas, quotes, newlines, Unicode round-trip
    full = client.get(f"{API}/reports/generated/{uid}/download?format=csv", headers=analyst_h)
    text_ = full.content.decode("utf-8-sig")
    parsed = list(csv.reader(io.StringIO(text_)))
    assert parsed[0] == ["FraudStream report", "Investigation report"] and ["Report ID", uid] in parsed
    assert ["Section", "Case list"] in parsed and ["Limitations"] in parsed
    assert any(FINDINGS in cell for line in parsed for cell in line)


def test_csv_formula_guard_unit():
    assert rep.csv_value("=1+1") == "'=1+1" and rep.csv_value("+cmd") == "'+cmd" and rep.csv_value("@SUM") == "'@SUM"
    assert rep.csv_value("-2") == "'-2" and rep.csv_value(-2) == -2 and rep.csv_value(0.5) == 0.5
    assert rep.csv_value(None) == "" and rep.csv_value(True) == "true" and rep.csv_value("plain") == "plain"


def test_pdf_export_is_a_pdf_with_an_embedded_unicode_font(client, analyst_h, data):
    start, end = _window(24)
    for t in ("fraud_activity", "risk_analytics", "alert_summary", "investigation", "model_performance"):
        uid = _gen(client, analyst_h, t, start, end)["report_uid"]
        r = client.get(f"{API}/reports/generated/{uid}/download?format=pdf", headers=analyst_h)
        assert r.status_code == 200 and r.headers["content-type"] == "application/pdf", (t, r.text[:200])
        assert r.content.startswith(b"%PDF-") and r.content.rstrip().endswith(b"%%EOF")
        assert b"/FontFile2" in r.content  # TrueType embedded (not Latin-1-only core fonts)
        assert re.search(r'filename="fraudstream-[a-z-]+-\d{8}-\d{8}-[0-9a-f]{8}\.pdf"', r.headers["content-disposition"])
    bad = client.get(f"{API}/reports/generated/{uid}/download?format=pdf&section=registry", headers=analyst_h)
    assert bad.status_code == 422


def test_empty_period_reports_zeros_and_unavailable_rates_and_still_exports(client, analyst_h, admin_h):
    s, e = datetime(2001, 1, 1, tzinfo=timezone.utc), datetime(2001, 1, 8, tzinfo=timezone.utc)
    for t in ("fraud_activity", "risk_analytics", "alert_summary", "investigation", "model_performance"):
        body = _gen(client, analyst_h, t, s, e)
        summ = _summary(body)
        assert all(
            v in (0, None)
            for k, v in summ.items()
            if k not in ("active_model", "test_pr_auc", "test_precision", "test_recall")
        ), (t, summ)
        if t == "fraud_activity":
            assert summ["flag_rate"] is None  # 0/0 is "not available", never 0 %
        for fmt in ("csv", "pdf"):
            assert (
                client.get(
                    f"{API}/reports/generated/{body['report_uid']}/download?format={fmt}", headers=analyst_h
                ).status_code
                == 200
            )
    assert _summary(_gen(client, admin_h, "system_activity", s, e))["events"] == 0


def test_large_reports_are_capped_and_say_so(client, analyst_h, data, make_events, monkeypatch):
    start, end = _window(24)
    seed = 700
    while True:  # make sure the window holds more alerts than the (lowered) cap
        with SessionLocal() as db:
            if db.scalar(select(func.count(Alert.id)).where(Alert.created_at >= start)) > 4:
                break
        _ingest_alerts(client, analyst_h, make_events, seed=seed)
        seed += 10
    monkeypatch.setattr(rep, "DETAIL_ROW_CAP", 3)
    monkeypatch.setattr(rep, "PDF_ROW_CAP", 2)
    monkeypatch.setattr(rep, "PREVIEW_ROWS", 1)
    body = client.post(
        f"{API}/reports", headers=analyst_h, json={"report_type": "alert_summary", "start": _iso(start), "end": _iso(end)}
    ).json()
    sec = _section(body, "alerts")
    assert sec["total_rows"] > 3 and sec["stored_rows"] == 3 and sec["truncated"] and len(sec["rows"]) == 1
    csv_text = client.get(
        f"{API}/reports/generated/{body['report_uid']}/download?format=csv", headers=analyst_h
    ).content.decode("utf-8-sig")
    assert f"(stored 3 of {sec['total_rows']} rows" in csv_text
    assert (
        client.get(f"{API}/reports/generated/{body['report_uid']}/download?format=pdf", headers=analyst_h).status_code == 200
    )


def test_pdf_renderer_handles_thousands_of_rows_and_huge_cells():
    rows = [
        {"a": f"row {i}, “é” <b>&amp;", "n": i, "t": ("x" * 50 + "\n") * 400 if i == 3 else "short"} for i in range(5000)
    ]
    content = {
        "report_uid": "00000000-0000-0000-0000-000000000000",
        "report_type": "investigation",
        "title": "Load test",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_by": "test",
        "profile": "local",
        "period": {"start": "2026-01-01T00:00:00+00:00", "end": "2026-01-02T00:00:00+00:00", "basis": "x", "label": "x"},
        "filters": {},
        "summary": [rep.metric("n", "Rows", 5000)],
        "sections": [rep.section("s", "Big", [("a", "A", "text"), ("n", "N", "int"), ("t", "T", "longtext")], rows)],
        "limitations": ["synthetic"],
    }
    t0 = time.perf_counter()
    pdf = rep.render_pdf(content)  # cell taller than a page would raise LayoutError
    assert pdf.startswith(b"%PDF-") and time.perf_counter() - t0 < 60
    assert len(rep.render_csv(content)) > 5000 * 10


def test_failed_generation_is_persisted_as_failed_and_cannot_be_downloaded(client, analyst_h, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("simulated failure with /internal/path")

    monkeypatch.setitem(rep.BUILDERS, "risk_analytics", boom)
    start, end = _window(1)
    r = client.post(
        f"{API}/reports", headers=analyst_h, json={"report_type": "risk_analytics", "start": _iso(start), "end": _iso(end)}
    )
    body = r.json()
    assert r.status_code == 201 and body["status"] == "FAILED" and body["content"] is None
    assert "/internal/path" not in body["error"] and "RuntimeError" in body["error"]
    assert (
        client.get(f"{API}/reports/generated/{body['report_uid']}/download?format=pdf", headers=analyst_h).status_code == 409
    )
    hist = client.get(f"{API}/reports/generated?report_type=risk_analytics&size=5", headers=analyst_h).json()["items"]
    assert hist[0]["report_uid"] == body["report_uid"] and hist[0]["status"] == "FAILED"


def test_history_is_persisted_paginated_and_downloads_reproduce_the_snapshot(client, analyst_h, make_events):
    start, end = _window(24)
    first = _gen(client, analyst_h, "fraud_activity", start, end)
    for ev in make_events("velocity_burst", n_normal=10, seed=901):  # data changes after generation
        client.post(f"{API}/transactions/ingest", headers=analyst_h, json=ev.to_wire())
    again = client.get(f"{API}/reports/generated/{first['report_uid']}", headers=analyst_h).json()
    assert _summary(again) == _summary(first)  # stored snapshot, not a re-query
    p1 = client.get(f"{API}/reports/generated?page=1&size=2", headers=analyst_h).json()
    p2 = client.get(f"{API}/reports/generated?page=2&size=2", headers=analyst_h).json()
    assert p1["total"] == p2["total"] >= 4 and len(p1["items"]) == 2
    assert not {r["report_uid"] for r in p1["items"]} & {r["report_uid"] for r in p2["items"]}
    assert "content" not in p1["items"][0]
    only = client.get(f"{API}/reports/generated?report_type=fraud_activity&size=100", headers=analyst_h).json()["items"]
    assert only and all(r["report_type"] == "fraud_activity" for r in only)
    assert client.get(f"{API}/reports/generated?report_type=bogus", headers=analyst_h).status_code == 422
    assert client.get(f"{API}/reports/generated/does-not-exist", headers=analyst_h).status_code == 404
    assert (
        client.get(f"{API}/reports/generated/{first['report_uid']}/download?format=xlsx", headers=analyst_h).status_code
        == 422
    )
    assert (
        client.get(
            f"{API}/reports/generated/{first['report_uid']}/download?format=csv&section=nope", headers=analyst_h
        ).status_code
        == 404
    )
    with SessionLocal() as db:
        a = db.scalar(select(AuditLog).where(AuditLog.entity_id == first["report_uid"]))
        assert a.action == "REPORT_GENERATED" and a.actor_name == "analyst"


def test_migration_0002_is_additive_on_a_populated_0001_database(tmp_path):
    from alembic import command
    from app.database.migrate import alembic_config, upgrade

    url = f"sqlite:///{(tmp_path / 'm.db').as_posix()}"
    upgrade(url, "0001")
    eng = create_engine(url)
    with eng.begin() as c:
        c.execute(text("INSERT INTO roles (id, name, description) VALUES (1, 'ADMIN', 'x')"))
        c.execute(
            text("INSERT INTO system_settings (key, value, updated_at) VALUES ('risk', '{\"a\": 1}', '2026-01-01 00:00:00')")
        )
    upgrade(url, "head")
    insp = inspect(eng)
    assert "generated_reports" in insp.get_table_names()
    with eng.connect() as c:
        assert c.execute(text("SELECT name FROM roles")).scalars().all() == ["ADMIN"]
        assert c.execute(text("SELECT value FROM system_settings WHERE key='risk'")).scalar() == '{"a": 1}'
        assert c.execute(text("SELECT version_num FROM alembic_version")).scalar() == "0002"
    command.downgrade(alembic_config(url), "0001")  # reversible, and only drops the new table
    assert "generated_reports" not in inspect(create_engine(url)).get_table_names()
    with create_engine(url).connect() as c:
        assert c.execute(text("SELECT name FROM roles")).scalars().all() == ["ADMIN"]
