"""Risk Analytics endpoints: optional time window, merchant category filter, stable ranking, limit
bounds. Real pipeline into the per-session temp SQLite DB; some rows are moved back in time so the
window has something to exclude. Expected values are computed directly from the database."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.database.session import SessionLocal
from app.models.entities import Prediction, Transaction
from sqlalchemy import case, func, select, update

API = "/api/v1"


def _ingest(client, h, evs):
    for ev in evs:
        assert client.post(f"{API}/transactions/ingest", headers=h, json=ev.to_wire()).status_code == 202


def _age_some_rows(n: int = 25) -> set[int]:
    """Move the n oldest transactions 3 h into the past (processed_at)."""
    with SessionLocal() as db:
        ids = list(db.scalars(select(Transaction.id).order_by(Transaction.id).limit(n)))
        db.execute(update(Transaction).where(Transaction.id.in_(ids))
                   .values(processed_at=datetime.now(timezone.utc) - timedelta(hours=3)))
        db.commit()
    return set(ids)


def _db_counts(minutes: int | None) -> tuple[int, int]:
    with SessionLocal() as db:
        q = select(func.count(Transaction.id), func.sum(case((Prediction.risk_category != "LOW", 1), else_=0))).join(Prediction)
        if minutes:
            q = q.where(Transaction.processed_at >= datetime.now(timezone.utc) - timedelta(minutes=minutes))
        n, f = db.execute(q).one()
        return int(n or 0), int(f or 0)


def test_time_window_filters_every_risk_endpoint(client, analyst_h, make_events):
    _ingest(client, analyst_h, make_events("velocity_burst", n_normal=20, seed=71))
    _age_some_rows()
    all_n, all_f = _db_counts(None)
    win_n, win_f = _db_counts(60)
    assert 0 < win_n < all_n, "test needs rows both inside and outside the window"

    m_all = client.get(f"{API}/analytics/merchants", headers=analyst_h).json()
    m_win = client.get(f"{API}/analytics/merchants?minutes=60", headers=analyst_h).json()
    assert sum(c["count"] for c in m_all["categories"]) == all_n and sum(c["flagged"] for c in m_all["categories"]) == all_f
    assert sum(c["count"] for c in m_win["categories"]) == win_n and sum(c["flagged"] for c in m_win["categories"]) == win_f
    assert m_all["period"]["window_minutes"] is None and m_win["period"]["label"] == "last 60 minutes"

    h_win = client.get(f"{API}/analytics/hourly?minutes=60", headers=analyst_h).json()
    assert len(h_win["hours"]) == 24 and sum(x["count"] for x in h_win["hours"]) == win_n
    assert sum(x["flagged"] for x in h_win["hours"]) == win_f

    c_win = client.get(f"{API}/analytics/confusion?minutes=60", headers=analyst_h).json()["risk_engine_vs_simulator"]
    with SessionLocal() as db:
        labelled = db.scalar(select(func.count(Transaction.id)).join(Prediction).where(
            Transaction.sim_is_suspicious.is_not(None),
            Transaction.processed_at >= datetime.now(timezone.utc) - timedelta(minutes=60)))
    assert c_win["tp"] + c_win["fp"] + c_win["fn"] + c_win["tn"] == labelled

    p_win = client.get(f"{API}/analytics/patterns?minutes=60", headers=analyst_h).json()
    p_all = client.get(f"{API}/analytics/patterns", headers=analyst_h).json()
    assert sum(s["count"] for s in p_win["scenarios"]) < sum(s["count"] for s in p_all["scenarios"])
    assert p_win["rule_hits_basis"]["flagged_predictions_examined"] <= win_f
    assert p_win["period"]["window_minutes"] == 60
    with SessionLocal() as db:
        truth = dict(db.execute(select(Transaction.sim_scenario, func.sum(case((Transaction.sim_is_suspicious.is_(True), 1), else_=0)))
                                .join(Prediction).where(Transaction.sim_scenario.is_not(None)).group_by(Transaction.sim_scenario)).all())
    assert {s["scenario"]: s["labelled_suspicious"] for s in p_all["scenarios"]} == {k: int(v) for k, v in truth.items()}

    for ep in ("patterns", "confusion", "merchants", "hourly"):
        assert client.get(f"{API}/analytics/{ep}?minutes=0", headers=analyst_h).status_code == 422
        assert client.get(f"{API}/analytics/{ep}?minutes=50000", headers=analyst_h).status_code == 422


def test_merchant_category_filter_ranking_and_limit(client, analyst_h, make_events):
    _ingest(client, analyst_h, make_events("suspicious_merchant", n_normal=10, seed=72))
    allm = client.get(f"{API}/analytics/merchants?limit=50", headers=analyst_h).json()
    cat = max(allm["categories"], key=lambda c: c["count"])["category"]
    one = client.get(f"{API}/analytics/merchants?category={cat}&limit=50", headers=analyst_h).json()
    assert [c["category"] for c in one["categories"]] == [cat] and one["category"] == cat
    assert all(m["category"] == cat for m in one["top_flagged_merchants"])
    with SessionLocal() as db:
        assert one["categories"][0]["count"] == db.scalar(select(func.count(Transaction.id)).join(Prediction)
                                                          .where(Transaction.merchant_category == cat))
    top = allm["top_flagged_merchants"]
    keys = [(-m["flagged"], -m["count"], m["merchant_id"]) for m in top]
    assert keys == sorted(keys), "ranking must be deterministic: flagged desc, count desc, id"
    for m in top:
        assert 0 <= m["avg_risk"] <= m["max_risk"] <= 100 and m["flag_rate"] == m["flagged"] / m["count"]
    assert client.get(f"{API}/analytics/merchants?category=does_not_exist", headers=analyst_h).json()["categories"] == []
    # previously limit<=0 reached SQL (SQLite LIMIT -1 = unlimited); now rejected. >50 is still clamped.
    assert client.get(f"{API}/analytics/merchants?limit=0", headers=analyst_h).status_code == 422
    assert client.get(f"{API}/analytics/merchants?limit=-1", headers=analyst_h).status_code == 422
    assert len(client.get(f"{API}/analytics/merchants?limit=500", headers=analyst_h).json()["top_flagged_merchants"]) <= 50
    assert client.get(f"{API}/analytics/velocity?limit=0", headers=analyst_h).status_code == 422
