"""Phase 12 report retention, on a throwaway SQLite database of its own (never the shared test database)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.models.entities import AuditLog, Base, GeneratedReport, Role, User
from app.services.report_retention import RetentionRule, eligible, prune

NOW = datetime(2026, 11, 15, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(tmp_path):
    eng = create_engine(f"sqlite:///{(tmp_path / 'retention.db').as_posix()}")
    Base.metadata.create_all(eng)
    with Session(eng) as s:
        role = Role(name="ADMIN")
        s.add(role)
        s.flush()
        u = User(username="admin", full_name="", password_hash="x", role_id=role.id)
        s.add(u)
        s.flush()
        # fraud_activity: 30 reports, one per day back from NOW; system_activity: 2 very old reports
        for i in range(30):
            s.add(_rep(u, "fraud_activity", NOW - timedelta(days=i, minutes=1), i))
        for i in range(2):
            s.add(_rep(u, "system_activity", NOW - timedelta(days=400 + i), 100 + i))
        s.add(AuditLog(actor_id=u.id, actor_name="admin", action="LOGIN", entity_type="user", entity_id="admin", details={}))
        s.commit()
        yield s
    eng.dispose()


def _rep(u: User, rtype: str, when: datetime, n: int) -> GeneratedReport:
    return GeneratedReport(report_uid=f"00000000-0000-0000-0000-{n:012d}", report_type=rtype, title=rtype,
                           period_start=when - timedelta(days=1), period_end=when, filters={}, status="COMPLETED",
                           row_count=1, generated_by_id=u.id, generated_by_name=u.username, created_at=when,
                           content={"x": n})


def _count(db: Session, model) -> int:  # type: ignore[no-untyped-def]
    return db.scalar(select(func.count()).select_from(model)) or 0


def test_rule_validation_refuses_unsafe_values():
    for bad in (RetentionRule(0), RetentionRule(-5), RetentionRule(30, keep_latest=0)):
        with pytest.raises(ValueError):
            bad.validate()


def test_dry_run_deletes_nothing_and_reports_what_would_go(db):
    out = prune(db, RetentionRule(older_than_days=7, keep_latest=5), now=NOW)
    # fraud_activity: created 7..29 days ago (23) are older than 7 days, all beyond the 5 newest -> 23;
    # system_activity: only 2 reports, both within keep_latest=5 -> kept despite being 400 days old
    assert out["apply"] is False and out["eligible"] == 23 and out["deleted"] == 0
    assert out["by_type"] == {"fraud_activity": 23}
    assert _count(db, GeneratedReport) == 32


def test_apply_deletes_exactly_the_eligible_reports_and_audits_them(db):
    expected = {r.report_uid for r in eligible(db, RetentionRule(7, 5), now=NOW)}
    audits_before = _count(db, AuditLog)
    out = prune(db, RetentionRule(older_than_days=7, keep_latest=5), apply=True, actor="test", now=NOW)
    assert out["deleted"] == 23
    left = {r.report_uid: r for r in db.scalars(select(GeneratedReport))}
    assert len(left) == 9 and not (expected & set(left))
    assert all(r.created_at > NOW - timedelta(days=7) or r.report_type == "system_activity" for r in left.values())
    # unrelated rows untouched; exactly one audit record describing the purge
    assert _count(db, User) == 1 and _count(db, Role) == 1
    assert _count(db, AuditLog) == audits_before + 1
    a = db.scalar(select(AuditLog).where(AuditLog.action == "REPORTS_PURGED"))
    assert a.entity_type == "report_retention" and a.details["count"] == 23 and a.details["actor"] == "test"
    assert {x["report_uid"] for x in a.details["reports"]} == expected and "keeping the 5 most recent" in a.details["rule"]
    # idempotent: a second run finds nothing more
    assert prune(db, RetentionRule(7, 5), apply=True, now=NOW)["deleted"] == 0


def test_keep_latest_and_type_filter(db):
    assert prune(db, RetentionRule(older_than_days=1, keep_latest=1, report_type="system_activity"),
                 apply=True, now=NOW)["deleted"] == 1
    kinds = [r.report_type for r in db.scalars(select(GeneratedReport))]
    assert kinds.count("system_activity") == 1 and kinds.count("fraud_activity") == 30
