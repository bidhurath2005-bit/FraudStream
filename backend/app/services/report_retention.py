"""Report retention (Phase 12): an explicit, documented, audited clean-up of stored report snapshots.

Rule: a stored report is ELIGIBLE for deletion only when BOTH hold
  1. it was generated more than ``older_than_days`` days ago (``generated_reports.created_at``), and
  2. it is not among the ``keep_latest`` most recently generated reports (counted per report type, so a rarely
     generated type keeps its history), and, if ``report_type`` is given, it is of that type.

Safety: dry-run by default (nothing is deleted without ``apply=True``); ``older_than_days`` must be >= 1 and
``keep_latest`` >= 1; only rows of ``generated_reports`` are touched (selected and deleted by primary key, nothing
cascades — no other table references them); deletion and the audit record are ONE transaction; the audit record
(``REPORTS_PURGED``, entity ``report_retention``) lists the rule, the count and every deleted report id/type/date.
Nothing runs automatically: there is no scheduler. Exposed as ``python -m app.cli prune-reports``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.models.entities import GeneratedReport
from app.services import audit

MAX_AUDITED_IDS = 5000


@dataclass(frozen=True)
class RetentionRule:
    older_than_days: int
    keep_latest: int = 20
    report_type: str | None = None

    def validate(self) -> None:
        if self.older_than_days < 1:
            raise ValueError("older_than_days must be >= 1")
        if self.keep_latest < 1:
            raise ValueError("keep_latest must be >= 1 (the newest reports are always kept)")

    def describe(self) -> str:
        t = f" of type {self.report_type}" if self.report_type else ""
        return (f"delete stored reports{t} generated more than {self.older_than_days} days ago, keeping the "
                f"{self.keep_latest} most recent report(s) of each type regardless of age")


def eligible(db: Session, rule: RetentionRule, now: datetime | None = None) -> list[GeneratedReport]:
    rule.validate()
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=rule.older_than_days)
    rank = func.row_number().over(partition_by=GeneratedReport.report_type,
                                  order_by=(GeneratedReport.created_at.desc(), GeneratedReport.id.desc())).label("rk")
    ranked = select(GeneratedReport.id, GeneratedReport.created_at, GeneratedReport.report_type, rank).subquery()
    ids = select(ranked.c.id).where(ranked.c.rk > rule.keep_latest, ranked.c.created_at < cutoff)
    if rule.report_type:
        ids = ids.where(ranked.c.report_type == rule.report_type)
    return list(db.scalars(select(GeneratedReport).where(GeneratedReport.id.in_(ids))
                           .order_by(GeneratedReport.created_at, GeneratedReport.id)))


def prune(db: Session, rule: RetentionRule, *, apply: bool = False, actor: str = "cli",
          now: datetime | None = None) -> dict[str, Any]:
    victims = eligible(db, rule, now)
    total_before = db.scalar(select(func.count(GeneratedReport.id))) or 0
    summary: dict[str, Any] = {
        "rule": rule.describe(), "older_than_days": rule.older_than_days, "keep_latest": rule.keep_latest,
        "report_type": rule.report_type, "apply": apply, "stored_reports": total_before, "eligible": len(victims),
        "oldest": victims[0].created_at.isoformat() if victims else None,
        "newest": victims[-1].created_at.isoformat() if victims else None,
        "by_type": {}, "deleted": 0,
    }
    for v in victims:
        summary["by_type"][v.report_type] = summary["by_type"].get(v.report_type, 0) + 1
    if not apply or not victims:
        db.rollback()
        return summary
    listed = [{"report_uid": v.report_uid, "report_type": v.report_type, "created_at": v.created_at.isoformat()}
              for v in victims[:MAX_AUDITED_IDS]]
    ids = [v.id for v in victims]
    db.expunge_all()
    res = db.execute(delete(GeneratedReport).where(GeneratedReport.id.in_(ids)))
    if res.rowcount != len(ids):
        db.rollback()
        raise RuntimeError(f"expected to delete {len(ids)} reports, the database reported {res.rowcount}; rolled back")
    audit.record(db, None, "REPORTS_PURGED", "report_retention", f"older_than_{rule.older_than_days}d",
                 details={"rule": rule.describe(), "actor": actor, "count": len(ids), "by_type": summary["by_type"],
                          "reports": listed, "reports_listed": len(listed)})
    db.commit()
    summary["deleted"] = len(ids)
    return summary
