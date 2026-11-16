"""generated reports (Phase 11C)

Additive only: creates one new table. No existing table, column or row is touched, and the
downgrade drops only this table.

Revision ID: 0002
Revises: 0001
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

import app.models.entities

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

JSON_T = sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql")


def upgrade() -> None:
    op.create_table(
        "generated_reports",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("report_uid", sa.String(length=36), nullable=False),
        sa.Column("report_type", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("period_start", app.models.entities.UTCDateTime(timezone=True), nullable=False),
        sa.Column("period_end", app.models.entities.UTCDateTime(timezone=True), nullable=False),
        sa.Column("filters", JSON_T, nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("error", sa.String(length=300), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("generated_by_id", sa.Integer(), nullable=True),
        sa.Column("generated_by_name", sa.String(length=64), nullable=False),
        sa.Column("created_at", app.models.entities.UTCDateTime(timezone=True), nullable=False),
        sa.Column("content", JSON_T, nullable=True),
        sa.ForeignKeyConstraint(["generated_by_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("report_uid"),
    )
    with op.batch_alter_table("generated_reports", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_generated_reports_report_type"), ["report_type"], unique=False)
        batch_op.create_index(batch_op.f("ix_generated_reports_created_at"), ["created_at"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("generated_reports", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_generated_reports_created_at"))
        batch_op.drop_index(batch_op.f("ix_generated_reports_report_type"))
    op.drop_table("generated_reports")
