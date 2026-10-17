"""switch default uuid generation to uuidv7

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-16 12:00:00

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column("payments", "id", server_default=sa.text("uuidv7()"))
    op.alter_column("outbox", "event_id", server_default=sa.text("uuidv7()"))


def downgrade() -> None:
    op.alter_column("payments", "id", server_default=sa.text("gen_random_uuid()"))
    op.alter_column("outbox", "event_id", server_default=sa.text("gen_random_uuid()"))
