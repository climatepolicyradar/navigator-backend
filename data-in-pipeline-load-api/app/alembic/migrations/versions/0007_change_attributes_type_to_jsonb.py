"""Change attributes type to JSONB

Revision ID: 0007
Revises: 0006
Create Date: 2026-04-13 12:18:30.795460

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0007"
down_revision: str | Sequence[str] | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("document") as batch_op:
        batch_op.alter_column(
            "attributes",
            existing_type=sa.JSON(),
            type_=postgresql.JSONB(),
            postgresql_using="attributes::jsonb",
        )

    with op.batch_alter_table("label") as batch_op:
        batch_op.alter_column(
            "attributes",
            existing_type=sa.JSON(),
            type_=postgresql.JSONB(),
            postgresql_using="attributes::jsonb",
        )

    # ### end Alembic commands ###


def downgrade() -> None:
    with op.batch_alter_table("label") as batch_op:
        batch_op.alter_column(
            "attributes",
            existing_type=postgresql.JSONB(),
            type_=sa.JSON(),
        )

    with op.batch_alter_table("document") as batch_op:
        batch_op.alter_column(
            "attributes",
            existing_type=postgresql.JSONB(),
            type_=sa.JSON(),
        )

    # ### end Alembic commands ###
