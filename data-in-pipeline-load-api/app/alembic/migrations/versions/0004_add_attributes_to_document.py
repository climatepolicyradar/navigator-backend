"""add attributes to document

Revision ID: 0004
Revises: 0003
Create Date: 2026-03-11

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: str | Sequence[str] | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("document", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("attributes", sa.JSON(), nullable=False, server_default="{}")
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("document", schema=None) as batch_op:
        batch_op.drop_column("attributes")
