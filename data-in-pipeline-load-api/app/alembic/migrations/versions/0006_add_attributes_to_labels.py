"""Add attributes to labels

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-01 13:54:55.660100

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0006"
down_revision: str | Sequence[str] | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    with op.batch_alter_table("label", schema=None) as batch_op:
        batch_op.add_column(sa.Column("attributes", sa.JSON(), nullable=True))

    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""

    with op.batch_alter_table("label", schema=None) as batch_op:
        batch_op.drop_column("attributes")

    # ### end Alembic commands ###
