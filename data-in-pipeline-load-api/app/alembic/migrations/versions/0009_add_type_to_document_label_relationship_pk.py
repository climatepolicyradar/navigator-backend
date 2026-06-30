"""Add type to documentlabelrelationship primary key

Revision ID: 0009
Revises: 0008
Create Date: 2026-06-30

"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "0009"
down_revision: str | Sequence[str] | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    conn = op.get_bind()
    # Discover the actual PK constraint name (varies depending on whether the
    # table was created via Alembic migrations or SQLModel.metadata.create_all)
    row = conn.execute(
        text(
            "SELECT constraint_name FROM information_schema.table_constraints "
            "WHERE table_name = 'documentlabelrelationship' AND constraint_type = 'PRIMARY KEY'"
        )
    ).fetchone()
    pk_name = row[0]
    conn.execute(text(f"ALTER TABLE documentlabelrelationship DROP CONSTRAINT {pk_name}"))
    conn.execute(
        text(
            "ALTER TABLE documentlabelrelationship "
            f"ADD CONSTRAINT {pk_name} PRIMARY KEY (document_id, label_id, type)"
        )
    )


def downgrade() -> None:
    """Downgrade schema."""
    conn = op.get_bind()
    row = conn.execute(
        text(
            "SELECT constraint_name FROM information_schema.table_constraints "
            "WHERE table_name = 'documentlabelrelationship' AND constraint_type = 'PRIMARY KEY'"
        )
    ).fetchone()
    pk_name = row[0]
    conn.execute(text(f"ALTER TABLE documentlabelrelationship DROP CONSTRAINT {pk_name}"))
    conn.execute(
        text(
            "ALTER TABLE documentlabelrelationship "
            f"ADD CONSTRAINT {pk_name} PRIMARY KEY (document_id, label_id)"
        )
    )
