"""add index for slug lookup on document attribute

Revision ID: 0010
Revises: 0009
Create Date: 2026-08-26 15:51:10.426193

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0010"
down_revision: str | Sequence[str] | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_document_deprecated_slug "
        "ON document ((attributes ->> 'deprecated_slug'))"
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP INDEX IF EXISTS ix_document_deprecated_slug")
