"""
Updated family doc primary key constraint to include language source

Revision ID: 0026
Revises: 0025
Create Date: 2023-12-07 10:58:25.326939

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "0026"
down_revision = "0025"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        ALTER TABLE physical_document_language
        DROP CONSTRAINT pk_physical_document_language;
        """
    )
    op.execute(
        """
        ALTER TABLE physical_document_language
        ADD CONSTRAINT pk_physical_document_language
        PRIMARY KEY (language_id, document_id, source);
        """
    )


def downgrade():
    op.execute(
        """
        ALTER TABLE physical_document_language
        DROP CONSTRAINT pk_physical_document_language;
        """
    )
    op.execute(
        """
        ALTER TABLE physical_document_language
        ADD CONSTRAINT pk_physical_document_language
        PRIMARY KEY (language_id, document_id);
        """
    )
