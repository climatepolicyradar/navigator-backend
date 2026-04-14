"""Migrate label IDs from {value} to {type}::{value}

Revision ID: 0008
Revises: 0007
Create Date: 2026-04-14

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0008"
down_revision: str | Sequence[str] | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

DOCUMENTLABELRELATIONSHIP_LABEL_FK = "fk_documentlabellink_label_id_label"
LABELLABELRELATIONSHIP_LABEL_FK = "fk_labellabelrelationship_label_id_label"
LABELLABELRELATIONSHIP_RELATED_LABEL_FK = (
    "fk_labellabelrelationship_related_label_id_label"
)


def upgrade() -> None:
    # Step 1: Drop FK constraints so we can freely update label.id
    op.drop_constraint(
        DOCUMENTLABELRELATIONSHIP_LABEL_FK,
        "documentlabelrelationship",
        type_="foreignkey",
    )
    op.drop_constraint(
        LABELLABELRELATIONSHIP_LABEL_FK,
        "labellabelrelationship",
        type_="foreignkey",
    )
    op.drop_constraint(
        LABELLABELRELATIONSHIP_RELATED_LABEL_FK,
        "labellabelrelationship",
        type_="foreignkey",
    )

    # Step 2: Update FK columns in link tables to point at the new label IDs.
    # We JOIN on the old label.id before updating label itself.
    # The NOT LIKE guard makes this idempotent.
    op.execute(
        """
        UPDATE documentlabelrelationship dlr
        SET label_id = l.type || '::' || l.id
        FROM label l
        WHERE dlr.label_id = l.id
          AND l.id NOT LIKE '%::%'
    """
    )

    op.execute(
        """
        UPDATE labellabelrelationship llr
        SET label_id = l.type || '::' || l.id
        FROM label l
        WHERE llr.label_id = l.id
          AND l.id NOT LIKE '%::%'
    """
    )

    op.execute(
        """
        UPDATE labellabelrelationship llr
        SET related_label_id = l.type || '::' || l.id
        FROM label l
        WHERE llr.related_label_id = l.id
          AND l.id NOT LIKE '%::%'
    """
    )

    # Step 3: Update the label PKs themselves
    op.execute(
        """
        UPDATE label
        SET id = type || '::' || id
        WHERE id NOT LIKE '%::%'
    """
    )

    # Step 4: Restore FK constraints
    op.create_foreign_key(
        DOCUMENTLABELRELATIONSHIP_LABEL_FK,
        "documentlabelrelationship",
        "label",
        ["label_id"],
        ["id"],
    )
    op.create_foreign_key(
        LABELLABELRELATIONSHIP_LABEL_FK,
        "labellabelrelationship",
        "label",
        ["label_id"],
        ["id"],
    )
    op.create_foreign_key(
        LABELLABELRELATIONSHIP_RELATED_LABEL_FK,
        "labellabelrelationship",
        "label",
        ["related_label_id"],
        ["id"],
    )


def downgrade() -> None:
    # Step 1: Drop FK constraints
    op.drop_constraint(
        DOCUMENTLABELRELATIONSHIP_LABEL_FK,
        "documentlabelrelationship",
        type_="foreignkey",
    )
    op.drop_constraint(
        LABELLABELRELATIONSHIP_LABEL_FK,
        "labellabelrelationship",
        type_="foreignkey",
    )
    op.drop_constraint(
        LABELLABELRELATIONSHIP_RELATED_LABEL_FK,
        "labellabelrelationship",
        type_="foreignkey",
    )

    # Step 2: Restore FK columns in link tables to the old bare label IDs.
    # SUBSTRING(id FROM LENGTH(type) + 3) strips the leading "{type}::" prefix.
    op.execute(
        """
        UPDATE documentlabelrelationship dlr
        SET label_id = SUBSTRING(l.id FROM LENGTH(l.type) + 3)
        FROM label l
        WHERE dlr.label_id = l.id
          AND l.id LIKE '%::%'
    """
    )

    op.execute(
        """
        UPDATE labellabelrelationship llr
        SET label_id = SUBSTRING(l.id FROM LENGTH(l.type) + 3)
        FROM label l
        WHERE llr.label_id = l.id
          AND l.id LIKE '%::%'
    """
    )

    op.execute(
        """
        UPDATE labellabelrelationship llr
        SET related_label_id = SUBSTRING(l.id FROM LENGTH(l.type) + 3)
        FROM label l
        WHERE llr.related_label_id = l.id
          AND l.id LIKE '%::%'
    """
    )

    # Step 3: Restore label PKs
    op.execute(
        """
        UPDATE label
        SET id = SUBSTRING(id FROM LENGTH(type) + 3)
        WHERE id LIKE '%::%'
    """
    )

    # Step 4: Restore FK constraints
    op.create_foreign_key(
        DOCUMENTLABELRELATIONSHIP_LABEL_FK,
        "documentlabelrelationship",
        "label",
        ["label_id"],
        ["id"],
    )
    op.create_foreign_key(
        LABELLABELRELATIONSHIP_LABEL_FK,
        "labellabelrelationship",
        "label",
        ["label_id"],
        ["id"],
    )
    op.create_foreign_key(
        LABELLABELRELATIONSHIP_RELATED_LABEL_FK,
        "labellabelrelationship",
        "label",
        ["related_label_id"],
        ["id"],
    )
