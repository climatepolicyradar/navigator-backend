import sqlalchemy as sa
from .family import Family, FamilyDocument

from app.db.session import Base


class Slug(Base):

    __tablename__ = "slug"
    __table_args__ = (
        sa.CheckConstraint(
            "num_nonnulls(family_import_id, family_document_import_id) = 1",
            name="must_reference_exactly_one_entity",
        ),
        sa.PrimaryKeyConstraint("name", name="pk_slug")
    )

    name = sa.Column(sa.Text, primary_key=True)
    family_import_id = sa.Column(sa.ForeignKey(Family.import_id))
    family_document_import_id = sa.Column(sa.ForeignKey(FamilyDocument.import_id))

