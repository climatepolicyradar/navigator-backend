import sqlalchemy as sa
from .family import Family, FamilyDocument

from app.db.session import Base


class Slug(Base):

    __tablename__ = "slug"
    __table_args__ = (
        sa.CheckConstraint(
            "num_nonnulls(family_id, family_document_id) = 1",
            name="must_reference_exactly_one_entity",
        ),
        sa.PrimaryKeyConstraint("name", name="pk_slug")
    )

    name = sa.Column(sa.Text, primary_key=True)
    family_id = sa.Column(sa.ForeignKey(Family.id))
    family_document_id = sa.Column(sa.ForeignKey(FamilyDocument.physical_document_id))

