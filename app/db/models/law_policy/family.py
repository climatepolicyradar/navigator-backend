import enum

import sqlalchemy as sa
from sqlalchemy.orm import relationship

from app.db.models.app import Organisation
from app.db.models.document import PhysicalDocument
from app.db.session import Base
from .geography import Geography


class FamilyCategory(str, enum.Enum):
    EXECUTIVE = "EXECUTIVE"
    LEGISLATIVE = "LEGISLATIVE"


class Variant(Base):
    """The type of variant of a document within a family.

    Variants are described in the family/collection/doc notion page.
    Examples: "original language", "official translation", "unofficial translation.
    """

    __tablename__ = "variant"
    variant_name = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)


class FamilyStatus(str, enum.Enum):
    CREATED = "Created"
    PUBLISHED = "Published"
    DELETED = "Deleted"


class Family(Base):

    __tablename__ = "family"

    title = sa.Column(sa.Text, nullable=False)
    import_id = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)
    geography_id = sa.Column(sa.ForeignKey(Geography.id), nullable=False)
    category_name = sa.Column(sa.Enum(FamilyCategory), nullable=False)
    family_status = sa.Column(sa.Enum(FamilyStatus), default=FamilyStatus.CREATED, nullable=False)

    slugs: list["Slug"] = relationship("Slug", innerjoin=True, lazy="joined")


class DocumentStatus(str, enum.Enum):
    CREATED = "Created"
    PUBLISHED = "Published"
    DELETED = "Deleted"


class FamilyDocumentType(Base):
    """A document type.

    E.g. strategy, plan, law
    """

    __tablename__ = "family_document_type"
    name = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)


class FamilyDocument(Base):
    __tablename__ = "family_document"

    family_import_id = sa.Column(sa.ForeignKey(Family.import_id), nullable=False)
    physical_document_id = sa.Column(
        sa.ForeignKey(PhysicalDocument.id), nullable=False, unique=True
    )

    cdn_object = sa.Column(sa.Text, nullable=True)
    import_id = sa.Column(sa.Text, primary_key=True)
    variant_name = sa.Column(sa.ForeignKey(Variant.variant_name), nullable=False)
    document_status = sa.Column(sa.Enum(DocumentStatus), default=DocumentStatus.CREATED, nullable=False)
    document_type = sa.Column(sa.ForeignKey(FamilyDocumentType.name), nullable=False)

    slugs: list["Slug"] = relationship("Slug", innerjoin=True, lazy="joined")
    physical_document: PhysicalDocument = relationship(PhysicalDocument, lazy="joined")


class FamilyOrganisation(Base):

    __tablename__ = "family_organisation"

    family_import_id = sa.Column(sa.ForeignKey(Family.import_id), nullable=False)
    organisation_id = sa.Column(sa.ForeignKey(Organisation.id), nullable=False)

    sa.PrimaryKeyConstraint(family_import_id)


class Slug(Base):

    __tablename__ = "slug"
    __table_args__ = (
        sa.CheckConstraint(
            "num_nonnulls(family_import_id, family_document_import_id) = 1",
            name="must_reference_exactly_one_entity",
        ),
        sa.PrimaryKeyConstraint("name", name="pk_slug"),
    )

    name = sa.Column(sa.Text, primary_key=True)
    family_import_id = sa.Column(sa.ForeignKey(Family.import_id))
    family_document_import_id = sa.Column(sa.ForeignKey(FamilyDocument.import_id))
