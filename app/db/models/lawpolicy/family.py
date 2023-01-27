import enum
import sqlalchemy as sa
from app.db.models.app import Organisation
from app.db.models.document import PhysicalDocument
from .geography import Geography
from app.db.session import Base


class FamilyCategory(Base):
    """A document-family category

    Currently:
        Policy, (executive)
        Law, (legislative)
        Case, (litigation)
    """

    __tablename__ = "family_category"

    name = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)


class FamilyType(Base):
    """A family type.

    E.g. Decree, Act, Strategy, Policy. Programme, Law etc
    """

    __tablename__ = "family_type"

    type_name = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)


class Variant(Base):
    """The type of variant of a document within a family.

    Variants are described in the family/collection/doc notion page.
    Examples: "original language", "official translation", "unofficial translation.
    """

    __tablename__ = "variant"
    id = sa.Column(sa.Integer, primary_key=True)
    variant = sa.Column(sa.Text, nullable=False, unique=True)
    description = sa.Column(sa.Text, nullable=False)


class Family(Base):

    __tablename__ = "family"

    id = sa.Column(sa.Text, primary_key=True)
    title = sa.Column(sa.Text, nullable=False)
    import_id = sa.Column(sa.Integer)
    description = sa.Column(sa.Text, nullable=False)
    geography_id = sa.Column(sa.ForeignKey(Geography.id), nullable=False)
    category_name = sa.Column(sa.ForeignKey(FamilyCategory.name), nullable=False)

    family_type = sa.Column(sa.ForeignKey(FamilyType.type_name), nullable=False)


class DocumentStatus(enum.Enum):
    CREATED = "Created"
    PUBLISHED = "Published"


class DocumentType(Base):
    """A document type.

    E.g. strategy, plan, law
    """

    __tablename__ = "document_type"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.Text, nullable=False, unique=True)
    description = sa.Column(sa.Text, nullable=False)


class FamilyDocument(Base):
    __tablename__ = "family_document"

    family_id = sa.Column(sa.ForeignKey(Family.id), nullable=False)
    physical_document_id = sa.Column(
        sa.ForeignKey(PhysicalDocument.id), nullable=False, primary_key=True
    )

    cdn_url = sa.Column(sa.Text, nullable=True)
    import_id = sa.Column(sa.Text, nullable=True)
    variant_id = sa.Column(sa.ForeignKey(Variant.id), nullable=False)
    document_status = sa.Column(sa.Enum(DocumentStatus), default=DocumentStatus.CREATED)
    document_type_id = sa.Column(sa.ForeignKey(DocumentType.id), nullable=False)


class FamilyOrganisation(Base):

    __tablename__ = "family_organisation"

    family_id = sa.Column(sa.ForeignKey(Family.id), nullable=False)
    organisation_id = sa.Column(sa.ForeignKey(Organisation.id), nullable=False)

    sa.PrimaryKeyConstraint(family_id, organisation_id)
