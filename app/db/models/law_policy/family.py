import enum
from datetime import datetime
from typing import Optional, cast

import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.db.models.app import Organisation
from app.db.models.document import PhysicalDocument
from app.db.session import Base
from .geography import Geography


class _BaseModelEnum(str, enum.Enum):
    """Family categories as understood in the context of law/policy."""

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str) and not str.istitle(value):
            return cls(value.title())
        raise ValueError(f"{value} is not a valid {cls.__name__}")

    def __str__(self):
        return self._value_


class FamilyCategory(_BaseModelEnum):
    """Family categories as understood in the context of law/policy."""

    EXECUTIVE = "Executive"
    LEGISLATIVE = "Legislative"


class Variant(Base):
    """
    The type of variant of a document within a family.

    Variants are described in the family/collection/doc notion page.
    Examples: "original language", "official translation", "unofficial translation.
    """

    __tablename__ = "variant"
    variant_name = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)


class FamilyStatus(_BaseModelEnum):
    """Family status to control visibility in the app."""

    CREATED = "Created"
    PUBLISHED = "Published"
    DELETED = "Deleted"


class Family(Base):
    """A representation of a group of documents that represent a single law/policy."""

    __tablename__ = "family"

    title = sa.Column(sa.Text, nullable=False)
    import_id = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)
    geography_id = sa.Column(sa.ForeignKey(Geography.id), nullable=False)
    family_category = sa.Column(sa.Enum(FamilyCategory), nullable=False)
    family_status = sa.Column(
        sa.Enum(FamilyStatus),
        default=FamilyStatus.CREATED,
        nullable=False,
    )

    slugs: list["Slug"] = relationship("Slug", lazy="joined")
    events: list["FamilyEvent"] = relationship("FamilyEvent", lazy="joined")

    @hybrid_property
    def published_date(self) -> Optional[datetime]:
        """A date to use for filtering by published date."""
        if not self.events:
            return None
        date = None
        for event in self.events:
            if event.event_type_name == "Approved/Published":
                return cast(datetime, event.date)
            if date is None:
                date = cast(datetime, event.date)
            else:
                date = min(cast(datetime, event.date), date)
        return date

    @hybrid_property
    def last_updated_date(self) -> Optional[datetime]:
        """A "last updated" date to use during display of Family."""
        if not self.events:
            return None
        date = None
        for event in self.events:
            if date is None:
                date = cast(datetime, event.date)
            else:
                date = max(cast(datetime, event.date), date)
        return date


class DocumentStatus(_BaseModelEnum):
    """FamilyDocument status to control visibility in the app."""

    CREATED = "Created"
    PUBLISHED = "Published"
    DELETED = "Deleted"


class FamilyDocumentRole(Base):
    """
    A document type.

    E.g. Main, Press Release, Amendment etc
    """

    __tablename__ = "family_document_role"
    name = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)


class FamilyDocumentType(Base):
    """
    A document type.

    E.g. strategy, plan, law
    """

    __tablename__ = "family_document_type"
    name = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)


class FamilyDocument(Base):
    """A link between a Family and a PhysicalDocument."""

    __tablename__ = "family_document"

    family_import_id = sa.Column(sa.ForeignKey(Family.import_id), nullable=False)
    physical_document_id = sa.Column(
        sa.ForeignKey(PhysicalDocument.id),
        nullable=False,
        unique=True,
    )

    import_id = sa.Column(sa.Text, primary_key=True)
    variant_name = sa.Column(sa.ForeignKey(Variant.variant_name), nullable=False)
    document_status = sa.Column(
        sa.Enum(DocumentStatus),
        default=DocumentStatus.CREATED,
        nullable=False,
    )
    document_type = sa.Column(sa.ForeignKey(FamilyDocumentType.name), nullable=True)
    document_role = sa.Column(sa.ForeignKey(FamilyDocumentRole.name), nullable=True)

    slugs: list["Slug"] = relationship("Slug", lazy="joined")
    physical_document: Optional[PhysicalDocument] = relationship(
        PhysicalDocument,
        lazy="joined",
    )


class FamilyOrganisation(Base):
    """A link between a Family and its owning Organisation."""

    __tablename__ = "family_organisation"

    family_import_id = sa.Column(sa.ForeignKey(Family.import_id), nullable=False)
    organisation_id = sa.Column(sa.ForeignKey(Organisation.id), nullable=False)

    sa.PrimaryKeyConstraint(family_import_id)


class Slug(Base):
    """An identifier for a Family of FamilyDocument to be used in URLs."""

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


class EventStatus(_BaseModelEnum):
    """Event status to flag data issues from import."""

    OK = "Ok"
    # Duplicate means a single event was applied to multiple families. In this
    # case we will need to validate, remove unecessary duplicates & create new
    # events through a data cleaning exercise.
    DUPLICATED = "Duplicated"


class FamilyEventType(Base):
    """Defines the types that can be associated with a law/policy event."""

    __tablename__ = "family_event_type"
    name = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)


class FamilyEvent(Base):
    """An event associated with a Family timeline with optional link to a document."""

    __tablename__ = "family_event"

    import_id = sa.Column(sa.Text, primary_key=True)
    title = sa.Column(sa.Text, nullable=False)
    date = sa.Column(sa.DateTime(timezone=True), nullable=False)
    event_type_name = sa.Column(sa.ForeignKey(FamilyEventType.name), nullable=False)
    family_import_id = sa.Column(sa.ForeignKey(Family.import_id), nullable=False)
    family_document_import_id = sa.Column(
        sa.ForeignKey(FamilyDocument.import_id),
        default=None,
        nullable=True,
    )
    status = sa.Column(sa.Enum(EventStatus), nullable=False)
