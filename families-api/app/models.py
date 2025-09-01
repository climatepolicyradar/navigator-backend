import logging
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from pydantic import computed_field
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlmodel import Column, Field, Relationship, SQLModel

from app.settings import settings

_LOGGER = logging.getLogger(__name__)

# we always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent


# region: Organisation
class Organisation(SQLModel, table=True):
    __tablename__ = "organisation"  # type: ignore[assignment]
    id: int = Field(primary_key=True)
    name: str
    attribution_url: str | None = None
    corpora: list["Corpus"] = Relationship(back_populates="organisation")


# endregion


# region: Corpus
class FamilyCorpusLink(SQLModel, table=True):
    __tablename__ = "family_corpus"  # type: ignore[assignment]
    corpus_import_id: str = Field(foreign_key="corpus.import_id", primary_key=True)
    family_import_id: str = Field(foreign_key="family.import_id", primary_key=True)


class CorpusBase(SQLModel):
    import_id: str
    title: str
    corpus_type_name: str


class Corpus(CorpusBase, table=True):
    __tablename__ = "corpus"  # type: ignore[assignment]
    import_id: str = Field(primary_key=True)
    families: list["Family"] = Relationship(
        back_populates="corpus", link_model=FamilyCorpusLink
    )
    organisation: Organisation = Relationship(back_populates="corpora")
    organisation_id: int = Field(foreign_key="organisation.id")


class CorpusPublic(CorpusBase):
    organisation: Organisation


# endregion


# region: Slug
class Slug(SQLModel, table=True):
    __tablename__ = "slug"  # type: ignore[assignment]
    name: str = Field(primary_key=True, index=True, unique=True)
    family_import_id: str | None = Field(
        index=True, foreign_key="family.import_id", nullable=True
    )
    family_document_import_id: str | None = Field(
        index=True, unique=True, foreign_key="family_document.import_id", nullable=True
    )
    collection_import_id: str | None = Field(
        index=True, unique=True, foreign_key="collection.import_id", nullable=True
    )
    created: datetime = Field(default_factory=datetime.now)


# endregion


# region: Geography
class FamilyGeographyLink(SQLModel, table=True):
    __tablename__ = "family_geography"  # type: ignore[assignment]
    geography_id: int = Field(foreign_key="geography.id", primary_key=True)
    family_import_id: str = Field(foreign_key="family.import_id", primary_key=True)


class GeographyBase(SQLModel):
    id: int = Field(primary_key=True)
    display_value: str
    value: str
    type: str
    slug: str


class Geography(GeographyBase, table=True):
    __tablename__ = "geography"  # type: ignore[assignment]
    value: str
    parent_id: int | None = Field(
        foreign_key="geography.id", nullable=True, default=None
    )
    """
    the relationship stuff here is a little non-standard and inherited from the
    previous implementation
    if we were to do this again we'd follow the standard SQLModel approach
    @see https://sqlmodel.tiangolo.com/tutorial/many-to-many/create-models-with-link/#link-table-model
    """
    parent: Optional["Geography"] = Relationship(
        back_populates="children",
        sa_relationship_kwargs={"remote_side": "[Geography.id]"},
    )
    children: list["Geography"] = Relationship(back_populates="parent")
    families: list["Family"] = Relationship(
        back_populates="unparsed_geographies", link_model=FamilyGeographyLink
    )


# endregion


# region: Collection
class CollectionFamilyLink(SQLModel, table=True):
    __tablename__ = "collection_family"  # type: ignore[assignment]
    collection_import_id: str = Field(
        foreign_key="collection.import_id", primary_key=True
    )
    family_import_id: str = Field(foreign_key="family.import_id", primary_key=True)


class CollectionBase(SQLModel):
    import_id: str
    title: str
    description: str
    valid_metadata: dict[str, Any]


class Collection(CollectionBase, table=True):
    __tablename__ = "collection"  # type: ignore[assignment]
    import_id: str = Field(primary_key=True)

    created: datetime = Field(default_factory=datetime.now)
    last_modified: datetime = Field(default_factory=datetime.now)
    valid_metadata: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSONB)
    )
    families: list["Family"] = Relationship(
        back_populates="unparsed_collections", link_model=CollectionFamilyLink
    )
    unparsed_slug: list["Slug"] = Relationship(
        sa_relationship_kwargs={"order_by": lambda: Slug.created.desc()}  # type: ignore
    )


class CollectionPublic(CollectionBase):
    valid_metadata: dict[str, Any] = Field(exclude=True)
    unparsed_slug: list[Slug] = Field(exclude=True)

    @computed_field(alias="metadata")
    @property
    def _metadata(self) -> dict[str, Any]:
        return self.valid_metadata

    @computed_field
    @property
    def slug(self) -> str:
        return self.unparsed_slug[0].name if len(self.unparsed_slug) > 0 else ""


class CollectionPublicWithFamilies(CollectionPublic):
    families: list["FamilyPublic"]


# endregion


# region: FamilyEvent
class FamilyEventBase(SQLModel):
    import_id: str


class FamilyEvent(FamilyEventBase, table=True):
    __tablename__ = "family_event"  # type: ignore[assignment]
    import_id: str = Field(primary_key=True)
    title: str
    date: datetime
    event_type_name: str
    family_import_id: str | None = Field(
        foreign_key="family.import_id", nullable=True, default=None
    )
    family: Optional["Family"] = Relationship(back_populates="unparsed_events")
    family_document_import_id: str | None = Field(
        foreign_key="family_document.import_id", nullable=True, default=None
    )
    family_document: Optional["FamilyDocument"] = Relationship(
        back_populates="unparsed_events",
    )
    status: str
    created: datetime = Field(default_factory=datetime.now)
    last_modified: datetime = Field(default_factory=datetime.now)
    valid_metadata: dict[str, Any] | None = Field(
        default_factory=None, sa_column=Column(JSONB)
    )


class FamilyEventPublic(FamilyEventBase):
    import_id: str
    title: str
    date: datetime
    event_type: str
    status: str
    unparsed_metadata: dict[str, Any] | None = Field(exclude=True, default=None)

    # metadata is reserved in SQLModel
    @computed_field(alias="metadata")
    @property
    def _metadata(self) -> dict[str, Any] | None:
        return self.unparsed_metadata


# endregion


# region: FamilyMetadata
class FamilyMetadata(SQLModel, table=True):
    __tablename__ = "family_metadata"  # type: ignore[assignment]
    family_import_id: str = Field(foreign_key="family.import_id", primary_key=True)
    value: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSONB))


# endregion


# region: Family
class FamilyBase(SQLModel):
    import_id: str = Field(primary_key=True)
    title: str
    description: str
    concepts: list[dict[str, Any]]
    last_modified: datetime = Field(default_factory=datetime.now, exclude=True)
    created: datetime = Field(default_factory=datetime.now, exclude=True)
    family_category: str


class Family(FamilyBase, table=True):
    __tablename__ = "family"  # type: ignore[assignment]
    corpus: Corpus = Relationship(
        back_populates="families", link_model=FamilyCorpusLink
    )
    family_documents: list["FamilyDocument"] = Relationship(back_populates="family")
    concepts: list[dict[str, Any]] = Field(
        default_factory=list, sa_column=Column(ARRAY(JSONB))
    )
    unparsed_geographies: list[Geography] = Relationship(
        back_populates="families", link_model=FamilyGeographyLink
    )
    unparsed_slug: list[Slug] = Relationship(
        sa_relationship_kwargs={"order_by": lambda: Slug.created.desc()}  # type: ignore
    )
    unparsed_metadata: Optional[FamilyMetadata] = Relationship()
    unparsed_events: list[FamilyEvent] = Relationship(back_populates="family")
    unparsed_collections: list[Collection] = Relationship(
        back_populates="families", link_model=CollectionFamilyLink
    )


class FamilyPublic(FamilyBase):
    import_id: str
    corpus: CorpusPublic = Field()
    unparsed_geographies: list[Geography] = Field(default_factory=list, exclude=True)
    unparsed_slug: list[Slug] = Field(exclude=True, default=list())
    unparsed_metadata: Optional[FamilyMetadata] = Field(exclude=True, default=None)
    unparsed_events: list[FamilyEvent] = Field(exclude=True, default=list())
    unparsed_collections: list[Collection] = Field(exclude=True, default=list())
    family_category: str = Field(exclude=True, default="")
    description: str = Field(exclude=True, default="")
    family_documents: list["FamilyDocument"] = Field(exclude=True, default=list())

    @computed_field
    @property
    def corpus_id(self) -> str:
        return self.corpus.import_id

    @computed_field
    @property
    def organisation(self) -> str:
        return self.corpus.organisation.name

    @computed_field
    @property
    def organisation_attribution_url(self) -> str | None:
        return self.corpus.organisation.attribution_url

    @computed_field
    @property
    def summary(self) -> str:
        return self.description

    @computed_field
    @property
    def geographies(self) -> list[str]:
        return [g.value for g in self.unparsed_geographies]

    @computed_field
    @property
    def published_date(self) -> datetime | None:
        # datetime_event_name stores the value of the event.event_type_name that should be used for published_date
        # otherwise we use the earliest date
        published_event_date = next(
            (
                event.date
                for event in self.unparsed_events
                if event.valid_metadata is not None
                and event.event_type_name == event.valid_metadata["datetime_event_name"]
            ),
            None,
        )
        earliest_event_date = min(
            (event.date for event in self.unparsed_events if event.date), default=None
        )
        return published_event_date or earliest_event_date

    @computed_field
    @property
    def last_updated_date(self) -> datetime | None:
        # get the most recent date that is not in the future
        now = datetime.now(tz=timezone.utc)
        latest_event_date = max(
            (
                event.date
                for event in self.unparsed_events
                if event.date and event.date <= now
            ),
            default=None,
        )
        return latest_event_date

    @computed_field
    @property
    def slug(self) -> str:
        return self.unparsed_slug[0].name if len(self.unparsed_slug) > 0 else ""

    @computed_field
    @property
    def category(self) -> str:
        return self.family_category

    @computed_field
    @property
    def corpus_type_name(self) -> str:
        return self.corpus.corpus_type_name

    @computed_field
    @property
    def collections(self) -> list[CollectionPublic]:
        return [
            CollectionPublic.model_validate(collection)
            for collection in self.unparsed_collections
        ]

    @computed_field
    @property
    def events(self) -> list[FamilyEventPublic]:
        return [
            FamilyEventPublic(
                import_id=event.import_id,
                title=event.title,
                date=event.date,
                event_type=event.event_type_name,
                status=event.status,
                unparsed_metadata=event.valid_metadata,
            )
            for event in self.unparsed_events
        ]

    @computed_field
    @property
    def documents(self) -> list["FamilyDocumentPublic"]:
        return [
            FamilyDocumentPublic.model_validate(family_document)
            for family_document in self.family_documents
            if family_document.physical_document
        ]

    # metadata is reserved in SQLModel
    @computed_field(alias="metadata")
    @property
    def _metadata(self) -> dict[str, Any]:
        return self.unparsed_metadata.value if self.unparsed_metadata else {}


# endregion


# region: FamilyDocument & PhysicalDocument
class FamilyDocumentStatus(Enum):
    CREATED = "created"
    PUBLISHED = "published"
    DELETED = "deleted"


class FamilyDocumentBase(SQLModel):
    import_id: str = Field(primary_key=True)
    variant_name: str | None
    document_status: FamilyDocumentStatus


class FamilyDocument(FamilyDocumentBase, table=True):
    __tablename__ = "family_document"  # type: ignore[assignment]
    family_import_id: str = Field(foreign_key="family.import_id")
    family: Family = Relationship(back_populates="family_documents")
    physical_document_id: int = Field(foreign_key="physical_document.id", unique=True)
    physical_document: Optional["PhysicalDocument"] = Relationship(
        back_populates="family_document"
    )
    unparsed_events: list[FamilyEvent] = Relationship(back_populates="family_document")
    valid_metadata: dict[str, Any] | None = Field(
        default_factory=None, sa_column=Column(JSONB)
    )
    unparsed_slug: list[Slug] = Relationship()


class FamilyDocumentPublic(FamilyDocumentBase):
    import_id: str
    valid_metadata: dict[str, Any]
    physical_document: "PhysicalDocument" = Field(exclude=True)
    unparsed_slug: list[Slug] = Field(exclude=True)
    unparsed_events: list[FamilyEvent] = Field(exclude=True)

    # events: list[FamilyEventPublic]

    @computed_field
    @property
    def slug(self) -> str:
        return self.unparsed_slug[0].name if len(self.unparsed_slug) > 0 else ""

    @computed_field
    @property
    def title(self) -> str:
        return self.physical_document.title

    @computed_field
    @property
    def cdn_object(self) -> str:
        return f"{settings.cdn_url}/navigator/{self.physical_document.cdn_object}"

    @computed_field
    @property
    def variant(self) -> str | None:
        return self.variant_name

    @computed_field
    @property
    def md5_sum(self) -> str | None:
        return self.physical_document.md5_sum

    @computed_field
    @property
    def source_url(self) -> str | None:
        return self.physical_document.source_url

    @computed_field
    @property
    def content_type(self) -> str | None:
        return self.physical_document.content_type

    @computed_field
    @property
    def language(self) -> str | None:
        return (
            self.physical_document.unparsed_languages[0].language_code
            if self.physical_document.unparsed_languages
            else None
        )

    @computed_field
    @property
    def languages(self) -> list[str]:
        return [
            language.language_code
            for language in self.physical_document.unparsed_languages
        ]

    @computed_field
    @property
    def document_type(self) -> str | None:
        return (
            self.valid_metadata.get("type", [None])[0] if self.valid_metadata else None
        )

    @computed_field
    @property
    def document_role(self) -> str:
        return self.valid_metadata.get("role", [""])[0] if self.valid_metadata else ""

    @computed_field
    @property
    def events(self) -> list[FamilyEventPublic]:
        return [
            FamilyEventPublic(
                import_id=event.import_id,
                title=event.title,
                date=event.date,
                event_type=event.event_type_name,
                status=event.status,
                unparsed_metadata=event.valid_metadata,
            )
            for event in self.unparsed_events
        ]


class FamilyDocumentPublicWithFamily(FamilyDocumentPublic):
    family: FamilyPublic


class PhysicalDDocumentLanguageLink(SQLModel, table=True):
    __tablename__ = "physical_document_language"  # type: ignore[assignment]
    language_id: int = Field(foreign_key="language.id", primary_key=True)
    document_id: int = Field(foreign_key="physical_document.id", primary_key=True)
    source: str
    visible: bool


class Language(SQLModel, table=True):
    id: int = Field(primary_key=True, index=True, unique=True)
    language_code: str
    part1_code: str | None
    part2_code: str | None
    name: str | None


class PhysicalDocumentBase(SQLModel):
    id: int = Field(primary_key=True)
    title: str = Field(index=True)
    md5_sum: str | None
    source_url: str | None
    content_type: str | None
    cdn_object: str | None


class PhysicalDocument(PhysicalDocumentBase, table=True):
    __tablename__ = "physical_document"  # type: ignore[assignment]
    family_document: FamilyDocument = Relationship(back_populates="physical_document")
    unparsed_languages: list[Language] = Relationship(
        link_model=PhysicalDDocumentLanguageLink
    )


class PhysicalDocumentPublic(PhysicalDocumentBase):
    family_document: FamilyDocumentPublic | None


# endregion
