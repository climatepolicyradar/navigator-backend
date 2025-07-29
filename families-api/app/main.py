import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generic, Optional, TypeVar

from api import log
from api.telemetry import Telemetry
from api.telemetry_config import ServiceManifest, TelemetryConfig
from fastapi import APIRouter, Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, computed_field
from pydantic_settings import BaseSettings
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlmodel import Column, Field, Relationship, Session, SQLModel, func, select

_LOGGER = logging.getLogger(__name__)

# we always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent


# Open Telemetry initialisation
ENV = os.getenv("ENV", "development")
os.environ["OTEL_PYTHON_LOG_CORRELATION"] = "True"
try:
    otel_config = TelemetryConfig.from_service_manifest(
        ServiceManifest.from_file(f"{root_dir}/service-manifest.json"), ENV, "0.1.0"
    )
except Exception as _:
    _LOGGER.error("Failed to load service manifest, using defaults")
    otel_config = TelemetryConfig(
        service_name="navigator-backend",
        namespace_name="navigator",
        service_version="0.0.0",
        environment=ENV,
    )

telemetry = Telemetry(otel_config)
tracer = telemetry.get_tracer()


class Organisation(SQLModel, table=True):
    __tablename__ = "organisation"  # type: ignore[assignment]
    id: int = Field(primary_key=True)
    name: str
    corpora: list["Corpus"] = Relationship(back_populates="organisation")


class FamilyCorpusLink(SQLModel, table=True):
    __tablename__ = "family_corpus"  # type: ignore[assignment]
    corpus_import_id: str = Field(foreign_key="corpus.import_id", primary_key=True)
    family_import_id: str = Field(foreign_key="family.import_id", primary_key=True)


class Corpus(SQLModel, table=True):
    __tablename__ = "corpus"  # type: ignore[assignment]
    import_id: str = Field(primary_key=True)
    title: str
    families: list["Family"] = Relationship(
        back_populates="corpus", link_model=FamilyCorpusLink
    )
    organisation: Organisation = Relationship(back_populates="corpora")
    organisation_id: int = Field(foreign_key="organisation.id")
    corpus_type_name: str


class Slug(SQLModel, table=True):
    __tablename__ = "slug"  # type: ignore[assignment]
    name: str = Field(primary_key=True, index=True, unique=True)
    family_import_id: str | None = Field(
        index=True, foreign_key="family.import_id", nullable=True
    )
    family_document_import_id: str | None = Field(
        index=True, unique=True, foreign_key="family_document.import_id", nullable=True
    )
    created: datetime = Field(default_factory=datetime.now)


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


class CollectionFamilyLink(SQLModel, table=True):
    __tablename__ = "collection_family"  # type: ignore[assignment]
    collection_import_id: str = Field(
        foreign_key="collection.import_id", primary_key=True
    )
    family_import_id: str = Field(foreign_key="family.import_id", primary_key=True)


class Collection(SQLModel, table=True):
    __tablename__ = "collection"  # type: ignore[assignment]
    import_id: str = Field(primary_key=True)
    title: str
    description: str
    created: datetime = Field(default_factory=datetime.now)
    last_modified: datetime = Field(default_factory=datetime.now)
    valid_metadata: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSONB)
    )
    families: list["Family"] = Relationship(
        back_populates="unparsed_collections", link_model=CollectionFamilyLink
    )


class FamilyEvent(SQLModel, table=True):
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


class FamilyMetadata(SQLModel, table=True):
    __tablename__ = "family_metadata"  # type: ignore[assignment]
    family_import_id: str = Field(foreign_key="family.import_id", primary_key=True)
    value: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSONB))


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
    unparsed_geographies: list[Geography] = Relationship(
        back_populates="families", link_model=FamilyGeographyLink
    )
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
        sa_relationship_kwargs={"order_by": lambda: Slug.created.desc()}
    )
    unparsed_metadata: Optional[FamilyMetadata] = Relationship()
    unparsed_events: list[FamilyEvent] = Relationship(back_populates="family")
    unparsed_collections: list[Collection] = Relationship(
        back_populates="families", link_model=CollectionFamilyLink
    )


class FamilyPublic(FamilyBase):
    import_id: str
    corpus: Corpus = Field()
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
    def collections(self) -> list[dict[str, Any]]:
        return [
            {
                "import_id": collection.import_id,
                "title": collection.title,
                "description": collection.description,
                "families": [
                    {
                        "description": family.description,
                        "slug": (
                            family.unparsed_slug[0].name
                            if len(family.unparsed_slug) > 0
                            else ""
                        ),
                        "title": family.title,
                    }
                    for family in collection.families
                ],
            }
            for collection in self.unparsed_collections
        ]

    @computed_field
    @property
    def events(self) -> list[dict[str, Any]]:
        return [
            {
                "title": event.title,
                "date": event.date,
                "event_type": event.event_type_name,
                "status": event.status,
                "metadata": event.valid_metadata,
            }
            for event in self.unparsed_events
        ]

    @computed_field
    @property
    def documents(self) -> list[dict[str, Any]]:
        return [
            {
                "import_id": document.import_id,
                "variant": document.variant_name,
                "slug": (
                    document.unparsed_slug[0].name
                    if len(document.unparsed_slug) > 0
                    else ""
                ),
                "title": document.physical_document.title,
                "md5_sum": document.physical_document.md5_sum,
                "cdn_object": f"{settings.cdn_url}/{document.physical_document.cdn_object}",
                "source_url": document.physical_document.source_url,
                "content_type": document.physical_document.content_type,
                "language": (
                    document.physical_document.unparsed_languages[0].language_code
                    if document.physical_document.unparsed_languages
                    else None
                ),
                "languages": [
                    language.language_code
                    for language in document.physical_document.unparsed_languages
                ],
                "document_type": (
                    document.valid_metadata.get("type", [None])[0]
                    if document.valid_metadata
                    else None
                ),
                "document_role": (
                    document.valid_metadata.get("role", [None])[0]
                    if document.valid_metadata
                    else None
                ),
                "events": [
                    {
                        "title": event.title,
                        "date": event.date,
                        "event_type": event.event_type_name,
                        "status": event.status,
                        "metadata": event.valid_metadata,
                    }
                    for event in document.unparsed_events
                ],
            }
            for document in self.family_documents
            if document.physical_document
        ]

    # metadata is reserved in SQLModel
    @computed_field(alias="metadata")
    @property
    def _metadata(self) -> dict[str, Any]:
        return self.unparsed_metadata.value if self.unparsed_metadata else {}


class FamilyDocumentBase(SQLModel):
    import_id: str = Field(primary_key=True)
    variant_name: str | None


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
    family: "FamilyPublic"


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


APIDataType = TypeVar("APIDataType")


class APIListResponse(BaseModel, Generic[APIDataType]):
    data: list[APIDataType]
    total: int
    page: int
    page_size: int


class APIItemResponse(BaseModel, Generic[APIDataType]):
    data: APIDataType


class Settings(BaseSettings):
    navigator_database_url: str
    cdn_url: str
    # @related: GITHUB_SHA_ENV_VAR
    github_sha: str = "unknown"


settings = Settings()
log.log("families-api")

router = APIRouter(
    prefix="/families",
)
app = FastAPI(
    docs_url="/families/docs",
    redoc_url="/families/redoc",
    openapi_url="/families/openapi.json",
)


_ALLOW_ORIGIN_REGEX = (
    r"http://localhost:3000|"
    r"http://bs-local.com:3000|"
    r"https://.+\.climatepolicyradar\.org|"
    r"https://.+\.staging.climatepolicyradar\.org|"
    r"https://.+\.production.climatepolicyradar\.org|"
    r"https://.+\.sandbox\.climatepolicyradar\.org|"
    r"https://climate-laws\.org|"
    r"https://.+\.climate-laws\.org|"
    r"https://climateprojectexplorer\.org|"
    r"https://.+\.climateprojectexplorer\.org"
)

# Add CORS middleware to allow cross origin requests from any port
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=_ALLOW_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


navigator_engine = create_engine(settings.navigator_database_url)


def get_session():
    with Session(navigator_engine) as session:
        yield session


@router.get("/", response_model=APIListResponse[PhysicalDocumentPublic])
def read_documents(*, session: Session = Depends(get_session)):
    documents = session.exec(
        select(PhysicalDocument)
        .where(
            PhysicalDocument.cdn_object.is_not(None),
        )
        .limit(10)
    ).all()

    data = [PhysicalDocumentPublic.model_validate(doc) for doc in documents]

    return APIListResponse(
        data=data,
        total=len(data),
        page=1,
        page_size=len(data),
    )


class ConceptPublic(BaseModel):
    id: str
    relation: str
    preferred_label: str
    type: str
    ids: list[str]
    subconcept_of_labels: list[str]


@router.get("/concepts")
def read_concepts(*, session: Session = Depends(get_session)):
    # Extract fields from the unnested JSONB objects
    stmt = text(
        """
      SELECT DISTINCT ON (concept->>'relation', concept->>'preferred_label')
          concept->>'relation' as relation,
          concept->>'preferred_label' as preferred_label,
          concept->>'id' as id,
          concept->>'ids' as ids,
          concept->>'type' as type,
          concept->>'subconcept_of_labels' as subconcept_of_labels
      FROM family, unnest(concepts) as concept
      WHERE concept->>'relation' IS NOT NULL 
      AND concept->>'preferred_label' IS NOT NULL
      ORDER BY concept->>'relation', concept->>'preferred_label'
    """
    )

    results = session.connection().execute(stmt).all()

    unique_concepts = [
        ConceptPublic.model_validate(
            {
                **row._asdict(),
                # This is needed to unpack the JSON arrays into Python lists
                "ids": json.loads(row.ids) if row.ids else [],
                "subconcept_of_labels": (
                    json.loads(row.subconcept_of_labels)
                    if row.subconcept_of_labels
                    else []
                ),
            }
        )
        for row in results
    ]

    return APIListResponse(
        data=unique_concepts,
        total=len(unique_concepts),
        page=1,
        page_size=len(unique_concepts),
    )


@router.get("/{family_id}", response_model=APIItemResponse[FamilyPublic])
def read_family(*, session: Session = Depends(get_session), family_id: str):
    # When should this break?
    # https://sqlmodel.tiangolo.com/tutorial/fastapi/read-one/#path-operation-for-one-hero
    family = session.exec(
        select(Family).where(Family.import_id == family_id)
    ).one_or_none()

    if family is None:
        raise HTTPException(status_code=404, detail="Not found")

    data = FamilyPublic.model_validate(family, from_attributes=True)

    return APIItemResponse(
        data=data,
    )


class GeographyDocumentCount(SQLModel):
    code: str
    name: str
    type: str
    count: int


@router.get(
    "/aggregations/by-geography",
    response_model=APIListResponse[GeographyDocumentCount],
)
def docs_by_geo(
    session: Session = Depends(get_session),
):
    stmt = (
        select(
            Geography.value.label("code"),
            Geography.display_value.label("name"),
            Geography.type,
            func.count(PhysicalDocument.id).label("count"),
        )
        .join(FamilyGeographyLink, Geography.id == FamilyGeographyLink.geography_id)
        .join(Family, FamilyGeographyLink.family_import_id == Family.import_id)
        .join(FamilyDocument, Family.import_id == FamilyDocument.family_import_id)
        .join(
            PhysicalDocument, FamilyDocument.physical_document_id == PhysicalDocument.id
        )
        .group_by(Geography.id)
        .order_by(func.count(PhysicalDocument.id).desc())
    )

    results = session.exec(stmt).all()

    data = [GeographyDocumentCount.model_validate(row._mapping) for row in results]

    return APIListResponse(
        data=data,
        total=len(data),
        page=1,
        page_size=len(data),
    )


# we use both to make sure we can have /families/health available publically
# and /health available to the internal network / AppRunner healthcheck
@app.get("/health")
@router.get("/health")
def health_check():
    return {
        "status": "ok",
        # @related: GITHUB_SHA_ENV_VAR
        "version": settings.github_sha,
    }


app.include_router(router)

# Open Telemetry instrumentation
telemetry.instrument_fastapi(app)
telemetry.setup_exception_hook()
