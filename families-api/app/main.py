from datetime import datetime
from typing import Any, Generic, Optional, TypeVar

from fastapi import APIRouter, Depends, FastAPI, HTTPException
from pydantic import BaseModel, computed_field
from pydantic_settings import BaseSettings
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlmodel import Column, Field, Relationship, Session, SQLModel, func, select


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


class FamilyMetadata(SQLModel, table=True):
    __tablename__ = "family_metadata"  # type: ignore[assignment]
    family_import_id: str = Field(foreign_key="family.import_id", primary_key=True)
    value: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSONB))


class FamilyBase(SQLModel):
    import_id: str = Field(primary_key=True)
    title: str
    description: str
    concepts: list[dict[str, Any]]
    last_modified: datetime = Field(default_factory=datetime.now)
    created: datetime = Field(default_factory=datetime.now)
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
    unparsed_slug: list[Slug] = Relationship()
    unparsed_metadata: Optional[FamilyMetadata] = Relationship()


class FamilyPublic(FamilyBase):
    import_id: str
    corpus: Corpus
    unparsed_geographies: list[Geography] = Field(default_factory=list, exclude=True)
    unparsed_slug: list[Slug] = Field(exclude=True, default=list())
    unparsed_metadata: Optional[FamilyMetadata] = Field(exclude=True, default=None)
    family_category: str = Field(exclude=True, default="")
    description: str = Field(exclude=True, default="")

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
    def published_date(self) -> datetime:
        return self.created

    @computed_field
    @property
    def last_updated_date(self) -> datetime:
        return self.last_modified

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

    # emtadata is reserved in SQLModel
    @computed_field(alias="metadata")
    @property
    def _metadata(self) -> dict[str, Any]:
        print(self.unparsed_metadata)
        return self.unparsed_metadata.value if self.unparsed_metadata else {}


# TODO: implement these models for the frontend
# export type TFamilyPage = {
#   organisation: string; // Done
#   title: string; // Done
#   summary: string; // Done
#   geographies: string[]; // Done
#   import_id: string; // Done
#   slug: string; // Done
#   corpus_id: string; // Done
#   published_date: string | null; // Done
#   last_updated_date: string | null; // Done
#   category: TCategory; // Done
#   corpus_type_name: TCorpusTypeSubCategory; // Done
#   metadata: TFamilyMetadata; // Done
#   events: TEvent[];
#   documents: TDocumentPage[];
#   collections: TCollection[];
# };

# export type TDocumentPage = {
#   import_id: string;
#   variant?: string | null;
#   slug: string;
#   title: string;
#   md5_sum?: string | null;
#   cdn_object?: string | null;
#   source_url: string;
#   content_type: TDocumentContentType;
#   language: string;
#   languages: string[];
#   document_type: string | null;
#   document_role: string;
# };

# export type TCollection = {
#   import_id: string;
#   title: string;
#   description: string;
#   families: TCollectionFamily[];
# };

# export type TCollectionFamily = {
#   description: string;
#   slug: string;
#   title: string;
# };

# export type TFamilyMetadata = {
#   topic?: string[];
#   hazard?: string[];
#   sector?: string[];
#   keyword?: string[];
#   framework?: string[];
#   instrument?: string[];
#   author_type?: string[];
#   author?: string[];
#   document_type?: string;
# };

# export type TCategory = "Legislative" | "Executive" | "Litigation" | "Policy" | "Law" | "UNFCCC" | "MCF" | "Reports";


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


class FamilyDocumentPublic(FamilyDocumentBase):
    family: "FamilyPublic"


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
    # @related: GITHUB_SHA_ENV_VAR
    github_sha: str = "unknown"


settings = Settings()

# TODO: Use JSON logging - https://linear.app/climate-policy-radar/issue/APP-571/add-json-logging-to-families-api
# TODO: Add OTel - https://linear.app/climate-policy-radar/issue/APP-572/add-otel-to-families-api
router = APIRouter(
    prefix="/families",
)
app = FastAPI(
    docs_url="/families/docs",
    redoc_url="/families/redoc",
    openapi_url="/families/openapi.json",
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
    alpha3: str
    name: str
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
            Geography.value.label("alpha3"),
            Geography.display_value.label("name"),
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
