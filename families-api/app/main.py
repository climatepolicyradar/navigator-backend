from typing import Generic, Optional, TypeVar

from fastapi import APIRouter, Depends, FastAPI
from pydantic_settings import BaseSettings
from sqlalchemy import create_engine
from sqlmodel import Field, Relationship, Session, SQLModel, func, select


class FamilyCorpusLink(SQLModel, table=True):
    __tablename__ = "family_corpus"  # type: ignore[assignment]
    corpus_import_id: int = Field(foreign_key="corpus.import_id", primary_key=True)
    family_import_id: int = Field(foreign_key="family.import_id", primary_key=True)


class Corpus(SQLModel, table=True):
    __tablename__ = "corpus"  # type: ignore[assignment]
    import_id: str = Field(primary_key=True)
    title: str
    families: list["Family"] = Relationship(
        back_populates="corpus", link_model=FamilyCorpusLink
    )


class FamilyGeographyLink(SQLModel, table=True):
    __tablename__ = "family_geography"  # type: ignore[assignment]
    geography_id: int = Field(foreign_key="geography.id", primary_key=True)
    family_import_id: int = Field(foreign_key="family.import_id", primary_key=True)


class GeographyBase(SQLModel):
    id: int = Field(primary_key=True)
    display_value: str
    value: str
    type: str
    slug: str


class Geography(GeographyBase, table=True):
    __tablename__ = "geography"  # type: ignore[assignment]
    value: str
    parent_id: int = Field(foreign_key="geography.id")
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
        back_populates="geographies", link_model=FamilyGeographyLink
    )


class FamilyBase(SQLModel):
    import_id: str = Field(primary_key=True)
    description: str | None


class Family(FamilyBase, table=True):
    __tablename__ = "family"  # type: ignore[assignment]
    geographies: list[Geography] = Relationship(
        back_populates="families", link_model=FamilyGeographyLink
    )
    corpus: Corpus = Relationship(
        back_populates="families", link_model=FamilyCorpusLink
    )
    family_documents: list["FamilyDocument"] = Relationship(back_populates="family")


class FamilyPublic(FamilyBase):
    corpus: Corpus
    geographies: list[Geography]


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
    family: FamilyPublic


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


class APIResponse(SQLModel, Generic[APIDataType]):
    data: list[APIDataType]
    total: int
    page: int
    page_size: int


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


def get_navigator_session():
    with Session(navigator_engine) as session:
        yield session


@router.get("/", response_model=APIResponse[PhysicalDocumentPublic])
def read_documents(*, session: Session = Depends(get_navigator_session)):
    documents = session.exec(
        select(PhysicalDocument)
        .where(
            PhysicalDocument.cdn_object.is_not(None),
        )
        .limit(10)
    ).all()

    data = [PhysicalDocumentPublic.model_validate(doc) for doc in documents]

    return APIResponse(
        data=data,
        total=len(data),
        page=1,
        page_size=len(data),
    )


class GeographyDocumentCount(SQLModel):
    alpha3: str
    name: str
    count: int


@router.get(
    "/aggregations/by-geography",
    response_model=APIResponse[GeographyDocumentCount],
)
def docs_by_geo(
    session: Session = Depends(get_navigator_session),
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

    return APIResponse(
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
    }


app.include_router(router)
