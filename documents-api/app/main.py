from typing import Generic, Optional, TypeVar

from fastapi import Depends, FastAPI
from sqlalchemy import create_engine
from sqlmodel import Field, Relationship, Session, SQLModel, func, select


class RDSModel(SQLModel):
    model_config = {"json_schema_extra": {"exclude": True}}


class FamilyCorpusLink(RDSModel, table=True):
    __tablename__ = "family_corpus"
    corpus_import_id: int | None = Field(
        default=None, foreign_key="corpus.import_id", primary_key=True
    )
    family_import_id: int | None = Field(
        default=None, foreign_key="family.import_id", primary_key=True
    )


class Corpus(RDSModel, table=True):
    __tablename__ = "corpus"
    import_id: str | None = Field(default=None, primary_key=True)
    title: str
    families: list["Family"] = Relationship(
        back_populates="corpus", link_model=FamilyCorpusLink
    )


class FamilyGeographyLink(RDSModel, table=True):
    __tablename__ = "family_geography"
    geography_id: int | None = Field(
        default=None, foreign_key="geography.id", primary_key=True
    )
    family_import_id: int | None = Field(
        default=None, foreign_key="family.import_id", primary_key=True
    )


class GeographyBase(RDSModel):
    id: int | None = Field(default=None, primary_key=True)
    display_value: str
    value: str
    type: str
    slug: str


class Geography(GeographyBase, table=True):
    __tablename__ = "geography"
    parent_id: int = Field(foreign_key="geography.id")
    # the relationship stuff here is a little non-standard
    # there doesn't seem to be an easier way to do this with RDSModel
    parent: Optional["Geography"] = Relationship(
        back_populates="children",
        sa_relationship_kwargs={"remote_side": "[Geography.id]"},
    )
    children: list["Geography"] = Relationship(back_populates="parent")
    families: list["Family"] = Relationship(
        back_populates="geographies", link_model=FamilyGeographyLink
    )


class FamilyBase(RDSModel):
    import_id: str | None = Field(default=None, primary_key=True)
    description: str | None


class Family(FamilyBase, table=True):
    __tablename__ = "family"
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


class FamilyDocumentBase(RDSModel):
    import_id: str | None = Field(default=None, primary_key=True)
    variant_name: str | None


class FamilyDocument(FamilyDocumentBase, table=True):
    __tablename__ = "family_document"
    family_import_id: str | None = Field(default=None, foreign_key="family.import_id")
    family: Family = Relationship(back_populates="family_documents")
    physical_document_id: int = Field(foreign_key="physical_document.id", unique=True)
    physical_document: Optional["PhysicalDocument"] = Relationship(
        back_populates="family_document"
    )


class FamilyDocumentPublic(FamilyDocumentBase):
    family: FamilyPublic


class PhysicalDocumentBase(RDSModel):
    id: int | None = Field(default=None, primary_key=True)
    title: str = Field(index=True)
    md5_sum: str | None
    source_url: str | None
    content_type: str | None
    cdn_object: str | None


class PhysicalDocument(PhysicalDocumentBase, table=True):
    __tablename__ = "physical_document"
    family_document: FamilyDocument | None = Relationship(
        back_populates="physical_document"
    )


class PhysicalDocumentPublic(PhysicalDocumentBase):
    family_document: FamilyDocumentPublic | None


APIDataType = TypeVar("APIDataType")


class APIResponse(SQLModel, Generic[APIDataType]):
    data: list[APIDataType]
    total: int
    page: int
    page_size: int


app = FastAPI()

navigator_engine = create_engine(
    # This just matches the docker-compose.yml file
    str("postgresql://navigator:navigator@localhost/navigator")
)


def get_navigator_session():
    with Session(navigator_engine) as session:
        yield session


@app.get("/", response_model=APIResponse[PhysicalDocumentPublic])
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


@app.get(
    "/aggregations/documents-by-geography",
    response_model=APIResponse[GeographyDocumentCount],
)
def docs_by_geo(session: Session = Depends(get_navigator_session)):
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
        .group_by(Geography.id, Geography.slug)
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














