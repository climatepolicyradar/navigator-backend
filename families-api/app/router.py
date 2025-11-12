import json
import logging
from pathlib import Path
from typing import Generic, TypeVar

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlmodel import Session, SQLModel, desc, func, select

from app.database import get_session
from app.models import (
    Collection,
    CollectionPublicWithFamilies,
    Corpus,
    Family,
    FamilyCorpusLink,
    FamilyDocument,
    FamilyDocumentPublicWithFamily,
    FamilyDocumentStatus,
    FamilyGeographyLink,
    FamilyPublic,
    Geography,
    PhysicalDocument,
    Slug,
)

_LOGGER = logging.getLogger(__name__)

# we always use a path relative to the file as the calling process can come
# from multiple locations
root_dir = Path(__file__).parent.parent

router = APIRouter(prefix="/families")


APIDataType = TypeVar("APIDataType")


class APIListResponse(BaseModel, Generic[APIDataType]):
    data: list[APIDataType]
    total: int
    page: int
    page_size: int


class APIItemResponse(BaseModel, Generic[APIDataType]):
    data: APIDataType


@router.get("/", response_model=APIListResponse[FamilyPublic])
def read_families(
    *,
    session: Session = Depends(get_session),
    page: int = Query(1, ge=1),
    corpus_import_ids: list[str] = Query(
        default=[],
        alias="corpus.import_id",
    ),
):
    limit = 50
    offset = (page - 1) * limit

    filters = []
    if corpus_import_ids:
        # We're filtering `Families.corpus` to tell SQLModel to generate the right SQL for filtering.
        # Direct attribute access e.g. Corpus.import_id doesn't work because the ORM
        # doesn't auto-join related tables in filters.
        filters.append(Family.corpus.has(Corpus.import_id.in_(corpus_import_ids)))  # type: ignore

    families = session.exec(
        Family.eager_loaded_select()
        .order_by(desc(Family.last_modified))
        .offset(offset)
        .limit(limit)
        .where(*filters)
    ).all()

    return APIListResponse(
        data=list(families),
        total=len(families),
        page=page,
        page_size=len(families),
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
      SELECT DISTINCT ON (concept->>'relation', concept->>'preferred_label', concept->>'subconcept_of_labels')
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


@router.get(
    "/documents", response_model=APIListResponse[FamilyDocumentPublicWithFamily]
)
def read_documents(
    *, session: Session = Depends(get_session), page: int = Query(1, ge=1)
):
    limit = 10
    offset = (page - 1) * limit
    documents = session.exec(
        select(FamilyDocument)
        .offset(offset)
        .limit(limit)
        .order_by(desc(FamilyDocument.last_modified))
    ).all()

    return APIListResponse(
        data=list(documents),
        total=len(documents),
        page=page,
        page_size=len(documents),
    )


@router.get(
    "/documents/{document_id}",
    response_model=APIItemResponse[FamilyDocumentPublicWithFamily],
)
def read_document(*, session: Session = Depends(get_session), document_id: str):
    document = session.exec(
        FamilyDocument.eager_loaded_select().where(
            FamilyDocument.import_id == document_id
        )
    ).one_or_none()

    if document is None:
        raise HTTPException(status_code=404, detail="Not found")

    return APIItemResponse(data=document)


@router.get(
    "/collections", response_model=APIListResponse[CollectionPublicWithFamilies]
)
def read_collections(
    *, session: Session = Depends(get_session), page: int = Query(1, ge=1)
):
    limit = 10
    offset = (page - 1) * limit
    collections = session.exec(select(Collection).offset(offset).limit(limit)).all()

    return APIListResponse(
        data=list(collections),
        total=len(collections),
        page=page,
        page_size=len(collections),
    )


@router.get(
    "/collections/{collection_id}",
    response_model=APIItemResponse[CollectionPublicWithFamilies],
)
def read_collection(*, session: Session = Depends(get_session), collection_id: str):
    collection = session.exec(
        select(Collection).where(Collection.import_id == collection_id)
    ).one_or_none()

    if collection is None:
        raise HTTPException(status_code=404, detail="Not found")

    return APIItemResponse(data=collection)


@router.get("/slugs", response_model=APIListResponse[Slug])
def read_slugs(*, session: Session = Depends(get_session), page: int = Query(1, ge=1)):
    limit = 10
    offset = (page - 1) * limit
    slugs = session.exec(select(Slug).offset(offset).limit(limit)).all()

    return APIListResponse(
        data=list(slugs),
        total=len(slugs),
        page=page,
        page_size=len(slugs),
    )


@router.get(
    "/slugs/{slug_name}",
    response_model=APIItemResponse[Slug],
)
def read_slug(*, session: Session = Depends(get_session), slug_name: str):
    slug = session.exec(select(Slug).where(Slug.name == slug_name)).one_or_none()

    if slug is None:
        raise HTTPException(status_code=404, detail="Not found")

    return APIItemResponse(data=slug)


@router.get("/{family_id}", response_model=APIItemResponse[FamilyPublic])
def read_family(*, session: Session = Depends(get_session), family_id: str):
    # When should this break?
    # https://sqlmodel.tiangolo.com/tutorial/fastapi/read-one/#path-operation-for-one-hero
    family = session.exec(
        Family.eager_loaded_select().where(Family.import_id == family_id)
    ).one_or_none()

    if family is None:
        raise HTTPException(status_code=404, detail="Not found")

    return APIItemResponse(
        data=family,
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
    corpus_import_ids: list[str] = Query(
        default=[],
        alias="corpus.import_id",
    ),
    document_statuses: list[FamilyDocumentStatus] = Query(
        default=[],
        alias="documents.document_status",
    ),
):
    filters = []
    if corpus_import_ids:
        filters.append(Corpus.import_id.in_(corpus_import_ids))  # type: ignore
    if document_statuses:
        filters.append(FamilyDocument.document_status.in_(document_statuses))  # type: ignore

    stmt = (
        select(
            Geography.value.label("code"),  # type: ignore
            Geography.display_value.label("name"),  # type: ignore
            Geography.type,
            func.count(PhysicalDocument.id).label("count"),  # type: ignore
        )
        .join(FamilyGeographyLink, Geography.id == FamilyGeographyLink.geography_id)  # type: ignore
        .join(Family, FamilyGeographyLink.family_import_id == Family.import_id)  # type: ignore
        .join(FamilyDocument, Family.import_id == FamilyDocument.family_import_id)  # type: ignore
        .join(
            PhysicalDocument,
            FamilyDocument.physical_document_id == PhysicalDocument.id,  # type: ignore
        )
        .join(FamilyCorpusLink, Family.import_id == FamilyCorpusLink.family_import_id)  # type: ignore
        .join(Corpus, FamilyCorpusLink.corpus_import_id == Corpus.import_id)  # type: ignore
        .where(*filters)
        .group_by(Geography.id)  # type: ignore
        .order_by(func.count(PhysicalDocument.id).desc())  # type: ignore
    )

    data = session.exec(stmt).all()

    return APIListResponse(
        data=list(data),
        total=len(data),
        page=1,
        page_size=len(data),
    )
