"""
Functions to support the documents endpoints

old functions (non DFC) are moved to the deprecated_documents.py file.
"""
import logging
from typing import Optional, cast
from sqlalchemy.orm import Session
from app.api.api_v1.schemas.document import FamilyAndDocumentsResponse
from app.db.models.law_policy.family import Family, Slug
from app.db.models.law_policy.geography import Geography
from app.db.models.law_policy.metadata import FamilyMetadata

_LOGGER = logging.getLogger(__file__)


def get_family_and_documents(
    db: Session, slug: str
) -> Optional[FamilyAndDocumentsResponse]:
    """
    Get a document along with the family information.

    :param Session db: connection to db
    :param str slug: id of document
    :return DocumentWithFamilyResponse: response object
    """

    db_objects = (
        db.query(Family, Geography, Slug, FamilyMetadata)
        .filter(Family.geography_id == Geography.id)
        .filter(Family.import_id == FamilyMetadata.family_import_id)
        .filter(Slug.name == slug)
    ).first()

    if not db_objects:
        return None

    family: Family
    family, geography, slug, family_metadata = db_objects

    # import_id = family.import_id

    # documents = db.query(FamilyDocument).filter(
    #     FamilyDocument.family_import_id == import_id
    # )

    return FamilyAndDocumentsResponse(
        title=cast(str, family.title),
        geography=cast(str, geography.value),
        category=cast(str, family.family_category),
        status=cast(str, family.family_status),
        slugs=[],
        events=[],
        documents=[],
        published_date=None,
        last_updated_date=None,
    )


# class FamilyAndDocumentsResponse(BaseModel):
#     """A response containing the document and associated family"""

#     title: str
#     geography: str
#     category: str
#     status: str
#     slugs: list[str]
#     events: list[FamilyEventsResponse]
#     published_date: Optional[str]
#     last_updated_date: Optional[str]
#     documents: list[FamilyDocumentsResponse]


# class FamilyDocumentsResponse(BaseModel):
#     variant: str
#     slugs: list[str]
#     # What follows is off PhysicalDocument
#     title: str
#     md5_sum: str
#     cdn_object: str
#     source_url: str
#     content_type: str

# class FamilyEventsResponse(BaseModel):
#     title: str
#     date: datetime
#     event_type: str
#     status: str
