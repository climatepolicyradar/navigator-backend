import logging

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    status,
)
from sqlalchemy import update

from app.api.api_v1.schemas.document import (
    DocumentUpdateRequest,
)
from app.core.auth import get_superuser_details
from app.core.validation import IMPORT_ID_MATCHER
from app.db.models.document.physical_document import PhysicalDocument
from app.db.models.law_policy.family import FamilyDocument, Slug
from app.db.session import get_db

_LOGGER = logging.getLogger(__name__)

admin_document_router = r = APIRouter()


@r.put("/documents/{import_id_or_slug}", status_code=status.HTTP_200_OK)
async def update_document(
    request: Request,
    import_id_or_slug: str,
    meta_data: DocumentUpdateRequest,
    db=Depends(get_db),
    current_user=Depends(get_superuser_details),
):
    # TODO: As this grows move it out into the crud later.

    _LOGGER.info(
        f"Superuser '{current_user.email}' called update_document",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "import_id_or_slug": import_id_or_slug,
                "meta_data": meta_data.as_json(),
            }
        },
    )

    # First query the FamilyDocument
    query = db.query(FamilyDocument)
    if IMPORT_ID_MATCHER.match(import_id_or_slug) is not None:
        family_document = query.filter(
            FamilyDocument.import_id == import_id_or_slug
        ).one_or_none()
        _LOGGER.info("update_document called with import_id")
    else:
        family_document = (
            query.join(Slug, Slug.family_document_import_id == FamilyDocument.import_id)
            .filter(Slug.name == import_id_or_slug)
            .one_or_none()
        )
        _LOGGER.info("update_document called with slug")

    # Check we have found one
    if family_document is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )

    # Get the physical document to update
    physical_document = family_document.physical_document

    # Note this code relies on the fields being the same as the db column names
    num_changed = db.execute(
        update(PhysicalDocument)
        .values(meta_data.dict())
        .where(PhysicalDocument.id == physical_document.id)
    ).rowcount

    if num_changed == 0:
        _LOGGER.info("update_document complete - nothing changed")
        return physical_document  # Nothing to do - as should be idempotent

    if num_changed > 1:
        # This should never happen due to table uniqueness constraints
        # TODO Rollback
        raise HTTPException(
            detail=(
                f"There was more than one document identified by {import_id_or_slug}. "
                "This should not happen!!!"
            ),
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    db.commit()
    db.refresh(physical_document)
    _LOGGER.info(
        "Call to update_document complete",
        extra={
            "props": {
                "superuser_email": current_user.email,
                "num_changed": num_changed,
                "import_id": family_document.import_id,
                "md5_sum": physical_document.md5_sum,
                "content_type": physical_document.content_type,
                "cdn_object": physical_document.cdn_object,
            }
        },
    )
    return physical_document
