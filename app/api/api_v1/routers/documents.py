from http.client import INTERNAL_SERVER_ERROR, NOT_FOUND
import logging
from typing import Union

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
)

from app.db.crud.document import (
    get_family_and_documents,
    get_family_document_and_context,
    get_slugged_objects,
)

from app.api.api_v1.schemas.document import (
    FamilyAndDocumentsResponse,
    FamilyDocumentWithContextResponse,
)

from app.db.session import get_db

_LOGGER = logging.getLogger(__file__)

documents_router = APIRouter()


@documents_router.get(
    "/documents/{import_id_or_slug}",
    response_model=Union[
        FamilyAndDocumentsResponse,
        FamilyDocumentWithContextResponse,
    ],
)
async def document_detail(
    import_id_or_slug: str,
    db=Depends(get_db),
    group_documents: bool = False,
):
    """Get details of the document with the given ID."""
    _LOGGER.info(
        f"Getting detailed information for document '{import_id_or_slug}'",
        extra={
            "props": {
                "import_id_or_slug": import_id_or_slug,
                "group_documents": group_documents,
            },
        },
    )

    ids = get_slugged_objects(db, import_id_or_slug)
    if not ids:
        raise HTTPException(
            status_code=NOT_FOUND, detail=f"Nothing found for {import_id_or_slug}"
        )

    family_document_import_id, family_import_id = ids

    response = None

    if family_import_id:
        response = get_family_and_documents(db, family_import_id)
    elif family_document_import_id:
        response = get_family_document_and_context(db, family_document_import_id)

    if response:
        return response

    raise HTTPException(
        status_code=INTERNAL_SERVER_ERROR,
        detail=f"Slug entry found and not pointing at anything: {import_id_or_slug}",
    )
