import logging
from http.client import NOT_FOUND
from typing import Annotated, Union

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from app.clients.db.session import get_db
from app.models.document import (
    FamilyAndDocumentsResponse,
    FamilyDocumentWithContextResponse,
)
from app.repository.document import (
    get_family_and_documents,
    get_family_document_and_context,
    get_slugged_objects,
)
from app.service.custom_app import AppTokenFactory

_LOGGER = logging.getLogger(__file__)

documents_router = APIRouter()


@documents_router.get(
    "/documents/{slug}",
    response_model=Union[
        FamilyAndDocumentsResponse,
        FamilyDocumentWithContextResponse,
    ],
)
async def family_or_document_detail(
    slug: str, request: Request, app_token: Annotated[str, Header()], db=Depends(get_db)
):
    """Get details of the family or document associated with the slug."""
    _LOGGER.info(
        f"Getting detailed information for family or document '{slug}'",
        extra={
            "props": {"import_id_or_slug": slug, "app_token": str(app_token)},
        },
    )

    # Decode the app token and validate it.
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    family_document_import_id, family_import_id = get_slugged_objects(
        db, slug, token.allowed_corpora_ids
    )
    if family_document_import_id is None and family_import_id is None:
        raise HTTPException(status_code=NOT_FOUND, detail=f"Nothing found for {slug}")

    try:
        # Family import id takes precedence, at at least one is not None
        if family_import_id:
            return get_family_and_documents(db, family_import_id)
        elif family_document_import_id:
            return get_family_document_and_context(db, family_document_import_id)
    except ValueError as err:
        raise HTTPException(status_code=NOT_FOUND, detail=str(err))
