import logging
from http.client import NOT_FOUND
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from app.clients.db.session import get_db
from app.models.document import CollectionOverviewResponse
from app.repository.collection import get_collection, get_id_from_slug
from app.service.custom_app import AppTokenFactory

_LOGGER = logging.getLogger(__file__)

collections_router = APIRouter()


@collections_router.get(
    "/collections/{slug}", response_model=CollectionOverviewResponse
)
def collection_detail(
    slug: str,
    request: Request,
    app_token: Annotated[str, Header()],
    db=Depends(get_db),
):
    """Get details of the collection associated with the import id."""
    _LOGGER.info(
        f"Getting detailed information for collection associated with the'{slug}'",
        extra={
            "props": {"slug": slug, "app_token": str(app_token)},
        },
    )

    # Decode the app token and validate it.
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    collection_import_id = get_id_from_slug(db, slug, token.allowed_corpora_ids)
    if collection_import_id is None:
        raise HTTPException(status_code=NOT_FOUND, detail=f"Nothing found for {slug}")

    try:
        if collection_import_id:
            return get_collection(db, collection_import_id)
    except ValueError as error:
        raise HTTPException(status_code=NOT_FOUND, detail=str(error))
