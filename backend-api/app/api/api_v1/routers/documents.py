import logging
from http.client import NOT_FOUND
from typing import Annotated, Union

from cpr_sdk.models.search import SearchResponse
from cpr_sdk.search_adaptors import VespaSearchAdapter
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
from app.service.search import get_document_from_vespa, get_family_from_vespa
from app.service.vespa import get_vespa_search_adapter
from app.telemetry_exceptions import ExceptionHandlingTelemetryRoute

_LOGGER = logging.getLogger(__file__)

documents_router = APIRouter(route_class=ExceptionHandlingTelemetryRoute)


@documents_router.get(
    "/documents/{slug}",
    response_model=Union[
        FamilyAndDocumentsResponse,
        FamilyDocumentWithContextResponse,
    ],
)
def family_or_document_detail(
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


@documents_router.get("/families/{import_id}", response_model=SearchResponse)
def family_detail_from_vespa(
    import_id: str,
    request: Request,
    app_token: Annotated[str, Header()],
    db=Depends(get_db),
    vespa_search_adapter: VespaSearchAdapter = Depends(get_vespa_search_adapter),
):
    """Get details of the family associated with a slug from vespa.

    NOTE: As part of our concepts spike, we're going to use this endpoint
    to get the family data from Vespa. The frontend will use this
    endpoint alongside the `/documents` endpoint if feature flags are
    enabled.

    :param str import_id: Family import id to get vespa representation
        for.
    :param Request request: Request object.
    :param Annotated[str, Header()] app_token: App token containing
        allowed corpora.
    :param Depends[get_db] db: Database session to query against.
    :return SearchResponse: An object representing the family in
        Vespa - including concepts.
    """
    _LOGGER.info(
        f"Getting detailed information for vespa family '{import_id}'",
        extra={
            "props": {"import_id_or_slug": import_id, "app_token": str(app_token)},
        },
    )

    # Decode the app token and validate it.
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    try:
        # TODO: Make this respect the allowed corpora from the decoded token.
        hits = get_family_from_vespa(
            family_id=import_id, db=db, vespa_search_adapter=vespa_search_adapter
        )
        if hits.total_family_hits == 0:
            raise HTTPException(
                status_code=NOT_FOUND, detail=f"Nothing found for {import_id} in Vespa"
            )
        return hits
    except ValueError as err:
        raise HTTPException(status_code=NOT_FOUND, detail=str(err))


@documents_router.get("/document/{import_id}", response_model=SearchResponse)
def doc_detail_from_vespa(
    import_id: str,
    request: Request,
    app_token: Annotated[str, Header()],
    db=Depends(get_db),
    vespa_search_adapter: VespaSearchAdapter = Depends(get_vespa_search_adapter),
):
    """Get details of the document associated with a slug from vespa.

    NOTE: As part of our concepts spike, we're going to use this endpoint
    to get the document data from Vespa. The frontend will use this
    endpoint alongside the `/documents` endpoint if feature flags are
    enabled.

    :param str import_id: Document import id to get vespa representation
        for.
    :param Request request: Request object.
    :param Annotated[str, Header()] app_token: App token containing
        allowed corpora.
    :param Depends[get_db] db: Database session to query against.
    :return SearchResponse: An object representing the document in
        Vespa - including concepts.
    """
    _LOGGER.info(
        f"Getting detailed information for vespa document '{import_id}'",
        extra={
            "props": {"import_id_or_slug": import_id, "app_token": str(app_token)},
        },
    )

    # Decode the app token and validate it.
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    try:
        # TODO: Make this respect the allowed corpora from the decoded token.
        hits = get_document_from_vespa(
            document_id=import_id, db=db, vespa_search_adapter=vespa_search_adapter
        )
        if hits.total_family_hits == 0:
            raise HTTPException(
                status_code=NOT_FOUND, detail=f"Nothing found for {import_id} in Vespa"
            )
        return hits
    except ValueError as err:
        raise HTTPException(status_code=NOT_FOUND, detail=str(err))
