from typing import Annotated

from fastapi import Depends, Header, Request

from app.api.api_v1.routers.lookups.router import lookups_router
from app.clients.db.session import get_db
from app.models.config import ApplicationConfig
from app.repository.lookups import get_config
from app.service.custom_app import AppTokenFactory


@lookups_router.get("/config", response_model=ApplicationConfig)
def lookup_config(
    request: Request, app_token: Annotated[str, Header()], db=Depends(get_db)
):
    """Get the config for the metadata."""
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    return get_config(db, token.allowed_corpora_ids)
