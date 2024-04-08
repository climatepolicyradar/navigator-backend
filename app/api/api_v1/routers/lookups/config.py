from fastapi import Depends, Request

from app.api.api_v1.routers.lookups.router import lookups_router
from app.api.api_v1.schemas.metadata import ApplicationConfig
from app.core.lookups import get_config
from app.db.session import get_db


@lookups_router.get("/config", response_model=ApplicationConfig)
def lookup_config(request: Request, db=Depends(get_db)):
    """Get the config for the metadata."""
    return get_config(db)
