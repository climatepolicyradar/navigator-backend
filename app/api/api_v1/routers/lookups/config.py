from typing import Sequence

from fastapi import Depends, Request, Response

from app.api.api_v1.schemas.metadata import Config
from app.core.lookups import get_metadata
from app.db.session import get_db
from app.db.crud.document import get_document_ids
from .router import lookups_router


@lookups_router.get("/config", response_model=Config)
def lookup_config(
    request: Request,
    db=Depends(get_db),
):
    """Get the config for the metadata."""
    return get_metadata(db)


@lookups_router.get(
    "/config/ids",
    response_model=Sequence[str],
    summary="Get a list of all document ids",
)
async def document_ids(
    response: Response,
    db=Depends(get_db),
) -> Sequence[str]:
    """
    Get all document ids.

    This endpoint is designed so that you can HEAD and get the hash (md5sum) from the
    ETag header.

    :param [Response] response: Response object
    :param [Session] db: Database connection
    :return [Sequence[str]]: All document IDs
    """
    (hash, id_list) = get_document_ids(db)
    response.headers["ETag"] = hash
    return id_list
