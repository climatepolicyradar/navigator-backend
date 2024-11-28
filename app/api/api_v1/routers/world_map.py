import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status

from app.clients.db.session import get_db
from app.errors import RepositoryError, ValidationError
from app.models.geography import GeographyStatsDTO
from app.service.custom_app import AppTokenFactory
from app.service.world_map import get_world_map_stats

_LOGGER = logging.getLogger(__file__)

world_map_router = APIRouter()


@world_map_router.get("/geographies", response_model=list[GeographyStatsDTO])
async def geographies(
    request: Request, app_token: Annotated[str, Header()], db=Depends(get_db)
):
    """Get a summary of family stats for all geographies for world map."""
    _LOGGER.info(
        "Getting world map counts for all geographies",
        extra={
            "props": {"app_token": str(app_token)},
        },
    )

    # Decode the app token and validate it.
    token = AppTokenFactory()
    token.decode_and_validate(db, request, app_token)

    try:
        world_map_stats = get_world_map_stats(db, token.allowed_corpora_ids)

        if world_map_stats == []:
            _LOGGER.error("No stats for world map found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No stats for world map found",
            )

        return world_map_stats
    except RepositoryError as e:
        _LOGGER.error(e)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=e.message
        )
    except ValidationError as e:
        _LOGGER.error(e)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)
