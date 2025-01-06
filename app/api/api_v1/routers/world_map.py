import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.clients.db.session import get_db
from app.dependencies.auth import get_validated_token
from app.errors import RepositoryError, ValidationError
from app.models.geography import GeographyStatsDTO
from app.service.custom_app import AppTokenFactory
from app.service.util import get_allowed_corpora_from_token
from app.service.world_map import get_world_map_stats

_LOGGER = logging.getLogger(__file__)

world_map_router = APIRouter()


@world_map_router.get("/geographies", response_model=list[GeographyStatsDTO])
async def world_map_stats(
    request: Request,
    db=Depends(get_db),
    token: Optional[AppTokenFactory] = Depends(get_validated_token),
):
    """Get a summary of family counts for all geographies for world map."""

    allowed_corpora_ids = get_allowed_corpora_from_token(token)

    _LOGGER.info(
        "Getting world map counts for all geographies",
        extra={
            "props": {"allowed_corpora_ids": str(allowed_corpora_ids)},
        },
    )

    try:
        world_map_stats = get_world_map_stats(db, allowed_corpora_ids)

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
