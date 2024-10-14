import logging

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.api_v1.schemas.geography import GeographyStatsDTO
from app.clients.db.session import get_db
from app.db.crud.geography import get_world_map_stats
from app.errors import RepositoryError

_LOGGER = logging.getLogger(__file__)

geographies_router = APIRouter()


@geographies_router.get("/geographies", response_model=list[GeographyStatsDTO])
async def geographies(db=Depends(get_db)):
    """Get a summary of fam stats for all geographies for world map."""
    _LOGGER.info("Getting detailed information on all geographies")

    try:
        world_map_stats = get_world_map_stats(db)

        if world_map_stats == []:
            _LOGGER.error("No stats for world map found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No stats for world map found",
            )

        return world_map_stats
    except RepositoryError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=e.message
        )
