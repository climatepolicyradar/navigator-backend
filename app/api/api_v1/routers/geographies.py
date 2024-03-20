import logging

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.api_v1.schemas.geography import GeographyDTO
from app.db.crud.geography import get_geography_stats
from app.db.session import get_db
from app.errors import RepositoryError

_LOGGER = logging.getLogger(__file__)

geographies_router = APIRouter()


@geographies_router.get("/geographies", response_model=list[GeographyDTO])
async def geographies(db=Depends(get_db)):
    """Get a summary of all geographies for world map."""
    _LOGGER.info("Getting detailed information on all geographies")

    try:
        geo_stats = get_geography_stats(db)

        if geo_stats == []:
            _LOGGER.error("No geography stats found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="No geography stats found"
            )

        return geo_stats
    except RepositoryError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=e.message
        )
