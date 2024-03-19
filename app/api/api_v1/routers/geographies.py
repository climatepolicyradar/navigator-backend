import logging

from fastapi import APIRouter, Depends

from app.api.api_v1.schemas.geography import GeographyDTO
from app.db.crud.geography import get_geography_stats
from app.db.session import get_db

_LOGGER = logging.getLogger(__file__)

geographies_router = APIRouter()


@geographies_router.get("/geographies", response_model=list[GeographyDTO])
async def geographies(db=Depends(get_db)):
    """Get a summary of all geographies for world map."""
    _LOGGER.info("Getting detailed information on all geographies")

    try:
        geo_stats = get_geography_stats(db)
        _LOGGER.info(geo_stats)

        if geo_stats == []:
            _LOGGER.error("No geo stats found")
            return []

        return geo_stats
    except Exception as e:
        # raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=e)
        _LOGGER.error(e)

    return []
