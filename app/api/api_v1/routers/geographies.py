import logging

from fastapi import APIRouter, Depends

from app.api.api_v1.schemas.geography import Geography
from app.db.session import get_db

_LOGGER = logging.getLogger(__file__)

geographies_router = APIRouter()


@geographies_router.get("/geographies", response_model=list[Geography])
async def geographies(db=Depends(get_db)):
    """Get a summary of all geographies for world map."""
    _LOGGER.info("Getting detailed information on all geographies")

    return [
        {
            "display_name": "Afghanistan",
            "iso_code": "AFG",
            "slug": "afghanistan",
            "family_counts": {
                "Executive": 100,
                "Legislative": 50,
                "UNFCCC": 25,
            },
        },
        {
            "display_name": "Bangladesh",
            "iso_code": "BGD",
            "slug": "bangladesh",
            "family_counts": {
                "Executive": 1000,
                "Legislative": 500,
                "UNFCCC": 250,
            },
        },
    ]
