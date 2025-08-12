import logging

from fastapi import APIRouter, Depends

from app.clients.db.session import get_db
from app.telemetry_exceptions import ExceptionHandlingTelemetryRoute

_LOGGER = logging.getLogger(__name__)

latest_published_router = APIRouter(route_class=ExceptionHandlingTelemetryRoute)


@latest_published_router.get(
    "/latest_published",
    summary="Gets five most recently published families.",
)
def latest_published(
    db=Depends(get_db),
):
    """Gets five most recently published families."""

    return [
        {
            "import_id": f"family_{i}",
            "title": f"Family {i} Title",
            "description": f"Description for family {i}",
            "category": "Category A",
            "published_date": "2023-10-01",
            "last_modified": "2023-10-02",
            "metadata": {"key": f"value_{i}"},
            "geographies": ["Geo1", "Geo2"],
            "slug": f"family-{i}",
        }
        for i in range(5)  # Placeholder for actual family data retrieval
    ]
