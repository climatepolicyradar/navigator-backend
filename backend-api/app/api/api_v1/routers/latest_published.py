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

    return ""
