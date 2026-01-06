import logging

from sqlalchemy import text
from sqlalchemy.exc import DisconnectionError, OperationalError
from sqlalchemy.orm import Session

_LOGGER = logging.getLogger(__name__)


def check_db_health(db: Session) -> bool:
    """Check database connection health.

    Performs a simple query to verify the database is accessible
    and responsive.

    :return: True if database is healthy, False otherwise
    :rtype: bool
    """
    try:
        return db.execute(text("SELECT 1")) is not None
    except (OperationalError, DisconnectionError):
        _LOGGER.exception("Database health check failed")
    except Exception:
        _LOGGER.exception("Unexpected error during health check")
    return False
