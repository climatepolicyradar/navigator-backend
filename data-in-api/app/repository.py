import logging

from sqlalchemy.exc import DisconnectionError, OperationalError
from sqlmodel import Session, select

_LOGGER = logging.getLogger(__name__)


def check_db_health(db: Session) -> bool:
    """Check database connection health.

    Performs a simple query to verify the database is accessible
    and responsive.

    :return: True if database is healthy, False otherwise
    :rtype: bool
    """
    try:
        result = db.exec(select(1))
        return result.first() is not None
    except (OperationalError, DisconnectionError):
        _LOGGER.exception("Database health check failed")
    except Exception:
        _LOGGER.exception("Unexpected error during health check")
    return False
