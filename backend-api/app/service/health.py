import logging

from fastapi import Depends
from sqlalchemy.orm import Session

from app.clients.db.session import get_db

_LOGGER = logging.getLogger(__file__)


def is_rds_online(db: Session) -> bool:
    """Runs a heartbeat SQL query against the DB to check health.

    :param db: Database session
    :type db: Session
    :return: True if database is responsive
    :rtype: bool
    """
    try:
        return db.execute("SELECT 1") is not None
    except Exception as e:
        _LOGGER.error(f"🔴 RDS health check failed: {e}")
        return False


def is_database_online(db: Session = Depends(get_db)) -> bool:
    """Checks database health.

    :param db: Database session, defaults to Depends(get_db)
    :type db: Session
    :return: True if all database components are online
    :rtype: bool
    """
    return is_rds_online(db)
