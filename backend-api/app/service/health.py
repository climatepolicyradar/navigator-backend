import logging

from cpr_sdk.search_adaptors import VespaSearchAdapter
from fastapi import Depends
from sqlalchemy.orm import Session

from app.clients.db.session import get_db
from app.config import DEVELOPMENT_MODE
from app.service.vespa import get_vespa_search_adapter

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


def is_vespa_online(
    vespa_search_adapter: VespaSearchAdapter, timeout_seconds: int = 1
) -> bool:
    """Check queries work against the vespa instance

    :param vespa_search_adapter: Vespa search adapter
    :type vespa_search_adapter: VespaSearchAdapter
    :param timeout_seconds: Timeout for the query
    :type timeout_seconds: int
    :return: True if Vespa is responsive
    :rtype: bool
    """
    try:
        query_body = {
            "yql": "select * from sources * where true",
            "hits": "0",
            "timeout": f"{timeout_seconds}",  # Default unit is seconds, otherwise specify unit
        }
        vespa_search_adapter.client.query(query_body)
        return True
    except Exception as e:
        if DEVELOPMENT_MODE is False:
            _LOGGER.error(f"🔴 Vespa health check failed: {e}")
    return False


def is_database_online(
    db: Session = Depends(get_db),
    vespa_search_adapter: VespaSearchAdapter = Depends(get_vespa_search_adapter),
) -> bool:
    """Checks database health.

    :param db: Database session, defaults to Depends(get_db)
    :type db: Session
    :param vespa_search_adapter: Vespa search adapter
    :type vespa_search_adapter: VespaSearchAdapter
    :return: True if all database components are online
    :rtype: bool
    """
    return all([is_rds_online(db), is_vespa_online(vespa_search_adapter)])
