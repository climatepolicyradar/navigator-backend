import logging

from fastapi import Depends
from sqlalchemy.orm import Session

from app.clients.db.session import get_db
from app.config import DEVELOPMENT_MODE
from app.service.search import _VESPA_CONNECTION

_LOGGER = logging.getLogger(__file__)


def is_rds_online(db):
    """Runs a sql query against the db"""
    return db.execute("SELECT 1") is not None


def is_vespa_online():
    """Check queries work against the vespa instance"""
    try:
        query_body = {"yql": "select * from sources * where true", "hits": "0"}
        _VESPA_CONNECTION.client.query(query_body)
    except Exception as e:
        if DEVELOPMENT_MODE is False:
            _LOGGER.error(e)
        return False
    return True


def is_database_online(db: Session = Depends(get_db)) -> bool:
    """Checks database health."""
    return all([is_rds_online(db), is_vespa_online()])
