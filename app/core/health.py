import logging

import requests
from fastapi import Depends
from requests.compat import urljoin
from sqlalchemy.orm import Session

from app.core.config import DEVELOPMENT_MODE, VESPA_URL
from app.db.session import get_db

_LOGGER = logging.getLogger(__file__)


VESPA_HEALTH_ENDPOINT = urljoin(VESPA_URL, "/status.html")


def is_rds_online(db):
    """Runs a sql query against the db"""
    return db.execute("SELECT 1") is not None


def is_vespa_online():
    """Checks the health endpoint for the deployed vespa instance"""
    try:
        response = requests.get(VESPA_HEALTH_ENDPOINT)
    except Exception as e:
        if DEVELOPMENT_MODE == "False":
            _LOGGER.error(e)
        return False
    return response.ok


def is_database_online(db: Session = Depends(get_db)) -> bool:
    """Checks database health."""
    return all([is_rds_online(db), is_vespa_online()])
