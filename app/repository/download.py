"""Functions to support browsing the RDS document structure"""

import os
from functools import lru_cache
from logging import getLogger

import pandas as pd
from fastapi import Depends

from app.clients.db.session import get_db

_LOGGER = getLogger(__name__)


@lru_cache()
def _get_query_template():
    with open(os.path.join("app", "repository", "sql", "download.sql"), "r") as file:
        return file.read()


def get_whole_database_dump(query, db=Depends(get_db)):
    with db.connection() as conn:
        df = pd.read_sql(query, conn.connection)
        return df
