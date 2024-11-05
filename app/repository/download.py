"""Functions to support browsing the RDS document structure"""

from logging import getLogger

import pandas as pd
from fastapi import Depends

from app.clients.db.session import get_db

_LOGGER = getLogger(__name__)


def get_whole_database_dump(query, db=Depends(get_db)):
    with db.connection() as conn:
        df = pd.read_sql(query, conn.connection)
        return df
