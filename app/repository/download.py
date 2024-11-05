"""Functions to support browsing the RDS document structure"""

import os
from logging import getLogger

import pandas as pd
from fastapi import Depends

from app.clients.db.session import get_db
from app.repository.helpers import get_query_template

_LOGGER = getLogger(__name__)


def get_whole_database_dump(
    ingest_cycle_start: str, allowed_corpora_ids: list[str], db=Depends(get_db)
):
    """Get  whole database dump and bind variables.

    :param str ingest_cycle_start: The current ingest cycle date.
    :param list[str] allowed_corpora_ids: The corpora from which we
        should allow the data to be dumped.
    :return str: The SQL query to perform on the database session.
    """
    query_template = get_query_template(
        os.path.join("app", "repository", "sql", "download.sql")
    )

    with db.connection() as conn:
        df = pd.read_sql(
            query_template,
            conn.connection,
            params={
                "ingest_cycle_start": ingest_cycle_start,
                "allowed_corpora_ids": allowed_corpora_ids,
            },
        )
        return df
