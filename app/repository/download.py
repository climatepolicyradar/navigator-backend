"""Functions to support browsing the RDS document structure"""

import os
from logging import getLogger

import pandas as pd
from fastapi import Depends

from app.clients.db.session import get_db
from app.repository.helpers import get_query_template

_LOGGER = getLogger(__name__)


def create_query(
    template_query, ingest_cycle_start: str, allowed_corpora_ids: list[str]
) -> str:
    """Create download whole database query, replacing variables.

    :param str ingest_cycle_start: The current ingest cycle date.
    :param list[str] allowed_corpora_ids: The corpora from which we
        should allow the data to be dumped.
    :return str: The SQL query to perform on the database session.
    """
    corpora_ids = "'" + "','".join(allowed_corpora_ids) + "'"
    return template_query.replace(  # type: ignore
        "{ingest_cycle_start}", ingest_cycle_start
    ).replace(
        "{allowed_corpora_ids}", corpora_ids
    )  # type: ignore


def get_whole_database_dump(
    ingest_cycle_start: str, allowed_corpora_ids: list[str], db=Depends(get_db)
):
    query_template = get_query_template(
        os.path.join("app", "repository", "sql", "download.sql")
    )
    query = create_query(query_template, ingest_cycle_start, allowed_corpora_ids)

    with db.connection() as conn:
        df = pd.read_sql(query, conn.connection)
        return df
