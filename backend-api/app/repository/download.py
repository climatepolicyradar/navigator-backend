"""Functions to support browsing the RDS document structure"""

import os
from logging import getLogger

import pandas as pd
from fastapi import Depends
from sqlalchemy import bindparam, text
from sqlalchemy.types import ARRAY, DATETIME, String

from app.clients.db.session import get_db
from app.repository.helpers import get_query_template

_LOGGER = getLogger(__name__)


def get_whole_database_dump(
    ingest_cycle_start: str, allowed_corpora_ids: list[str], db=Depends(get_db)
):
    """Get whole database dump and bind variables.

    :param str ingest_cycle_start: The current ingest cycle date.
    :param list[str] allowed_corpora_ids: The corpora from which we
        should allow the data to be dumped.
    :return pd.DataFrame: A DataFrame containing the results of the SQL
        query that gets the whole database dump in our desired format.
    """
    query = text(
        get_query_template(os.path.join("app", "repository", "sql", "download.sql"))
    ).bindparams(
        bindparam("ingest_cycle_start", type_=DATETIME),
        bindparam(
            "allowed_corpora_ids", value=allowed_corpora_ids, type_=ARRAY(String)
        ),
    )

    with db.connection() as conn:
        result = conn.execute(
            query,
            {
                "ingest_cycle_start": ingest_cycle_start,
                "allowed_corpora_ids": allowed_corpora_ids,
            },
        )
        columns = result.keys()
        df = pd.DataFrame(result.fetchall(), columns=columns)
        return df
