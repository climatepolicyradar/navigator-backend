import logging
from typing import Any, cast

from sqlalchemy.engine import Engine
from sqlalchemy.sql import text

logger = logging.getLogger(__name__)


class PytestHelpers:  # noqa: D101
    def __init__(self, engine: Engine):
        self.engine = engine

    def _get_psql_compatible_url(self) -> str:
        # psql doesn't like the + syntax that is sometimes used in sqlalchemy
        return str(self.engine.url).replace("+psycopg2", "")

    def execute(self, sql: str) -> None:
        """Execute sql."""
        conn = cast(Any, self.engine.connect())
        conn.execute(text(sql))
        conn.close()

    def drop_all(self) -> None:
        """Drop all of the tables/indexes etc."""
        self.execute("DROP SCHEMA public CASCADE")
        self.execute("CREATE SCHEMA public")

    def add_alembic(self) -> None:
        """Add alembic migration table to test DB."""
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS alembic_version (
               version_num VARCHAR(32) NOT NULL,
               CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
            )
            """
        )


def clean_tables(session, exclude, sqlalchemy_base):
    """Clean (aka: truncate) table.  SQLAlchemy models listed in exclude will be skipped."""
    non_static_tables = [
        t for t in reversed(sqlalchemy_base.metadata.sorted_tables) if t not in exclude
    ]
    for table in non_static_tables:
        # "DELETE FROM $table" is quicker than TRUNCATE for small tables
        session.execute(table.delete())

    # reset all the sequences
    sql = "SELECT c.relname FROM pg_class c WHERE c.relkind = 'S'"
    for [sequence] in session.execute(text(sql)):
        session.execute(text(f"ALTER SEQUENCE {sequence} RESTART WITH 1"))

    session.commit()
