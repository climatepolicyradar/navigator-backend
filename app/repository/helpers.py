"""Helper functions for the repository layer."""

from datetime import date, datetime, timedelta
from functools import lru_cache

from sqlalchemy.orm import Query


def render_query(statement, db_session):
    """
    Generate an SQL expression string with bound parameters rendered inline
    for the given SQLAlchemy statement.
    WARNING: This method of escaping is insecure, incomplete, and for debugging
    purposes only. Executing SQL statements with inline-rendered user values is
    extremely insecure.
    Based on http://stackoverflow.com/questions/5631078/sqlalchemy-print-the-actual-query
    """
    if isinstance(statement, Query):
        statement = statement.statement
    dialect = db_session.bind.dialect

    class LiteralCompiler(dialect.statement_compiler):
        def visit_bindparam(
            self, bindparam, within_columns_clause=False, literal_binds=False, **kwargs
        ):
            return self.render_literal_value(bindparam.value, bindparam.type)

        def render_array_value(self, val, item_type):
            if isinstance(val, list):
                return "{}".format(
                    ",".join([self.render_array_value(x, item_type) for x in val])
                )
            return self.render_literal_value(val, item_type)

        def render_literal_value(self, value, type_):
            if value is None:
                return None
            if isinstance(value, int):
                return str(value)
            if isinstance(value, (str, date, datetime, timedelta)):
                return "'{}'".format(str(value).replace("'", "''"))
            if isinstance(value, (list)):
                return "'{{{}}}'".format(
                    ",".join(
                        [self.render_array_value(x, type_.item_type) for x in value]
                    )
                )
            if isinstance(value, tuple):
                return "'{{{}}}'".format(
                    ",".join(
                        [self.render_array_value(x, type_.item_type) for x in value]
                    )
                )
            return super(LiteralCompiler, self).render_literal_value(value, type_)

    return LiteralCompiler(dialect, statement).process(statement)


@lru_cache()
def get_query_template(filepath: str) -> str:
    """Read query for non-deleted docs and their associated data."""
    with open(filepath, "r") as file:
        return file.read()
