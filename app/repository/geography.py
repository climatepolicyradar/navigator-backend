"""Functions to support the geographies endpoint."""

import logging
from typing import Optional, Sequence

from db_client.models.dfce.family import (
    Corpus,
    Family,
    FamilyCorpus,
    FamilyDocument,
    FamilyGeography,
    FamilyStatus,
)
from db_client.models.dfce.geography import Geography
from sqlalchemy import func
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Query, Session

from app.errors import RepositoryError
from app.models.geography import GeographyStatsDTO

_LOGGER = logging.getLogger(__file__)


def get_geo_subquery(
    db: Session,
    allowed_geo_slugs: Optional[Sequence[str]] = None,
    allowed_geo_values: Optional[Sequence[str]] = None,
    family_document_import_id: Optional[str] = None,
) -> Query:
    """
    Create a subquery to fetch geographies associated with families.

    :param Session db: Database session.
    :param Optional[Sequence[str]] allowed_geo_slugs: Optional list of
        allowed geography slugs.
    :param Optional[Sequence[str]] allowed_geo_values: Optional list of
        allowed geography values.
    :param Optional[str] family_document_import_id: Optional family
        document import ID.
    :return Query: A subquery for geographies.
    """
    geo_subquery = (
        db.query(
            Geography.value.label("value"),
            Geography.slug.label("slug"),
            FamilyGeography.family_import_id,
        )
        .join(FamilyGeography, FamilyGeography.geography_id == Geography.id)
        .filter(FamilyGeography.family_import_id == Family.import_id)
    )

    if allowed_geo_slugs is not None:
        geo_subquery = geo_subquery.filter(Geography.slug.in_(allowed_geo_slugs))

    if allowed_geo_values is not None:
        geo_subquery = geo_subquery.filter(Geography.value.in_(allowed_geo_values))

    if family_document_import_id is not None:
        geo_subquery = geo_subquery.join(
            FamilyDocument,
            FamilyDocument.family_import_id == FamilyGeography.family_import_id,
        ).filter(FamilyDocument.import_id == family_document_import_id)

    return geo_subquery.subquery("geo_subquery")


def _db_count_fams_in_category_and_geo(
    db: Session, allowed_corpora: Optional[list[str]]
) -> Query:
    """
    Query the database for the fam count per category per geo.

    NOTE: SqlAlchemy will make a complete hash of query generation if
    columns are used in the query() call. Therefore, entire objects are
    returned.

    :param Session db: DB Session to perform query on.
    :param Optional[list[str]] allowed_corpora: The list of allowed
        corpora IDs to filter on.
    :return Query: A Query object containing the queries to perform.
    """
    # Get the required Geography information and cross join each with all of the unique
    # family_category values (so if some geographies have no documents for a particular
    # family_category, we can set the count for that category to 0).
    family_categories = db.query(Family.family_category).distinct().subquery()
    geo_family_combinations = db.query(
        Geography.id.label("geography_id"),
        Geography.display_value,
        Geography.slug,
        Geography.value,
        family_categories.c.family_category,
    ).subquery("geo_family_combinations")

    # Get a count of documents in each present family_category for each geography.
    counts = (
        db.query(
            Family.family_category,
            FamilyGeography.geography_id,
            func.count().label("records_count"),
        )
        .join(FamilyGeography, Family.import_id == FamilyGeography.family_import_id)
        # .join(FamilyCorpus, Family.import_id == FamilyCorpus.family_import_id)
        # .join(Corpus, Corpus.import_id == FamilyCorpus.family_import_id)
        .filter(Family.family_status == FamilyStatus.PUBLISHED)
        .group_by(Family.family_category, FamilyGeography.geography_id)
        .subquery("counts")
    )
    # if allowed_corpora is not None:
    #     counts = counts.where(Corpus.import_id.in_(allowed_corpora))
    # counts = counts.group_by(
    #     Family.family_category, FamilyGeography.geography_id
    # ).subquery("counts")
    # _LOGGER.info(counts)

    # Aggregate family_category counts per geography into a JSONB object, and if a
    # family_category count is missing, set the count for that category to 0 so each
    # geography will always have a count for all family_category values.
    query = (
        db.query(
            geo_family_combinations.c.display_value.label("display_value"),
            geo_family_combinations.c.slug.label("slug"),
            geo_family_combinations.c.value.label("value"),
            func.jsonb_object_agg(
                geo_family_combinations.c.family_category,
                func.coalesce(counts.c.records_count, 0),
            ).label("counts"),
        )
        .select_from(
            geo_family_combinations.join(
                counts,
                (geo_family_combinations.c.geography_id == counts.c.geography_id)
                & (
                    geo_family_combinations.c.family_category
                    == counts.c.family_category
                ),
                isouter=True,
            )
        )
        .group_by(
            geo_family_combinations.c.display_value,
            geo_family_combinations.c.slug,
            geo_family_combinations.c.value,
        )
        .order_by(geo_family_combinations.c.display_value)
    )
    print(query.statement.compile(dialect=postgresql.dialect()))
    # print(str(query.statement.compile(compile_kwargs={"literal_binds": True})))
    print(render_query(query, db))
    return query


def _to_dto(family_doc_geo_stats) -> GeographyStatsDTO:
    """
    Convert result set item to GeographyDTO.

    :param family_doc_geo_stats: A Tuple representing a record in the
        result set of a performed query.
    :return GeographyStatsDTO: A JSON serialisable representation of the
        Tuple record returned from the database.
    """
    return GeographyStatsDTO(
        display_name=family_doc_geo_stats.display_value,
        iso_code=family_doc_geo_stats.value,
        slug=family_doc_geo_stats.slug,
        family_counts=family_doc_geo_stats.counts,
    )


def get_world_map_stats(
    db: Session, allowed_corpora: Optional[list[str]]
) -> list[GeographyStatsDTO]:
    """
    Get a count of fam per category per geography for all geographies.

    :param db Session: The database session.
    :param Optional[list[str]] allowed_corpora: The list of allowed
        corpora IDs to filter on.
    :return list[GeographyStatsDTO]: A list of Geography stats objects
    """
    try:
        family_geo_stats = _db_count_fams_in_category_and_geo(db, allowed_corpora).all()
    except OperationalError as e:
        _LOGGER.error(e)
        raise RepositoryError("Error querying the database for geography stats")

    if not family_geo_stats:
        return []

    result = [_to_dto(fgs) for fgs in family_geo_stats]
    return result


from datetime import date, datetime, timedelta

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
            if isinstance(value, int):
                return str(value)
            elif isinstance(value, (str, date, datetime, timedelta)):
                return "'{}'".format(str(value).replace("'", "''"))
            elif isinstance(value, list):
                return "'{{{}}}'".format(
                    ",".join(
                        [self.render_array_value(x, type_.item_type) for x in value]
                    )
                )
            return super(LiteralCompiler, self).render_literal_value(value, type_)

    return LiteralCompiler(dialect, statement).process(statement)
