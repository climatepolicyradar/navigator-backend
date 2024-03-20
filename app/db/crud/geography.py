"""
Functions to support the documents endpoints

old functions (non DFC) are moved to the deprecated_documents.py file.
"""

import logging

from db_client.models.law_policy.family import Family, FamilyCategory, FamilyDocument
from db_client.models.law_policy.geography import Geography
from sqlalchemy import func
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Query, Session

from app.api.api_v1.schemas.geography import GeographyStatsDTO
from app.errors import RepositoryError

_LOGGER = logging.getLogger(__file__)


def _db_count_docs_in_category_and_geo(db: Session) -> Query:
    """Query the database for the doc count per category per geo.

    NOTE: SqlAlchemy will make a complete hash of query generation if
    columns are used in the query() call. Therefore, entire objects are
    returned.

    :param Session db: DB Session to perform query on.
    :return Query: A Query object containing the queries to perform.
    """
    subquery = (
        db.query(
            Family.family_category,
            Family.geography_id,
            func.count().label("records_count"),
        )
        .join(FamilyDocument, FamilyDocument.family_import_id == Family.import_id)
        .group_by(Family.family_category, Family.geography_id)
        .subquery()
    )
    query = (
        db.query(
            Geography.display_value,
            Geography.slug,
            Geography.value,
            subquery.c.family_category,
            subquery.c.records_count,
        )
        .join(Geography, subquery.c.geography_id == Geography.id, isouter=True)
        .order_by(Geography.display_value, subquery.c.family_category)
    )

    return query


def _to_dto(family_doc_geo_stats) -> GeographyStatsDTO:
    """Convert result set item to GeographyDTO.

    :param family_doc_geo_stats: A Tuple representing a record in the
        result set of a performed query.
    :return GeographyStatsDTO: A JSON serialisable representation of the
        Tuple record returned from the database.
    """
    return GeographyStatsDTO(
        display_name=family_doc_geo_stats.display_value,
        iso_code=family_doc_geo_stats.value,
        slug=family_doc_geo_stats.slug,
        family_counts={
            FamilyCategory.EXECUTIVE: 10,
            FamilyCategory.LEGISLATIVE: 10,
            FamilyCategory.UNFCCC: 10,
        },
    )


def get_geography_stats(db: Session) -> list[GeographyStatsDTO]:
    """Get a count of docs per category per geography for all geographies.

    :param db Session: The database session.
    :return list[GeographyStatsDTO]: A list of Geography stats objects
    """
    try:
        family_doc_geo_stats = _db_count_docs_in_category_and_geo(db).all()
    except OperationalError as e:
        _LOGGER.error(e)
        raise RepositoryError("Error querying the database for geography stats")

    if not family_doc_geo_stats:
        return []

    result = [_to_dto(fdgs) for fdgs in family_doc_geo_stats]
    return result
