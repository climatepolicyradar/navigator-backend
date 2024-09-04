"""Functions to support the geographies endpoint."""

import logging

from db_client.models.dfce.family import Family, FamilyGeography, FamilyStatus
from db_client.models.dfce.geography import Geography
from sqlalchemy import func
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Query, Session

from app.api.api_v1.schemas.geography import GeographyStatsDTO
from app.errors import RepositoryError

_LOGGER = logging.getLogger(__file__)


def get_geo_subquery(
    db: Session, allowed_geo_slugs=None, allowed_geo_values=None
) -> Query:

    geo_subquery = (
        db.query(
            func.min(Geography.value).label("value"),
            func.min(Geography.slug).label("slug"),
            FamilyGeography.family_import_id,
        )
        .join(FamilyGeography, FamilyGeography.geography_id == Geography.id)
        .filter(FamilyGeography.family_import_id == Family.import_id)
        .group_by(Geography.value, Geography.slug, FamilyGeography.family_import_id)
    )
    """ NOTE: This is an intermediate step to migrate to multi-geography support.
    We grab the minimum geography value for each family to use as a fallback for a single geography.
    This is because there is no rank for geography values and we need to pick one.

    This also looks dodgy as the "value" and "slug" may not match up.
    However, the browse code only uses one of these values, so it should be fine.

    Don't forget this is temporary and will be removed once multi-geography support is implemented.

    Also remember to update the pipeline query to pass these in when changing this.
    """

    if allowed_geo_slugs is not None:
        geo_subquery = geo_subquery.filter(Geography.slug.in_(allowed_geo_slugs))

    if allowed_geo_values is not None:
        geo_subquery = geo_subquery.filter(Geography.value.in_(allowed_geo_values))

    return geo_subquery.subquery("geo_subquery")


def _db_count_fams_in_category_and_geo(db: Session) -> Query:
    """
    Query the database for the fam count per category per geo.

    NOTE: SqlAlchemy will make a complete hash of query generation if
    columns are used in the query() call. Therefore, entire objects are
    returned.

    :param Session db: DB Session to perform query on.
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
        .filter(Family.family_status == FamilyStatus.PUBLISHED)
        .group_by(Family.family_category, FamilyGeography.geography_id)
        .subquery("counts")
    )

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


def get_world_map_stats(db: Session) -> list[GeographyStatsDTO]:
    """
    Get a count of fam per category per geography for all geographies.

    :param db Session: The database session.
    :return list[GeographyStatsDTO]: A list of Geography stats objects
    """
    try:
        family_geo_stats = _db_count_fams_in_category_and_geo(db).all()
    except OperationalError as e:
        _LOGGER.error(e)
        raise RepositoryError("Error querying the database for geography stats")

    if not family_geo_stats:
        return []

    result = [_to_dto(fgs) for fgs in family_geo_stats]
    return result
