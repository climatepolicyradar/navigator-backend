"""Functions to support the geographies endpoint."""

import logging
from typing import Optional

from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.errors import RepositoryError, ValidationError
from app.models.geography import GeographyStatsDTO
from app.repository.geography import count_families_per_category_in_each_geo

_LOGGER = logging.getLogger(__file__)


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
    if allowed_corpora is None or allowed_corpora == []:
        raise ValidationError("No allowed corpora provided")

    try:
        family_geo_stats = count_families_per_category_in_each_geo(db, allowed_corpora)
    except OperationalError as e:
        _LOGGER.error(e)
        raise RepositoryError("Error querying the database for geography stats")

    if not family_geo_stats:
        return []
    return family_geo_stats
