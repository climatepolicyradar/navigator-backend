"""Functions to support the geographies endpoint."""

import logging
import os
from typing import Optional, Sequence

from db_client.models.dfce.family import Family, FamilyDocument, FamilyGeography
from db_client.models.dfce.geography import Geography
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Query, Session
from sqlalchemy.types import ARRAY, String

from app.models.geography import GeographyStatsDTO
from app.repository.helpers import get_query_template

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


def count_families_per_category_in_each_geo(
    db: Session,
    allowed_corpora: Optional[list[str]],
) -> list[GeographyStatsDTO]:
    """Get a list of counts of families by category per geography.

    :param db: Database session
    :param allowed_corpora: A list of corpora IDs to filter on. If None or empty,
        returns data for all corpora.
    :return list[GeographyStatsDTO]: A list of counts of families by
        category per geography.
    """

    query_template = text(
        get_query_template(os.path.join("app", "repository", "sql", "world_map.sql"))
    )
    query_template = query_template.bindparams(
        bindparam("allowed_corpora_ids", value=allowed_corpora, type_=ARRAY(String)),
    )

    family_geo_stats = db.execute(
        query_template, {"allowed_corpora_ids": allowed_corpora}
    ).all()
    results = [_to_dto(fgs) for fgs in family_geo_stats]
    return results


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
