import json
from typing import cast

from slugify import slugify
from sqlalchemy.orm import Session

from db_client.models.law_policy import Geography
from db_client.models.law_policy.geography import (
    CPR_DEFINED_GEOS,
    GEO_OTHER,
    GeoStatistics,
)

from .utils import has_rows, load_tree


def _add_geo_slugs(geo_tree: list[dict[str, dict]]):
    for entry in geo_tree:
        data = entry["node"]
        data["slug"] = slugify(data["display_value"], separator="-")

        child_nodes = cast(list[dict[str, dict]], entry["children"])
        if child_nodes:
            _add_geo_slugs(child_nodes)


def remove_old_international_geo(db: Session) -> None:
    db_international = (
        db.query(Geography).filter(Geography.value == "INT").one_or_none()
    )
    if db_international is not None:
        db_stats = (
            db.query(GeoStatistics)
            .filter(GeoStatistics.geography_id == db_international.id)
            .one_or_none()
        )
        if db_stats is not None:
            db.delete(db_stats)
            db.flush()
        db.delete(db_international)
        db.flush()


def populate_geography(db: Session) -> None:
    """Populates the geography table with pre-defined data."""

    geo_populated = has_rows(db, Geography)
    # First ensure our defined entries are present
    remove_old_international_geo(db)

    # Add the Other region
    other = db.query(Geography).filter(Geography.value == GEO_OTHER).one_or_none()
    if other is None:
        other = Geography(
            display_value=GEO_OTHER,
            slug=slugify(GEO_OTHER),
            value=GEO_OTHER,
            type="ISO-3166 CPR Extension",
        )
        db.add(other)
        db.flush()

    # Add the CPR geo definitions in Other
    for value, description in CPR_DEFINED_GEOS.items():
        db_geo = db.query(Geography).filter(Geography.value == value).one_or_none()
        if db_geo is None:
            db.add(
                Geography(
                    display_value=description,
                    slug=slugify(value),
                    value=value,
                    type="ISO-3166 CPR Extension",
                    parent_id=other.id,
                )
            )

    if geo_populated:
        return

    with open("app/data_migrations/data/geography_data.json") as geo_data_file:
        geo_data = json.loads(geo_data_file.read())
        _add_geo_slugs(geo_data)
        load_tree(db, Geography, geo_data)
