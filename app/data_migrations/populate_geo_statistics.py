import json
from typing import Union

from sqlalchemy import update
from sqlalchemy.orm import Session

from db_client.models.law_policy import GeoStatistics, Geography

from .utils import has_rows


def to_float(value: str) -> Union[float, None]:
    first_str = value.split(" ")[0]
    retval = None
    try:
        retval = float(first_str)
    except ValueError:
        print(f"Unparsable for float: {first_str}")
    return retval


def populate_geo_statistics(db: Session) -> None:
    _populate_initial_geo_statistics(db)
    db.flush()
    _apply_geo_statistics_updates(db)


def _apply_geo_statistics_updates(db: Session) -> None:
    with open("app/data_migrations/data/geo_stats_updates.json") as geo_stats_file:
        geo_stats_data = json.load(geo_stats_file)
        for geo_stat in geo_stats_data:
            geography_id = (
                db.query(Geography.id)
                .filter_by(value=geo_stat["iso"], display_value=geo_stat["name"])
                .scalar()
            )
            geo_stats_id = (
                db.query(GeoStatistics.id).filter_by(geography_id=geography_id).scalar()
            )
            args = {**geo_stat}
            args["geography_id"] = geography_id
            del args["iso"]
            result = db.execute(
                update(GeoStatistics)
                .values(**args)
                .where(GeoStatistics.geography_id == geography_id)
            )

            if result.rowcount == 0:  # type: ignore
                raise ValueError(
                    f"In geo_stats id: {geo_stats_id} for geo: {geo_stat['name']}"
                )


def _populate_initial_geo_statistics(db: Session) -> None:
    """Populates the geo_statistics table with pre-defined data."""

    if has_rows(db, GeoStatistics):
        return

    # Load geo_stats data from structured data file
    with open("app/data_migrations/data/geo_stats_data.json") as geo_stats_file:
        geo_stats_data = json.load(geo_stats_file)
        for geo_stat in geo_stats_data:
            geography_id = (
                db.query(Geography.id)
                .filter_by(value=geo_stat["iso"], display_value=geo_stat["name"])
                .scalar()
            )
            args = {**geo_stat}
            args["geography_id"] = geography_id
            del args["iso"]
            db.add(GeoStatistics(**args))
