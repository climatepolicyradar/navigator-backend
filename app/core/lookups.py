from typing import Optional, Sequence, cast

from sqlalchemy.orm import Session
from app.api.api_v1.schemas.metadata import ApplicationConfig
from app.core.organisation import get_organisation_taxonomy_by_name

from app.core.util import tree_table_to_json
from app.db.models.app.users import Organisation
from app.db.models.law_policy import Geography


def get_config(db: Session) -> ApplicationConfig:
    org_names = db.query(Organisation.name).all()

    return ApplicationConfig(
        geographies=tree_table_to_json(table=Geography, db=db),
        taxonomies={
            org_name[0]: get_organisation_taxonomy_by_name(db=db, org_name=org_name[0])
            for org_name in org_names
        },
    )


def get_countries_for_region(db: Session, region_slug: str) -> Sequence[Geography]:
    geography = db.query(Geography).filter(Geography.slug == region_slug).first()
    if geography is None:
        return []

    is_valid_region = geography.parent_id is None
    if not is_valid_region:  # either unknown or not a region
        return []

    cast(Geography, geography)

    return db.query(Geography).filter(Geography.parent_id == geography.id).all()


def get_country_by_slug(db: Session, country_slug: str) -> Optional[Geography]:
    geography = db.query(Geography).filter(Geography.slug == country_slug).first()

    if geography is None:
        return None

    # TODO: improve when we go beyond countries
    is_valid_country = geography.parent_id is not None
    if not is_valid_country:
        return None

    return geography
