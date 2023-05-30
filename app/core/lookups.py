from typing import Optional, Sequence, cast

from sqlalchemy.orm import Session
from app.api.api_v1.schemas.metadata import ApplicationConfig
from app.core.organisation import get_organisation_taxonomy_by_name

from app.core.util import tree_table_to_json
from app.db.models.app.users import Organisation
from app.db.models.document.physical_document import Language
from app.db.models.law_policy import (
    Geography,
    FamilyDocumentRole,
    FamilyDocumentType,
    Variant,
)


def get_config(db: Session) -> ApplicationConfig:
    return ApplicationConfig(
        geographies=tree_table_to_json(table=Geography, db=db),
        taxonomies={
            org.name: get_organisation_taxonomy_by_name(db=db, org_name=org.name)
            for org in db.query(Organisation).all()
        },
        languages={lang.language_code: lang.name for lang in db.query(Language).all()},
        document_roles=[
            doc_role.name
            for doc_role in db.query(FamilyDocumentRole)
            .order_by(FamilyDocumentRole.name)
            .all()
        ],
        document_types=[
            doc_type.name
            for doc_type in db.query(FamilyDocumentType)
            .order_by(FamilyDocumentType.name)
            .all()
        ],
        document_variants=[
            variant.variant_name
            for variant in db.query(Variant).order_by(Variant.variant_name).all()
        ],
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
