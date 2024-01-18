from typing import Optional, Sequence, cast

from sqlalchemy.orm import Session
from sqlalchemy.exc import MultipleResultsFound
from app.api.api_v1.schemas.metadata import ApplicationConfig
from app.core.organisation import get_organisation_taxonomy_by_name

from app.core.util import tree_table_to_json
from app.core.validation import IMPORT_ID_MATCHER
from app.db.models.app.users import Organisation
from app.db.models.document.physical_document import Language
from app.db.models.law_policy import (
    Geography,
    FamilyDocumentRole,
    FamilyDocumentType,
    Variant,
)
from app.db.models.law_policy.family import FamilyDocument, Slug


import logging

_LOGGER = logging.getLogger(__name__)


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


def get_countries_for_slugs(
    db: Session,
    country_slugs: Sequence[str],
) -> Sequence[Geography]:
    geographies = db.query(Geography).filter(Geography.slug.in_(country_slugs)).all()

    # TODO: improve validity checking when we go beyond countries
    return [geo for geo in geographies if geo.parent_id is not None]


def get_country_by_slug(db: Session, country_slug: str) -> Optional[Geography]:
    geography = db.query(Geography).filter(Geography.slug == country_slug).first()

    if geography is None:
        return None

    # TODO: improve when we go beyond countries
    is_valid_country = geography.parent_id is not None
    if not is_valid_country:
        return None

    return geography


def get_country_slug_from_country_code(db: Session, country_code: str) -> Optional[str]:
    try:
        geography = db.query(Geography).filter_by(value=country_code).one_or_none()
    except MultipleResultsFound:
        _LOGGER.exception(
            "Multiple geographies with country code '%s' found.", country_code
        )
        return None

    if geography is None:
        return None

    geography_slug = geography.slug
    return geography_slug


def is_country_code(db: Session, country_code: str) -> bool:
    EXPECTED_GEO_CODE_LENGTH = 3
    if len(country_code) != EXPECTED_GEO_CODE_LENGTH:
        return False

    try:
        country_code = (
            db.query(Geography).filter(Geography.value == country_code).one_or_none()
        )
    except MultipleResultsFound:
        _LOGGER.exception(
            "Multiple geographies with country code '%s' found.", country_code
        )
        return False

    return bool(country_code is not None)


def get_family_document_by_import_id_or_slug(
    db: Session, import_id_or_slug: str
) -> Optional[FamilyDocument]:
    query = db.query(FamilyDocument)
    is_import_id = IMPORT_ID_MATCHER.match(import_id_or_slug) is not None
    if is_import_id:
        family_document = query.filter(
            FamilyDocument.import_id == import_id_or_slug
        ).one_or_none()
    else:
        family_document = (
            query.join(Slug, Slug.family_document_import_id == FamilyDocument.import_id)
            .filter(Slug.name == import_id_or_slug)
            .one_or_none()
        )
    return family_document
