import logging
from typing import Optional, Sequence, cast

from db_client.models.dfce import Geography, Variant
from db_client.models.dfce.family import FamilyDocument, Slug
from db_client.models.document.physical_document import Language
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session

from app.models.metadata import ApplicationConfig
from app.repository.organisation import get_organisation_config, get_organisations
from app.service.pipeline import IMPORT_ID_MATCHER
from app.service.util import tree_table_to_json

_LOGGER = logging.getLogger(__name__)


def get_config(db: Session, allowed_corpora: list[str]) -> ApplicationConfig:
    # First get the CCLW stats
    return ApplicationConfig(
        geographies=tree_table_to_json(table=Geography, db=db),
        organisations={
            cast(str, org.name): get_organisation_config(db, org)
            for org in get_organisations(db, allowed_corpora)
        },
        languages={lang.language_code: lang.name for lang in db.query(Language).all()},
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


def doc_type_from_family_document_metadata(family_document: FamilyDocument) -> str:
    """Retrieves the document type from FamilyDocument metadata

    If the field is missing, empty or None, returns an empty string
    Will also return at empty string if the first value of metadata is None
    """
    doctype: list = family_document.valid_metadata.get("type")
    if not doctype or len(doctype) == 0:
        return ""
    return cast(str, doctype[0] or "")
