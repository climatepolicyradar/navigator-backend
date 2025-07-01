import logging
from typing import Optional, Sequence, cast

from db_client.models.dfce import Geography, Variant
from db_client.models.dfce.family import FamilyDocument, Slug
from db_client.models.document.physical_document import Language
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session

from app.errors import ValidationError
from app.models.config import ApplicationConfig
from app.service.config import get_corpus_type_config_for_allowed_corpora
from app.service.pipeline import IMPORT_ID_MATCHER
from app.service.util import tree_table_to_json

_LOGGER = logging.getLogger(__name__)


def get_config(db: Session, allowed_corpora: list[str]) -> ApplicationConfig:
    return ApplicationConfig(
        geographies=tree_table_to_json(table=Geography, db=db),
        languages={lang.language_code: lang.name for lang in db.query(Language).all()},
        document_variants=[
            variant.variant_name
            for variant in db.query(Variant).order_by(Variant.variant_name).all()
        ],
        corpus_types=get_corpus_type_config_for_allowed_corpora(db, allowed_corpora),
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


def get_countries_by_iso_codes(
    db: Session,
    country_iso_codes: Sequence[str],
) -> Sequence[Geography]:
    """
    Retrieve countries by their ISO alpha-3 codes.

    :param Session db: Database session.
    :param Sequence[str] country_iso_codes: Sequence of ISO alpha-3 country codes.
    :return Sequence[Geography]: Sequence of Geography objects for valid countries.
    """
    geographies = (
        db.query(Geography).filter(Geography.value.in_(country_iso_codes)).all()
    )
    return [geo for geo in geographies if geo.parent_id is not None]


def get_geographies_as_iso_codes_with_fallback(
    db: Session,
    geography_identifiers: Sequence[str],
) -> list[str]:
    """Temp function to handle mixed lists of ISO codes and slugs.

    Retrieve geographies by trying ISO codes first, then falling back to slugs.
    Handles mixed lists of ISO codes and slugs.

    :param Session db: Database session.
    :param Sequence[str] geography_identifiers: Sequence of geography identifiers
        (could be ISO codes or slugs).
    :return list[str]: List of ISO codes for valid countries.
    """
    if not geography_identifiers:
        return []

    found_geographies = []
    remaining_identifiers = list(geography_identifiers)

    # First attempt: try to find by ISO codes (Geography.value)
    iso_geographies = (
        db.query(Geography).filter(Geography.value.in_(geography_identifiers)).all()
    )

    if iso_geographies:
        # Found some by ISO codes
        iso_found_values = [geo.value for geo in iso_geographies]
        found_geographies.extend(
            [geo for geo in iso_geographies if geo.parent_id is not None]
        )

        # Remove found ISO codes from remaining identifiers
        remaining_identifiers = [
            id for id in geography_identifiers if id not in iso_found_values
        ]

    # Second attempt: try remaining identifiers as slugs (Geography.slug)
    if remaining_identifiers:
        slug_geographies = (
            db.query(Geography).filter(Geography.slug.in_(remaining_identifiers)).all()
        )

        if slug_geographies:
            found_geographies.extend(
                [geo for geo in slug_geographies if geo.parent_id is not None]
            )
        else:
            _LOGGER.warning(
                f"No geographies found by slugs for: {remaining_identifiers}"
            )

    # Fail on unfound identifiers
    all_found_values = [geo.value for geo in found_geographies]
    all_found_slugs = [geo.slug for geo in found_geographies]
    unfound = [
        geo_id
        for geo_id in geography_identifiers
        if geo_id not in all_found_values and geo_id not in all_found_slugs
    ]
    if unfound:
        _LOGGER.error(f"Could not find geographies for identifiers: {unfound}")
        raise ValidationError(
            f"Could not find geographies for the following identifiers: {unfound}"
        )

    return [geo.value for geo in found_geographies]


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
