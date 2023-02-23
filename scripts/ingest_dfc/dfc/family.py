from typing import Any, cast

from sqlalchemy.orm import Session

from app.db.models.deprecated import Document
from app.db.models.law_policy import (
    DocumentStatus,
    FamilyCategory,
    Family,
    FamilyDocument,
    FamilyDocumentType,
    FamilyOrganisation,
    Geography,
    Slug,
    Variant,
)
from app.db.models.law_policy.family import FamilyStatus
from scripts.ingest_dfc.dfc.metadata import add_metadata
from scripts.ingest_dfc.dfc.organisation import get_organisation_taxonomy
from scripts.ingest_dfc.dfc.physical_document import physical_document_from_row
from scripts.ingest_dfc.utils import DfcRow, get_or_create, to_dict


def family_from_row(
    db: Session,
    row: DfcRow,
    existing_document: Document,
    org_id: int,
    result: dict[str, Any],
) -> Family:
    """
    Create any missing Family, FamilyDocument & Associated links from the given row

    :param [Session] db: connection to the database.
    :param [int] org_id: the organisation id associated with this row.
    :param [DfcRow] row: the row built from the CSV.
    :param [dict[str, Any]] result: a result dict in which to track what was created
    :raises [ValueError]: When there is an existing family name that only differs by
        case or when the geography associated with this row cannot be found in the
        database.
    :return [Family]: The family that was either retrieved or created
    """
    # GET OR CREATE FAMILY
    family = _maybe_create_family(db, row, org_id, result)

    # GET OR CREATE FAMILY DOCUMENT
    _maybe_create_family_document(db, row, family, existing_document, result)

    return family


def _maybe_create_family(
    db: Session, row: DfcRow, org_id: int, result: dict[str, Any]
) -> Family:
    def _create_family_links(family: Family):
        print(f"- Creating family slug for import {family.import_id}")
        family_slug = Slug(name=row.cpr_family_slug, family_import_id=family.import_id)

        db.add(family_slug)
        result["family_slug"] = (to_dict(family_slug),)

        print(f"- Creating family organisation for import {row.cpr_family_id}")
        family_organisation = FamilyOrganisation(
            family_import_id=family.import_id, organisation_id=org_id
        )
        db.add(family_organisation)
        result["family_organisation"] = to_dict(family_organisation)

        id, taxonomy = get_organisation_taxonomy(db, org_id)
        add_metadata(db, cast(str, family.import_id), taxonomy, id, row)

    category = FamilyCategory(row.category.upper())

    # GET GEOGRAPHY
    print(f"- Getting Geography for {row.geography_iso}")
    geography = _get_geography(db, row)

    if not _validate_family_name(db, row.family_name, row.cpr_family_id):
        raise ValueError(
            f"Processing row {row.row_number} got family {row.family_name} "
            "that is different to the existing family title for family id "
            f"{row.cpr_family_id}"
        )
    family = get_or_create(
        db,
        Family,
        import_id=row.cpr_family_id,
        extra={
            "title": row.family_name,
            "geography_id": geography.id,
            "description": row.family_summary,
            "family_category": category,
            "family_status": FamilyStatus.PUBLISHED,
        },
        after_create=_create_family_links,
    )
    result["family"] = to_dict(family)
    return family


def _maybe_create_family_document(
    db: Session,
    row: DfcRow,
    family: Family,
    existing_document: Document,
    result: dict[str, Any],
) -> FamilyDocument:
    print(f"- Creating family document for import {row.cpr_document_id}")

    # FIXME: these should come from well-known values, not whatever is in the CSV
    variant_name = get_or_create(
        db, Variant, variant_name=row.document_role, extra={"description": ""}
    ).variant_name
    document_type = get_or_create(
        db, FamilyDocumentType, name=row.document_type, extra={"description": ""}
    ).name

    family_document = (
        db.query(FamilyDocument).filter_by(import_id=row.cpr_document_id).one_or_none()
    )
    if family_document is not None:
        # If the family document exists we can assume that the associated physical
        # document and slug have also been created
        return family_document

    physical_document = physical_document_from_row(db, row, existing_document, result)
    family_document = FamilyDocument(
        family_import_id=family.import_id,
        physical_document_id=physical_document.id,
        import_id=row.cpr_document_id,
        variant_name=variant_name,
        document_status=DocumentStatus.PUBLISHED,
        document_type=document_type,
    )
    db.add(family_document)
    db.flush()

    result["family_document"] = to_dict(family_document)
    print(f"- Creating slug for FamilyDocument with import_id {row.cpr_document_id}")
    _add_family_document_slug(db, row, family_document, result)

    return family_document


def _validate_family_name(db: Session, family_name: str, import_id: str) -> bool:
    matching_family = (
        db.query(Family).filter(Family.import_id == import_id).one_or_none()
    )
    if matching_family is None:
        return True

    return matching_family.title.strip().lower() == family_name.strip().lower()


def _get_geography(db: Session, row: DfcRow) -> Geography:
    geography = (
        db.query(Geography).filter(Geography.value == row.geography_iso).one_or_none()
    )
    if geography is None:
        raise ValueError(
            f"Geography value of {row.geography_iso} does not exist in the database."
        )
    return geography


def _add_family_document_slug(
    db: Session, row: DfcRow, family_document: FamilyDocument, result: dict[str, Any]
) -> Slug:
    """
    Adds the slugs for the family and family_document.

    :param [Session] db: connection to the database.
    :param [DfcRow] row: the row built from the CSV.
    :param [FamilyDocument] family_document: family document associated with this row.
    :param [dict[str, Any]] result: a dictionary in which to record what was created.
    :return [Slug]: the created slug object
    """
    family_document_slug = Slug(
        name=row.cpr_document_slug,
        family_document_import_id=family_document.import_id,
    )
    db.add(family_document_slug)
    db.flush()
    result["family_document_slug"] = {"document_slug": to_dict(family_document_slug)}
    return family_document_slug
