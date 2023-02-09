from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models.deprecated import Document
from app.db.models.law_policy import (
    DocumentStatus,
    Family,
    FamilyCategory,
    FamilyDocument,
    FamilyDocumentType,
    FamilyOrganisation,
    FamilyType,
    Geography,
    Slug,
    Variant,
)

from scripts.ingest_dfc.dfc.physical_document import (
    physical_document_from_row,
)
from scripts.ingest_dfc.utils import DfcRow, get_or_create, to_dict


def family_from_row(
    db: Session,
    row: DfcRow,
    existing_document: Document,
    org_id: int,
    result: dict[str, Any],
) -> Family:
    """Create any missing Family, FamilyDocument & Associated links from the given row

    Args:
        db (Session): connection to the database.
        org_id (int): the organisation id associated with this row.
        row (DfcRow): the row built from the CSV.
        result (dict): a result dict in which to track what was created
    Raises:
        ValueError: When there is an existing family name that only differs by case
            or when the geography associated with this row cannot be found in the
            database.

    Returns:
        Family : The family that was either retrieved or created
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
        print(f"- Creating family slug for import {row.cpr_family_id}")
        family_slug = Slug(name=row.cpr_family_slug, family_import_id=family.import_id)

        db.add(family_slug)
        db.commit()
        result["family_slug"] = (to_dict(family_slug),)

        print(f"- Creating family organisation for import {row.cpr_family_id}")
        family_organisation = FamilyOrganisation(
            family_import_id=family.import_id, organisation_id=org_id
        )
        db.add(family_organisation)
        db.commit()
        result["family_organisation"] = to_dict(family_organisation)

    # FIXME: these should come from well-known values, not whatever is in the CSV
    category = get_or_create(
        db, FamilyCategory, category_name=row.category, extra={"description": ""}
    )
    family_type = get_or_create(
        db, FamilyType, type_name=row.document_type, extra={"description": ""}
    )

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
            "category_name": category.category_name,
            "family_type": family_type.type_name,
            "description": row.family_summary,
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
        db.query(FamilyDocument).filter_by(import_id=row.cpr_document_id).first()
    )
    if family_document is not None:
        # If the family document exists we can assume that the associated physical
        # document and slug have also been created
        return family_document

    physical_document = physical_document_from_row(db, row, existing_document, result)
    family_document = FamilyDocument(
        family_import_id=family.import_id,
        physical_document_id=physical_document.id,
        cdn_object=existing_document.cdn_object,
        import_id=row.cpr_document_id,
        variant_name=variant_name,
        document_status=DocumentStatus.PUBLISHED,
        document_type=document_type,
    )
    db.add(family_document)
    db.commit()

    result["family_document"] = to_dict(family_document)
    print(f"- Creating slug for FamilyDocument with import_id {row.cpr_document_id}")
    _add_family_document_slug(db, row, family_document, result)

    return family_document


def _validate_family_name(db: Session, family_name: str, import_id: str) -> bool:
    matching_family = db.query(Family).filter(Family.import_id == import_id).first()
    if matching_family is None:
        return True

    return matching_family.title.strip().lower() == family_name.strip().lower()


def _get_geography(db: Session, row: DfcRow) -> Geography:
    geography = db.query(Geography).filter(Geography.value == row.geography_iso).first()
    if geography is None:
        raise ValueError(
            f"Geography value of {row.geography_iso} does not exist in the database."
        )
    return geography


def _add_family_document_slug(
    db: Session, row: DfcRow, family_document: FamilyDocument, result: dict[str, Any]
) -> Slug:
    """Adds the slugs for the family and family_document.

    Args:
        db (Session): connection to the database.
        row (DfcRow): the row built from the CSV.
        family_document the family document associated with this row.

    Returns:
        dict : a created dictionary to describe what was created.
    """
    family_document_slug = Slug(
        name=row.cpr_document_slug,
        family_document_id=family_document.physical_document_id,
    )
    db.add(family_document_slug)
    db.commit()
    result["family_document_slug"] = {"document_slug": to_dict(family_document_slug)}
    return family_document_slug
