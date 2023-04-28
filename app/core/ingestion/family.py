from typing import Any, Optional, cast

from sqlalchemy.orm import Session
from app.core.ingestion.ingest_row import DocumentIngestRow
from app.core.ingestion.metadata import add_metadata
from app.core.organisation import get_organisation_taxonomy
from app.core.ingestion.physical_document import create_physical_document_from_row
from app.core.ingestion.utils import (
    create,
    get_or_create,
    to_dict,
    update_if_changed,
)

from app.db.models.law_policy import (
    DocumentStatus,
    FamilyCategory,
    Family,
    FamilyDocument,
    FamilyOrganisation,
    FamilyStatus,
    Geography,
    Slug,
)


def handle_family_from_row(
    db: Session,
    row: DocumentIngestRow,
    org_id: int,
    result: dict[str, Any],
) -> Family:
    """
    Create any Family + other entities and links from the row found in the db.

    :param [Session] db: connection to the database.
    :param [int] org_id: the organisation id associated with this row.
    :param [IngestRow] row: the row built from the CSV.
    :param [dict[str, Any]] result: a result dict in which to track what was created
    :raises [ValueError]: When there is an existing family name that only differs by
        case or when the geography associated with this row cannot be found in the
        database.
    :return [Family]: The family that was either retrieved or created
    """
    family = _operate_on_family(db, row, org_id, result)

    handle_family_document_from_row(db, row, family, result)

    return family


def _after_create_family(
    db: Session, row: DocumentIngestRow, org_id: int, result: dict[str, Any]
):
    def _create_family_links(family: Family):
        family_slug = Slug(name=row.cpr_family_slug, family_import_id=family.import_id)

        db.add(family_slug)
        result["family_slug"] = (to_dict(family_slug),)

        family_organisation = FamilyOrganisation(
            family_import_id=family.import_id, organisation_id=org_id
        )
        db.add(family_organisation)
        result["family_organisation"] = to_dict(family_organisation)

        id, taxonomy = get_organisation_taxonomy(db, org_id)
        add_metadata(db, cast(str, family.import_id), taxonomy, id, row)

    return _create_family_links


def _operate_on_family(
    db: Session,
    row: DocumentIngestRow,
    org_id: int,
    result: dict[str, Any],
) -> Family:
    category = FamilyCategory(row.category.upper())

    geography = _get_geography(db, row)
    extra = {
        "title": row.family_name,
        "geography_id": geography.id,
        "description": row.family_summary,
        "family_category": category,
    }

    family = (
        db.query(Family).filter(Family.import_id == row.cpr_family_id).one_or_none()
    )

    if family is None:
        family = create(
            db,
            Family,
            import_id=row.cpr_family_id,
            extra={**extra, "family_status": FamilyStatus.CREATED},
            after_create=_after_create_family(db, row, org_id, result),
        )
        result["family"] = to_dict(family)
    else:
        updated = {}

        update_if_changed(updated, "title", row.family_name, family)
        update_if_changed(updated, "description", row.family_summary, family)
        update_if_changed(updated, "family_category", category, family)

        if len(updated) > 0:
            db.add(family)
            db.flush()
            result["family"] = updated

    return family


def handle_family_document_from_row(
    db: Session,
    row: DocumentIngestRow,
    family: Family,
    result: dict[str, Any],
) -> FamilyDocument:
    def none_if_empty(data: str) -> Optional[str]:
        return data if data != "" else None

    # NOTE: op is determined by existence or otherwise of FamilyDocument
    family_document = (
        db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == row.cpr_document_id)
        .one_or_none()
    )

    # If the family document exists we can assume that the associated physical
    # document and slug have also been created
    if family_document is not None:
        updated = {}
        update_if_changed(
            updated,
            "family_import_id",
            none_if_empty(row.cpr_family_id),
            family_document,
        )
        update_if_changed(
            updated,
            "document_type",
            none_if_empty(row.document_type),
            family_document,
        )
        update_if_changed(
            updated,
            "document_role",
            none_if_empty(row.document_role),
            family_document,
        )
        update_if_changed(
            updated,
            "variant_name",
            none_if_empty(row.document_variant),
            family_document,
        )
        if len(updated) > 0:
            db.add(family_document)
            db.flush()
            result["family_document"] = updated

        # Now the physical document
        updated = {}

        # If source_url changed then create a new physical_document
        if row.get_first_url() != family_document.physical_document.source_url:
            physical_document = create_physical_document_from_row(db, row, result)
            family_document.physical_document = physical_document
        else:
            update_if_changed(
                updated,
                "title",
                row.document_title,
                family_document.physical_document,
            )

        if len(updated) > 0:
            db.add(family_document.physical_document)
            db.flush()
            result["physical_document"] = updated

        # Check if slug has changed
        existing_slug = (
            db.query(Slug).filter(Slug.name == row.cpr_document_slug).one_or_none()
        )
        if existing_slug is None:
            _add_family_document_slug(db, row, family_document, result)
    else:
        physical_document = create_physical_document_from_row(db, row, result)
        family_document = FamilyDocument(
            family_import_id=family.import_id,
            physical_document_id=physical_document.id,
            import_id=row.cpr_document_id,
            variant_name=none_if_empty(row.document_variant),
            document_status=DocumentStatus.PUBLISHED,
            document_type=none_if_empty(row.document_type),
            document_role=none_if_empty(row.document_role),
        )

        db.add(family_document)
        db.flush()
        result["family_document"] = to_dict(family_document)
        _add_family_document_slug(db, row, family_document, result)

    return family_document


def _get_geography(db: Session, row: DocumentIngestRow) -> Geography:
    geography = (
        db.query(Geography).filter(Geography.value == row.geography_iso).one_or_none()
    )
    if geography is None:
        raise ValueError(
            f"Geography value of {row.geography_iso} does not exist in the database."
        )
    return geography


def _add_family_document_slug(
    db: Session,
    row: DocumentIngestRow,
    family_document: FamilyDocument,
    result: dict[str, Any],
) -> Slug:
    """
    Adds the slugs for the family and family_document.

    :param [Session] db: connection to the database.
    :param [IngestRow] row: the row built from the CSV.
    :param [FamilyDocument] family_document: family document associated with this row.
    :param [dict[str, Any]] result: a dictionary in which to record what was created.
    :return [Slug]: the created slug object
    """
    family_document_slug = get_or_create(
        db,
        Slug,
        name=row.cpr_document_slug,
        family_document_import_id=family_document.import_id,
    )
    result["family_document_slug"] = to_dict(family_document_slug)
    return family_document_slug
