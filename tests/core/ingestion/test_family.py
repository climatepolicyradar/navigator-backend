import pytest
from sqlalchemy.orm import Session
from app.core.ingestion.family import migrate_family_from_row
from app.core.ingestion.ingest_row import DocumentIngestRow
from app.db.models.deprecated import Document
from app.db.models.law_policy.family import (
    Family,
    FamilyCategory,
    FamilyOrganisation,
    FamilyStatus,
    Slug,
)
from tests.core.ingestion.helpers import (
    DOCUMENT_IMPORT_ID,
    FAMILY_IMPORT_ID,
    SLUG_FAMILY_NAME,
    add_a_slug_for_family1_and_flush,
    get_doc_ingest_row_data,
    init_doc_for_migration,
    populate_for_ingest,
)


def test_family_from_row(test_db: Session):
    populate_for_ingest(test_db)
    init_doc_for_migration(test_db)
    row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    result = {}

    doc = (
        test_db.query(Document)
        .filter(Document.import_id == DOCUMENT_IMPORT_ID)
        .one_or_none()
    )

    family = migrate_family_from_row(test_db, row, doc, org_id=1, result=result)

    actual_keys = set(result.keys())
    expected_keys = set(
        [
            "family_slug",
            "family_organisation",
            "family",
            "physical_document",
            "family_document",
            "family_document_slug",
        ]
    )
    assert actual_keys.symmetric_difference(expected_keys) == set([])

    assert test_db.query(Slug).filter_by(name=SLUG_FAMILY_NAME).one()
    assert (
        test_db.query(FamilyOrganisation)
        .filter_by(family_import_id=FAMILY_IMPORT_ID)
        .one()
    )
    new_family = test_db.query(Family).filter_by(import_id=FAMILY_IMPORT_ID).one()
    assert family == new_family
    assert new_family.published_date is None
    assert new_family.last_updated_date is None


def test_family_from_row__bad_family_name(test_db: Session):
    populate_for_ingest(test_db)
    init_doc_for_migration(test_db)
    result = {}
    row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    # Pre-Add the family
    category = FamilyCategory(row.category.upper())
    test_db.add(
        Family(
            import_id=FAMILY_IMPORT_ID,
            title=row.family_name,
            geography_id=2,
            description=row.family_summary,
            family_category=category,
            family_status=FamilyStatus.PUBLISHED,
        )
    )
    add_a_slug_for_family1_and_flush(test_db)
    test_db.query(Family).filter(Family.import_id == FAMILY_IMPORT_ID).one()

    # Modify name before call
    row.family_name = "cheese"

    # Get the pre-existing doc
    doc = (
        test_db.query(Document)
        .filter(Document.import_id == DOCUMENT_IMPORT_ID)
        .one_or_none()
    )

    with pytest.raises(ValueError) as e_info:
        migrate_family_from_row(test_db, row, doc, 1, result)

    assert (
        str(e_info.value)
        == "Processing row 1 got family cheese that is different to the existing family title for family id CCLW.family.1001.0"
    )
