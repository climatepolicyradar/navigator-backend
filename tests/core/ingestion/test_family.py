from sqlalchemy.orm import Session
from app.core.ingestion.family import handle_family_from_row
from app.core.ingestion.ingest_row import DocumentIngestRow
from app.core.ingestion.physical_document import create_physical_document_from_row
from app.core.ingestion.utils import IngestOperation
from app.db.models.law_policy.family import (
    DocumentStatus,
    Family,
    FamilyCategory,
    FamilyDocument,
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
    populate_for_ingest,
)


def test_family_from_row__creates(test_db: Session):
    populate_for_ingest(test_db)
    row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    result = {}
    family = handle_family_from_row(
        test_db, IngestOperation.CREATE, None, row, org_id=1, result=result
    )

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


def test_family_from_row__updates(test_db: Session):
    populate_for_ingest(test_db)
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
    pd = create_physical_document_from_row(test_db, row, result)
    fd = FamilyDocument(
        family_import_id=FAMILY_IMPORT_ID,
        physical_document_id=pd.id,
        import_id=DOCUMENT_IMPORT_ID,
        document_status=DocumentStatus.CREATED,
    )
    test_db.add(fd)
    add_a_slug_for_family1_and_flush(test_db)
    test_db.query(Family).filter(Family.import_id == FAMILY_IMPORT_ID).one()
    test_db.flush()

    # Modify name before call
    row.family_name = "cheese"

    # Get the pre-existing doc
    handle_family_from_row(test_db, IngestOperation.UPDATE, fd, row, 1, result)
