from sqlalchemy.orm import Session
from app.core.ingestion.processor import ingest_unfccc_document_row
from app.core.ingestion.unfccc.ingest_row_unfccc import UNFCCCDocumentIngestRow
from app.core.ingestion.utils import UNFCCCIngestContext
from app.db.models.document.physical_document import PhysicalDocument
from app.db.models.law_policy.collection import (
    Collection,
    CollectionFamily,
    CollectionOrganisation,
)
from app.db.models.law_policy.family import (
    Family,
    FamilyDocument,
    FamilyOrganisation,
    Slug,
)
from tests.core.ingestion.helpers import (
    COLLECTION_IMPORT_ID,
    DOCUMENT_IMPORT_ID,
    DOCUMENT_TITLE,
    FAMILY_IMPORT_ID,
    SLUG_DOCUMENT_NAME,
    SLUG_FAMILY_NAME,
    get_doc_ingest_row_data,
    populate_for_ingest,
)

# FIXME: All this file needs attention


def setup_for_update(test_db):
    context = UNFCCCIngestContext()
    row = UNFCCCDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    populate_for_ingest(test_db)
    ingest_unfccc_document_row(test_db, context, row)
    return context, row


def assert_dfc(db: Session, n_docs: int, n_families: int, n_collections: int):
    assert n_docs == db.query(FamilyDocument).count()
    assert n_docs == db.query(PhysicalDocument).count()
    assert n_families == db.query(Family).count()
    assert n_collections == db.query(Collection).count()


def test_ingest_row__with_multiple_rows(test_db: Session):
    context = UNFCCCIngestContext()
    row = UNFCCCDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.cpr_family_id = "UNFCCC.family.test.1"
    row.cpr_family_slug = "fam-test-1"
    populate_for_ingest(test_db)

    # First row
    result = ingest_unfccc_document_row(test_db, context, row)
    assert 9 == len(result.keys())
    assert_dfc(test_db, 1, 1, 1)

    # Second row - adds another document to family
    row.cpr_document_id = "UNFCCC.doc.test.1"
    row.cpr_document_slug = "doc-test-1"
    result = ingest_unfccc_document_row(test_db, context, row)
    assert 3 == len(result.keys())
    assert_dfc(test_db, 2, 1, 1)

    # Third row - adds another family and document
    row.cpr_family_id = "UNFCCC.family.test.2"
    row.cpr_family_slug = "fam-test-2"
    row.cpr_document_id = "UNFCCC.doc.test.2"
    row.cpr_document_slug = "doc-test-2"
    result = ingest_unfccc_document_row(test_db, context, row)
    assert 7 == len(result.keys())
    assert_dfc(test_db, 3, 2, 1)

    # Forth - adds another document to the family
    row.cpr_document_id = "UNFCCC.doc.test.3"
    row.cpr_document_slug = "doc-test-3"
    result = ingest_unfccc_document_row(test_db, context, row)
    assert 3 == len(result.keys())
    assert_dfc(test_db, 4, 2, 1)

    # Finally change the family id of the document just added
    row.cpr_family_id = "UNFCCC.family.test.1"
    row.cpr_family_slug = "fam-test-1"
    result = ingest_unfccc_document_row(test_db, context, row)
    assert 1 == len(result.keys())
    assert_dfc(test_db, 4, 2, 1)

    # Now assert both families have correct documents
    assert (
        3
        == test_db.query(FamilyDocument)
        .filter_by(family_import_id="UNFCCC.family.test.1")
        .count()
    )
    assert (
        1
        == test_db.query(FamilyDocument)
        .filter_by(family_import_id="UNFCCC.family.test.2")
        .count()
    )

    # Now assert collection has 2 families
    assert 1 == test_db.query(Collection).count()
    assert 2 == test_db.query(CollectionFamily).count()


def test_ingest_row__creates_missing_documents(test_db: Session):
    context = UNFCCCIngestContext()
    row = UNFCCCDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    populate_for_ingest(test_db)
    result = ingest_unfccc_document_row(test_db, context, row)
    actual_keys = set(result.keys())
    expected_keys = set(
        [
            "family_slug",
            "family_organisation",
            "family",
            "physical_document",
            "family_document",
            "family_document_slug",
            "collection",
            "collection_organisation",
            "collection_family",
        ]
    )
    assert actual_keys.symmetric_difference(expected_keys) == set([])
    # Assert db objects
    assert test_db.query(Slug).filter_by(name=SLUG_FAMILY_NAME).one()
    assert (
        test_db.query(FamilyOrganisation)
        .filter_by(family_import_id=FAMILY_IMPORT_ID)
        .one()
    )
    assert test_db.query(Family).filter_by(import_id=FAMILY_IMPORT_ID).one()
    assert test_db.query(PhysicalDocument).filter_by(title=DOCUMENT_TITLE).one()
    assert test_db.query(FamilyDocument).filter_by(import_id=DOCUMENT_IMPORT_ID).one()
    assert test_db.query(Slug).filter_by(name=SLUG_DOCUMENT_NAME).one()
    assert test_db.query(Collection).filter_by(import_id=COLLECTION_IMPORT_ID).one()
    assert (
        test_db.query(CollectionOrganisation)
        .filter_by(collection_import_id=COLLECTION_IMPORT_ID)
        .one()
    )
    assert (
        test_db.query(CollectionFamily)
        .filter_by(
            collection_import_id=COLLECTION_IMPORT_ID, family_import_id=FAMILY_IMPORT_ID
        )
        .one()
    )


def test_ingest_row__idempotent(test_db: Session):
    context, row = setup_for_update(test_db)

    result = ingest_unfccc_document_row(test_db, context, row)
    assert len(result) == 0

    # Assert db objects
    assert test_db.query(Slug).filter_by(name=SLUG_FAMILY_NAME).one()
    assert (
        test_db.query(FamilyOrganisation)
        .filter_by(family_import_id=FAMILY_IMPORT_ID)
        .one()
    )
    assert test_db.query(Family).filter_by(import_id=FAMILY_IMPORT_ID).one()
    assert test_db.query(PhysicalDocument).filter_by(title=DOCUMENT_TITLE).one()
    assert test_db.query(FamilyDocument).filter_by(import_id=DOCUMENT_IMPORT_ID).one()
    assert test_db.query(Slug).filter_by(name=SLUG_DOCUMENT_NAME).one()
    assert test_db.query(Collection).filter_by(import_id=COLLECTION_IMPORT_ID).one()
    assert (
        test_db.query(CollectionOrganisation)
        .filter_by(collection_import_id=COLLECTION_IMPORT_ID)
        .one()
    )
    assert (
        test_db.query(CollectionFamily)
        .filter_by(
            collection_import_id=COLLECTION_IMPORT_ID, family_import_id=FAMILY_IMPORT_ID
        )
        .one()
    )
