import pytest

from sqlalchemy.orm import Session
from app.core.ingestion.cclw.ingest_row_cclw import CCLWDocumentIngestRow
from app.core.ingestion.processor import ingest_cclw_document_row
from app.core.ingestion.utils import CCLWIngestContext
from app.db.models.document.physical_document import PhysicalDocument
from app.db.models.law_policy.collection import (
    Collection,
    CollectionFamily,
    CollectionOrganisation,
)
from app.db.models.law_policy.family import (
    Family,
    FamilyCategory,
    FamilyDocument,
    FamilyOrganisation,
    Slug,
)
from tests.core.ingestion.helpers import (
    BAD_MULTI_URL,
    COLLECTION_IMPORT_ID,
    DOCUMENT_IMPORT_ID,
    DOCUMENT_TITLE,
    FAMILY_IMPORT_ID,
    SLUG_DOCUMENT_NAME,
    SLUG_FAMILY_NAME,
    get_doc_ingest_row_data,
    populate_for_ingest,
)


def setup_for_update(test_db):
    context = CCLWIngestContext()
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    populate_for_ingest(test_db)
    ingest_cclw_document_row(test_db, context, row)
    return context, row


def assert_dfc(db: Session, n_docs: int, n_families: int, n_collections: int):
    assert n_docs == db.query(FamilyDocument).count()
    assert n_docs == db.query(PhysicalDocument).count()
    assert n_families == db.query(Family).count()
    assert n_collections == db.query(Collection).count()


def test_ingest_row__with_multiple_rows(test_db: Session):
    context = CCLWIngestContext()
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.cpr_family_id = "CCLW.family.test.1"
    row.cpr_family_slug = "fam-test-1"
    populate_for_ingest(test_db)

    # First row
    result = ingest_cclw_document_row(test_db, context, row)
    assert 9 == len(result.keys())
    assert_dfc(test_db, 1, 1, 1)

    # Second row - adds another document to family
    row.cpr_document_id = "CCLW.doc.test.1"
    row.cpr_document_slug = "doc-test-1"
    result = ingest_cclw_document_row(test_db, context, row)
    assert 3 == len(result.keys())
    assert_dfc(test_db, 2, 1, 1)

    # Third row - adds another family and document
    row.cpr_family_id = "CCLW.family.test.2"
    row.cpr_family_slug = "fam-test-2"
    row.cpr_document_id = "CCLW.doc.test.2"
    row.cpr_document_slug = "doc-test-2"
    result = ingest_cclw_document_row(test_db, context, row)
    assert 7 == len(result.keys())
    assert_dfc(test_db, 3, 2, 1)

    # Forth - adds another document to the family
    row.cpr_document_id = "CCLW.doc.test.3"
    row.cpr_document_slug = "doc-test-3"
    result = ingest_cclw_document_row(test_db, context, row)
    assert 3 == len(result.keys())
    assert_dfc(test_db, 4, 2, 1)

    # Finally change the family id of the document just added
    row.cpr_family_id = "CCLW.family.test.1"
    row.cpr_family_slug = "fam-test-1"
    result = ingest_cclw_document_row(test_db, context, row)
    assert 1 == len(result.keys())
    assert_dfc(test_db, 4, 2, 1)

    # Now assert both families have correct documents
    assert (
        3
        == test_db.query(FamilyDocument)
        .filter_by(family_import_id="CCLW.family.test.1")
        .count()
    )
    assert (
        1
        == test_db.query(FamilyDocument)
        .filter_by(family_import_id="CCLW.family.test.2")
        .count()
    )

    # Now assert collection has 2 families
    assert 1 == test_db.query(Collection).count()
    assert 2 == test_db.query(CollectionFamily).count()


def test_ingest_row__creates_missing_documents(test_db: Session):
    context = CCLWIngestContext()
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    populate_for_ingest(test_db)
    result = ingest_cclw_document_row(test_db, context, row)
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

    result = ingest_cclw_document_row(test_db, context, row)
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


# The following tests appear in the order of the properties for DocumentIngestRow
# id: Immutable
# document_id: Immutable
# collection_name: Test
# collection_summary: Test
# document_title: Test
# family_name: Test
# family_summary: Test
# document_role: Test
# document_variant: Test
# geography_iso: Immutable
# documents: Test
# category: Test
# sectors: METADATA
# instruments: METADATA
# frameworks: METADATA
# responses: METADATA - topics
# natural_hazards: METADATA - hazard
# keywords: METADATA
# document_type: Test
# language: Immutable
# geography: Immutable
# cpr_document_id: Immutable
# cpr_family_id: Immutable
# cpr_collection_id: Immutable
# cpr_family_slug: Not done
# cpr_document_slug: Test


def test_ingest_row__updates_collection_name(test_db: Session):
    context, row = setup_for_update(test_db)
    row.collection_name = "changed"

    result = ingest_cclw_document_row(test_db, context, row)
    assert len(result) == 1
    assert "collection" in result
    assert result["collection"]["title"] == "changed"

    # Check db
    assert 1 == test_db.query(Collection).count()
    assert (
        1
        == test_db.query(Collection)
        .filter_by(import_id=COLLECTION_IMPORT_ID, title="changed")
        .count()
    )


def test_ingest_row__updates_collection_summary(test_db: Session):
    context, row = setup_for_update(test_db)
    row.collection_summary = "changed"

    result = ingest_cclw_document_row(test_db, context, row)
    assert len(result) == 1
    assert "collection" in result
    assert result["collection"]["description"] == "changed"

    # Check db
    assert 1 == test_db.query(Collection).count()
    assert (
        1
        == test_db.query(Collection)
        .filter_by(import_id=COLLECTION_IMPORT_ID, description="changed")
        .count()
    )


def test_ingest_row__updates_document_title(test_db: Session):
    context, row = setup_for_update(test_db)
    row.document_title = "changed"

    result = ingest_cclw_document_row(test_db, context, row)
    assert len(result) == 1
    assert "physical_document" in result
    assert result["physical_document"]["title"] == "changed"

    # Check db
    assert 1 == test_db.query(PhysicalDocument).count()
    assert 1 == test_db.query(PhysicalDocument).filter_by(title="changed").count()


def test_ingest_row__updates_family_name(test_db: Session):
    context, row = setup_for_update(test_db)
    row.family_name = "changed"

    result = ingest_cclw_document_row(test_db, context, row)
    assert len(result) == 1
    assert "family" in result
    assert result["family"]["title"] == "changed"

    # Check db
    assert 1 == test_db.query(Family).count()
    assert (
        1
        == test_db.query(Family)
        .filter_by(import_id=FAMILY_IMPORT_ID, title="changed")
        .count()
    )


def test_ingest_row__updates_family_summary(test_db: Session):
    context, row = setup_for_update(test_db)
    row.family_summary = "changed"

    result = ingest_cclw_document_row(test_db, context, row)
    assert len(result) == 1
    assert "family" in result
    assert result["family"]["description"] == "changed"

    # Check db
    assert 1 == test_db.query(Family).count()
    assert (
        1
        == test_db.query(Family)
        .filter_by(import_id=FAMILY_IMPORT_ID, description="changed")
        .count()
    )


def test_ingest_row__updates_family_document_role(test_db: Session):
    context, row = setup_for_update(test_db)
    row.document_role = "ANNEX"

    result = ingest_cclw_document_row(test_db, context, row)
    assert len(result) == 1
    assert "family_document" in result
    assert result["family_document"]["document_role"] == "ANNEX"

    # Check db
    assert 1 == test_db.query(FamilyDocument).count()
    assert (
        1
        == test_db.query(FamilyDocument)
        .filter_by(import_id=DOCUMENT_IMPORT_ID, document_role="ANNEX")
        .count()
    )


def test_ingest_row__updates_family_document_variant(test_db: Session):
    context, row = setup_for_update(test_db)
    row.document_variant = "Translation"

    result = ingest_cclw_document_row(test_db, context, row)
    assert len(result) == 1
    assert "family_document" in result
    assert result["family_document"]["variant_name"] == "Translation"

    # Check db
    assert 1 == test_db.query(FamilyDocument).count()
    assert (
        1
        == test_db.query(FamilyDocument)
        .filter_by(import_id=DOCUMENT_IMPORT_ID, variant_name="Translation")
        .count()
    )


def test_ingest_row__updates_source_url(test_db: Session):
    context, row = setup_for_update(test_db)
    row.documents = "https://www.com"

    result = ingest_cclw_document_row(test_db, context, row)
    assert len(result) == 1
    assert "physical_document" in result
    assert result["physical_document"]["source_url"] == "https://www.com"

    # Check db
    assert 2 == test_db.query(PhysicalDocument).count()
    assert (
        1
        == test_db.query(PhysicalDocument)
        .filter_by(source_url="https://www.com")
        .count()
    )
    fd = (
        test_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == row.cpr_document_id)
        .one()
    )
    assert fd.physical_document.source_url == "https://www.com"


def test_ingest_row__updates_family_category(test_db: Session):
    context, row = setup_for_update(test_db)
    row.category = FamilyCategory.LEGISLATIVE

    result = ingest_cclw_document_row(test_db, context, row)
    assert len(result) == 1
    assert "family" in result
    assert result["family"]["family_category"] == "Legislative"

    # Check db
    assert 1 == test_db.query(Family).count()
    assert (
        1
        == test_db.query(Family)
        .filter_by(import_id=FAMILY_IMPORT_ID, family_category="Legislative")
        .count()
    )


def test_ingest_row__updates_family_document_type(test_db: Session):
    context, row = setup_for_update(test_db)
    row.document_type = "Edict"

    result = ingest_cclw_document_row(test_db, context, row)
    assert len(result) == 1
    assert "family_document" in result
    assert result["family_document"]["document_type"] == "Edict"

    # Check db
    assert 1 == test_db.query(FamilyDocument).count()
    assert (
        1
        == test_db.query(FamilyDocument)
        .filter_by(import_id=DOCUMENT_IMPORT_ID, document_type="Edict")
        .count()
    )


def test_ingest_row__updates_fd_slug(test_db: Session):
    context, row = setup_for_update(test_db)
    row.cpr_document_slug = "changed"

    result = ingest_cclw_document_row(test_db, context, row)
    assert len(result) == 1
    assert "family_document_slug" in result
    assert result["family_document_slug"]["name"] == "changed"

    # Check db
    assert 3 == test_db.query(Slug).count()
    assert (
        1
        == test_db.query(Slug)
        .filter_by(family_document_import_id=DOCUMENT_IMPORT_ID, name="changed")
        .count()
    )


#
# Tests for the class DocumentIngestRow...
#


def test_IngestRow__from_row():
    ingest_row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))

    assert ingest_row
    assert ingest_row.cpr_document_id == "CCLW.executive.1001.0"
    assert ingest_row.get_first_url() == "http://place1"


def test_IngestRow__from_row_raises_when_multi_urls():
    ingest_row = CCLWDocumentIngestRow.from_row(
        1, get_doc_ingest_row_data(0, contents=BAD_MULTI_URL)
    )

    assert ingest_row
    assert ingest_row.cpr_document_id == "CCLW.executive.1002.0"
    with pytest.raises(ValueError):
        ingest_row.get_first_url()
