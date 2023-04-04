import pytest
from app.core.ingestion.ingest_row import DocumentIngestRow
from app.core.ingestion.processor import ingest_document_row
from app.core.ingestion.utils import IngestContext
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


def setup_for_update(test_db):
    context = IngestContext(org_id=1, results=[])
    row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    populate_for_ingest(test_db)
    ingest_document_row(test_db, context, row)
    return context, row


def test_ingest_row__creates_missing_documents(test_db):
    context = IngestContext(org_id=1, results=[])
    row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    populate_for_ingest(test_db)
    result = ingest_document_row(test_db, context, row)
    actual_keys = set(result.keys())
    assert result["operation"] == "Create"
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
            "operation",
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


def test_ingest_row__idempotent(test_db):
    context, row = setup_for_update(test_db)

    result = ingest_document_row(test_db, context, row)
    assert len(result) == 1
    assert "operation" in result
    assert result["operation"] == "Update"


# The following tests appear in the order of the properties for DocumentIngestRow
# id: Immutable
# document_id: Immutable
# collection_name: Test
# collection_summary: Test
# document_title: Test
# family_name: Test
# family_summary: Test
# document_role: TODO
# document_variant: TODO
# geography_iso: Immutable
# documents: Immutable
# category: TODO
# sectors: METADATA
# instruments: METADATA
# frameworks: METADATA
# responses: METADATA - topics
# natural_hazards: METADATA - hazard
# keywords: TODO
# document_type: TODO
# language: Immutable
# geography: Immutable
# cpr_document_id: Immutable
# cpr_family_id: Immutable
# cpr_collection_id: Immutable
# cpr_family_slug: Not done
# cpr_document_slug: Test


def test_ingest_row__updates_collection_name(test_db):
    context, row = setup_for_update(test_db)
    row.collection_name = "changed"

    result = ingest_document_row(test_db, context, row)
    assert len(result) == 2
    assert "operation" in result
    assert result["operation"] == "Update"
    assert "collection" in result
    assert result["collection"]["title"] == "changed"


def test_ingest_row__updates_collection_summary(test_db):
    context, row = setup_for_update(test_db)
    row.collection_summary = "changed"

    result = ingest_document_row(test_db, context, row)
    assert len(result) == 2
    assert "operation" in result
    assert result["operation"] == "Update"
    assert "collection" in result
    assert result["collection"]["description"] == "changed"

    # TODO : Check db


def test_ingest_row__updates_document_title(test_db):
    context, row = setup_for_update(test_db)
    row.document_title = "changed"

    result = ingest_document_row(test_db, context, row)
    assert len(result) == 2
    assert "operation" in result
    assert result["operation"] == "Update"
    assert "physical_document" in result
    assert result["physical_document"]["title"] == "changed"

    # TODO : Check db


def test_ingest_row__updates_family_name(test_db):
    context, row = setup_for_update(test_db)
    row.family_name = "changed"

    result = ingest_document_row(test_db, context, row)
    assert len(result) == 2
    assert "operation" in result
    assert result["operation"] == "Update"
    assert "family" in result
    assert result["family"]["title"] == "changed"

    # TODO : Check db


def test_ingest_row__updates_family_summary(test_db):
    context, row = setup_for_update(test_db)
    row.family_summary = "changed"

    result = ingest_document_row(test_db, context, row)
    assert len(result) == 2
    assert "operation" in result
    assert result["operation"] == "Update"
    assert "family" in result
    assert result["family"]["description"] == "changed"

    # TODO : Check db


def test_ingest_row__updates_fd_slug(test_db):
    context, row = setup_for_update(test_db)
    row.cpr_document_slug = "changed"

    result = ingest_document_row(test_db, context, row)
    assert len(result) == 2
    assert "operation" in result
    assert result["operation"] == "Update"
    assert "family_document_slug" in result
    assert result["family_document_slug"]["name"] == "changed"

    # TODO : Check db


#
# Tests for the class DocumentIngestRow...
#


def test_IngestRow__from_row():
    ingest_row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))

    assert ingest_row
    assert ingest_row.cpr_document_id == "CCLW.executive.1001.0"
    assert ingest_row.get_first_url() == "http://place1"


def test_IngestRow__from_row_raises_when_multi_urls():
    ingest_row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(1))

    assert ingest_row
    assert ingest_row.cpr_document_id == "CCLW.executive.1002.0"
    with pytest.raises(ValueError):
        ingest_row.get_first_url()
