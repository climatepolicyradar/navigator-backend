import pytest
from sqlalchemy.orm import Session
from app.core.ingestion.ingest_row import IngestRow
from app.core.ingestion.processor import ingest_row
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
    get_ingest_row_data,
    init_for_ingest,
)


def test_ingest_row__skips_missing_documents(test_db):
    context = IngestContext(org_id=1, results=[])
    row = IngestRow.from_row(1, get_ingest_row_data(0))
    result = ingest_row(test_db, context, row)
    assert len(result.keys()) == 1
    assert result["existing_document"] is False


def test_ingest_row__migrates_existing_documents(test_db: Session):
    context = IngestContext(org_id=1, results=[])
    init_for_ingest(test_db)

    row = IngestRow.from_row(1, get_ingest_row_data(0))
    result = ingest_row(test_db, context, row)
    test_db.flush()

    # Assert keys for created db objects
    assert result["existing_document"] is True
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
            "existing_document",
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


# Tests for the class IngestRow...


def test_IngestRow__from_row():
    ingest_row = IngestRow.from_row(1, get_ingest_row_data(0))

    assert ingest_row
    assert ingest_row.cpr_document_id == "CCLW.executive.1001.0"
    assert ingest_row.get_first_url() == "http://place1"


def test_IngestRow__from_row_raises_when_multi_urls():
    ingest_row = IngestRow.from_row(1, get_ingest_row_data(1))

    assert ingest_row
    assert ingest_row.cpr_document_id == "CCLW.executive.1002.0"
    with pytest.raises(ValueError):
        ingest_row.get_first_url()
