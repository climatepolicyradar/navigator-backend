from typing import cast
from sqlalchemy.orm import Session
from app.core.ingestion.collection import collection_from_row
from app.core.ingestion.ingest_row import DocumentIngestRow
from app.core.ingestion.utils import get_or_create
from app.db.models.law_policy.collection import (
    Collection,
    CollectionFamily,
    CollectionOrganisation,
)
from app.db.models.law_policy.family import Family

from tests.core.ingestion.helpers import (
    COLLECTION_IMPORT_ID,
    FAMILY_IMPORT_ID,
    add_a_slug_for_family1_and_flush,
    get_doc_ingest_row_data,
    init_doc_for_migration,
    populate_for_ingest,
)


def test_collection_from_row(test_db: Session):
    populate_for_ingest(test_db)
    init_doc_for_migration(test_db)
    row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    result = {}
    family = get_or_create(
        test_db,
        Family,
        import_id=FAMILY_IMPORT_ID,
        extra={
            "title": "title",
            "geography_id": 2,
            "description": "description",
            "family_category": "EXECUTIVE",
            "family_status": "Published",
        },
    )
    add_a_slug_for_family1_and_flush(test_db)

    collection = collection_from_row(
        test_db, row, 1, cast(str, family.import_id), result
    )
    assert collection
    actual_keys = set(result.keys())
    expected_keys = set(
        [
            "collection",
            "collection_organisation",
            "collection_family",
        ]
    )
    assert actual_keys.symmetric_difference(expected_keys) == set([])

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
