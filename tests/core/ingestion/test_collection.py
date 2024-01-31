from typing import cast
from sqlalchemy.orm import Session
from app.core.ingestion.cclw.ingest_row_cclw import CCLWDocumentIngestRow
from app.core.ingestion.processor import (
    build_params_from_cclw,
    handle_cclw_collection_and_link,
)
from app.core.ingestion.utils import get_or_create
from db_client.models.law_policy.collection import (
    Collection,
    CollectionFamily,
    CollectionOrganisation,
)
from db_client.models.law_policy.family import Family

from tests.core.ingestion.helpers import (
    COLLECTION_IMPORT_ID,
    FAMILY_IMPORT_ID,
    add_a_slug_for_family1_and_flush,
    get_doc_ingest_row_data,
    populate_for_ingest,
)


def db_setup(test_db):
    populate_for_ingest(test_db)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    family = get_or_create(
        test_db,
        Family,
        import_id=FAMILY_IMPORT_ID,
        extra={
            "title": "title",
            "geography_id": 2,
            "description": "description",
            "family_category": "EXECUTIVE",
        },
    )
    add_a_slug_for_family1_and_flush(test_db)
    return row, family


def setup_with_collection(test_db):
    first_result = {}
    row, family = db_setup(test_db)

    handle_cclw_collection_and_link(
        test_db,
        build_params_from_cclw(row),
        1,
        cast(str, family.import_id),
        first_result,
    )

    return row, family


def test_handle_collection_from_row__creates(test_db: Session):
    result = {}
    row, family = db_setup(test_db)

    collection = handle_cclw_collection_and_link(
        test_db, build_params_from_cclw(row), 1, cast(str, family.import_id), result
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


def test_handle_collection_from_row__updates_name_and_summary(test_db: Session):
    row, family = setup_with_collection(test_db)

    result = {}
    row.collection_name = "new name"
    row.collection_summary = "new summary"
    collection = handle_cclw_collection_and_link(
        test_db, build_params_from_cclw(row), 1, cast(str, family.import_id), result
    )
    assert collection
    actual_keys = set(result.keys())
    expected_keys = set(
        [
            "collection",
        ]
    )
    assert actual_keys.symmetric_difference(expected_keys) == set([])

    updated_collection = (
        test_db.query(Collection).filter_by(import_id=COLLECTION_IMPORT_ID).one()
    )
    assert updated_collection is not None
    assert updated_collection.title == "new name"
    assert updated_collection.description == "new summary"


def test_handle_collection_from_row__changes_collection(test_db: Session):
    row, family = setup_with_collection(test_db)

    result = {}
    row.cpr_collection_id = f"{COLLECTION_IMPORT_ID}#2"
    row.collection_name = "new name"
    row.collection_summary = "new summary"
    collection = handle_cclw_collection_and_link(
        test_db, build_params_from_cclw(row), 1, cast(str, family.import_id), result
    )
    assert collection
    actual_keys = set(result.keys())
    expected_keys = set(
        [
            "collection_organisation",
            "collection_family",
            "collection",
        ]
    )
    assert actual_keys.symmetric_difference(expected_keys) == set([])

    # Test original collection is unchanged
    original_collection = (
        test_db.query(Collection).filter_by(import_id=COLLECTION_IMPORT_ID).one()
    )
    assert original_collection is not None
    assert original_collection.title == "Collection1"
    assert original_collection.description == "CollectionSummary1"

    # Test new collection
    new_collection = (
        test_db.query(Collection).filter_by(import_id=row.cpr_collection_id).one()
    )
    assert new_collection is not None
    assert new_collection.title == "new name"
    assert new_collection.description == "new summary"

    # Test new collection links
    link = (
        test_db.query(CollectionFamily)
        .filter_by(family_import_id=family.import_id)
        .one()
    )
    assert link is not None
    assert link.collection_import_id == row.cpr_collection_id


def test_handle_collection_from_row__removes_family_from_collection(test_db: Session):
    row, family = setup_with_collection(test_db)

    result = {}
    row.cpr_collection_id = ""
    row.collection_name = ""
    row.collection_summary = ""
    collection = handle_cclw_collection_and_link(
        test_db, build_params_from_cclw(row), 1, cast(str, family.import_id), result
    )
    test_db.commit()
    assert collection is None
    actual_keys = set(result.keys())
    expected_keys = set([])
    assert actual_keys.symmetric_difference(expected_keys) == set([])

    # Test original collection is unchanged
    original_collection = (
        test_db.query(Collection).filter_by(import_id=COLLECTION_IMPORT_ID).one()
    )
    assert original_collection is not None
    assert original_collection.title == "Collection1"
    assert original_collection.description == "CollectionSummary1"

    # Test no collection links
    link = (
        test_db.query(CollectionFamily)
        .filter_by(family_import_id=family.import_id)
        .all()
    )
    assert len(link) == 0


def test_handle_collection_from_row__ignores_na(test_db: Session):
    result = {}
    row, family = db_setup(test_db)
    row.cpr_collection_id = "n/a"

    collection = handle_cclw_collection_and_link(
        test_db, build_params_from_cclw(row), 1, cast(str, family.import_id), result
    )

    assert collection is None
    assert result == {}
