from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.core.ingestion.cclw.ingest_row_cclw import CCLWDocumentIngestRow
from app.core.ingestion.family import (
    handle_family_document_from_params,
    handle_family_from_params,
)
from app.core.ingestion.physical_document import create_physical_document_from_params
from app.core.ingestion.processor import build_params_from_cclw
from app.db.models.law_policy.family import (
    DocumentStatus,
    Family,
    FamilyCategory,
    FamilyDocument,
    FamilyOrganisation,
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
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    result = {}
    family = handle_family_from_params(
        test_db, build_params_from_cclw(row), org_id=1, result=result
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
        test_db.query(Slug.created).filter_by(name=SLUG_FAMILY_NAME).scalar()
        is not None
    )
    assert (
        test_db.query(FamilyOrganisation)
        .filter_by(family_import_id=FAMILY_IMPORT_ID)
        .one()
    )
    new_family = test_db.query(Family).filter_by(import_id=FAMILY_IMPORT_ID).one()
    assert family == new_family
    assert new_family.published_date is None
    assert new_family.last_updated_date is None
    assert new_family.created is not None
    assert new_family.last_modified is not None
    assert new_family.created == new_family.last_modified


def test_family_from_row__updates(test_db: Session, patch_current_time):
    populate_for_ingest(test_db)
    with patch_current_time("2000-01-01 00:00:00.0Z", Family):
        result = {}
        row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
        # Pre-Add the family
        category = FamilyCategory(row.category.upper())
        test_db.add(
            Family(
                import_id=FAMILY_IMPORT_ID,
                title=row.family_name,
                geography_id=2,
                description=row.family_summary,
                family_category=category,
            )
        )
        pd = create_physical_document_from_params(
            test_db, build_params_from_cclw(row), result
        )
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
    family = handle_family_from_params(test_db, build_params_from_cclw(row), 1, result)

    assert family.title == "cheese"
    assert 1 == test_db.query(Family).filter_by(title="cheese").count()
    assert family.created is not None
    assert family.created == datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
    assert family.last_modified is not None
    assert family.created < family.last_modified

    slugs = (
        test_db.query(Slug)
        .filter_by(family_import_id=FAMILY_IMPORT_ID)
        .order_by(Slug.created)
        .all()
    )
    assert len(slugs) == 2
    assert slugs[0].created < slugs[1].created


def test_family_document_from_row__creates(test_db: Session):
    populate_for_ingest(test_db)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    family = add_a_family(test_db)
    result = {}
    family_document = handle_family_document_from_params(
        test_db, build_params_from_cclw(row), family, result=result
    )

    actual_keys = set(result.keys())
    expected_keys = set(
        [
            "physical_document",
            "family_document",
            "family_document_slug",
        ]
    )
    assert actual_keys.symmetric_difference(expected_keys) == set([])

    db_family_doc = (
        test_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == DOCUMENT_IMPORT_ID)
        .one()
    )
    assert db_family_doc == family_document
    assert db_family_doc.physical_document.title == row.document_title
    assert db_family_doc.created is not None
    assert db_family_doc.last_modified is not None
    assert db_family_doc.created == db_family_doc.last_modified

    assert (
        test_db.query(Slug.created)
        .filter_by(family_document_import_id=DOCUMENT_IMPORT_ID)
        .scalar()
        is not None
    )


def test_family_document_from_row__updates(test_db: Session, patch_current_time):
    populate_for_ingest(test_db)
    with patch_current_time("2000-01-01 00:00:00.0Z", FamilyDocument):
        family = add_a_family(test_db)
        row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
        result = {}
        handle_family_document_from_params(
            test_db, build_params_from_cclw(row), family, result=result
        )

    result = {}
    row.document_title = "test-title"
    row.document_role = "PRESS RELEASE"
    family_document = handle_family_document_from_params(
        test_db, build_params_from_cclw(row), family, result=result
    )

    assert family_document.created is not None
    assert family_document.created == datetime(
        2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc
    )
    assert family_document.last_modified is not None
    assert family_document.created < family_document.last_modified

    actual_keys = set(result.keys())
    expected_keys = set(
        [
            "physical_document",
            "family_document",
        ]
    )
    assert actual_keys.symmetric_difference(expected_keys) == set([])

    db_family_doc = (
        test_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == DOCUMENT_IMPORT_ID)
        .one()
    )
    assert db_family_doc == family_document
    assert db_family_doc.physical_document.title == "test-title"
    assert db_family_doc.document_role == "PRESS RELEASE"
    assert db_family_doc.created == datetime(
        2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc
    )
    assert db_family_doc.last_modified is not None
    assert db_family_doc.created < db_family_doc.last_modified

    slugs = (
        test_db.query(Slug)
        .filter_by(family_document_import_id=DOCUMENT_IMPORT_ID)
        .order_by(Slug.created)
        .all()
    )
    assert len(slugs) == 2
    assert slugs[0].created < slugs[1].created


def test_family_document_from_row__updates_status(test_db: Session, patch_current_time):
    populate_for_ingest(test_db)
    with patch_current_time("2000-01-01 00:00:00.0Z", FamilyDocument):
        row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
        family = add_a_family(test_db)
        result = {}
        handle_family_document_from_params(
            test_db, build_params_from_cclw(row), family, result=result
        )

    result = {}
    row.cpr_document_status = "DELETED"
    family_document = handle_family_document_from_params(
        test_db, build_params_from_cclw(row), family, result=result
    )

    assert list(result.keys()) == ["family_document"]

    db_family_doc = (
        test_db.query(FamilyDocument)
        .filter(FamilyDocument.import_id == DOCUMENT_IMPORT_ID)
        .one()
    )
    assert db_family_doc == family_document
    assert db_family_doc.document_status == "DELETED"
    assert db_family_doc.created == datetime(
        2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc
    )
    assert db_family_doc.last_modified is not None
    assert db_family_doc.created < db_family_doc.last_modified


def add_a_family(test_db: Session) -> Family:
    family = Family(
        import_id=FAMILY_IMPORT_ID,
        title="title",
        geography_id=2,
        description="description",
        family_category="EXECUTIVE",
    )
    test_db.add(family)
    test_db.flush()
    return family
