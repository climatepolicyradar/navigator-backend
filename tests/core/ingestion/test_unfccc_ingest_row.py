import copy
from datetime import datetime
from sqlalchemy.orm import Session
from app.core.ingestion.processor import (
    ingest_collection_row,
    ingest_unfccc_document_row,
    initialise_context,
)
from app.core.ingestion.unfccc.ingest_row_unfccc import (
    CollectionIngestRow,
    UNFCCCDocumentIngestRow,
)
from app.db.models.law_policy.collection import CollectionFamily, CollectionOrganisation
from app.db.models.law_policy.family import Family
from app.db.models.law_policy.geography import GEO_INTERNATIONAL, GEO_NONE, Geography

from tests.core.ingestion.helpers import (
    populate_for_ingest,
)
from app.db.models.law_policy import Collection


DOC_ROW = UNFCCCDocumentIngestRow(
    row_number=1,
    category="UNFCCC",
    submission_type="Plan",
    family_name="family_name",
    document_title="document_title",
    documents="documents",
    author="author",
    author_type="Party",
    geography="GBR",
    geography_iso="GBR",
    date=datetime.now(),
    document_role="MAIN",
    document_variant="Original Language",
    language=["en"],
    cpr_collection_id=["id1"],
    cpr_document_id="cpr_document_id",
    cpr_family_id="cpr_family_id",
    cpr_family_slug="cpr_family_slug",
    cpr_document_slug="cpr_document_slug",
    cpr_document_status="PUBLISHED",
    download_url="download_url",
)


def test_ingest_single_collection_and_document(test_db: Session):
    populate_for_ingest(test_db)
    test_db.commit()
    context = initialise_context(test_db, "UNFCCC")

    # Act - create collection
    collection_row = CollectionIngestRow(
        row_number=1,
        cpr_collection_id="id1",
        collection_name="collection-title",
        collection_summary="collection-description",
    )
    result = ingest_collection_row(test_db, context, collection_row)

    # Assert we have created a collection and a link to the org
    assert len(result) == 2
    assert "collection" in result.keys()
    assert "collection_organisation" in result.keys()
    assert test_db.query(Collection).filter(Collection.import_id == "id1").one()
    assert (
        test_db.query(CollectionOrganisation)
        .filter(CollectionOrganisation.collection_import_id == "id1")
        .one()
    )

    # Act - create document
    document_row = copy.deepcopy(DOC_ROW)

    result = ingest_unfccc_document_row(test_db, context, document_row)
    assert len(result) == 7


def test_ingest_two_collections_and_document(test_db: Session):
    populate_for_ingest(test_db)
    test_db.commit()
    context = initialise_context(test_db, "UNFCCC")

    # Act - create collections
    collection_row = CollectionIngestRow(
        row_number=1,
        cpr_collection_id="id1",
        collection_name="collection-title",
        collection_summary="collection-description",
    )
    ingest_collection_row(test_db, context, collection_row)
    collection_row2 = CollectionIngestRow(
        row_number=2,
        cpr_collection_id="id2",
        collection_name="collection-title2",
        collection_summary="collection-description2",
    )
    ingest_collection_row(test_db, context, collection_row2)
    assert 2 == test_db.query(Collection).count()

    # Act - create document
    document_row = copy.deepcopy(DOC_ROW)
    document_row.cpr_collection_id = ["id1", "id2"]
    result = ingest_unfccc_document_row(test_db, context, document_row)

    assert len(result) == 7
    assert (
        test_db.query(CollectionOrganisation)
        .filter(CollectionOrganisation.collection_import_id == "id1")
        .one()
    )
    assert (
        test_db.query(CollectionOrganisation)
        .filter(CollectionOrganisation.collection_import_id == "id2")
        .one()
    )
    assert (
        test_db.query(CollectionFamily)
        .filter(CollectionFamily.collection_import_id == "id1")
        .filter(CollectionFamily.family_import_id == "cpr_family_id")
        .one()
    )
    assert (
        test_db.query(CollectionFamily)
        .filter(CollectionFamily.collection_import_id == "id2")
        .filter(CollectionFamily.family_import_id == "cpr_family_id")
        .one()
    )


def test_ingest_blank_geo(test_db: Session):
    populate_for_ingest(test_db)
    test_db.commit()
    context = initialise_context(test_db, "UNFCCC")

    # Arrange - create collection
    collection_row = CollectionIngestRow(
        row_number=1,
        cpr_collection_id="id1",
        collection_name="collection-title",
        collection_summary="collection-description",
    )
    result = ingest_collection_row(test_db, context, collection_row)

    # Act - create document
    document_row = copy.deepcopy(DOC_ROW)
    document_row.geography_iso = ""

    result = ingest_unfccc_document_row(test_db, context, document_row)
    assert len(result) == 7

    assert 1 == test_db.query(Family).count()
    family = test_db.query(Family).first()
    assert family
    assert family.geography_id
    geo = test_db.get(Geography, family.geography_id)
    no_geo = test_db.query(Geography).filter(Geography.value == GEO_NONE).one()
    assert geo == no_geo


def test_ingest_international_geo(test_db: Session):
    populate_for_ingest(test_db)
    test_db.commit()
    context = initialise_context(test_db, "UNFCCC")

    # Arrange - create collection
    collection_row = CollectionIngestRow(
        row_number=1,
        cpr_collection_id="id1",
        collection_name="collection-title",
        collection_summary="collection-description",
    )
    result = ingest_collection_row(test_db, context, collection_row)

    # Act - create document
    document_row = copy.deepcopy(DOC_ROW)
    document_row.geography_iso = "INT"

    result = ingest_unfccc_document_row(test_db, context, document_row)
    test_db.commit()
    assert len(result) == 7

    assert 1 == test_db.query(Family).count()
    family = test_db.query(Family).first()
    assert family
    assert family.geography_id
    geo = test_db.get(Geography, family.geography_id)
    international = (
        test_db.query(Geography).filter(Geography.value == GEO_INTERNATIONAL).one()
    )
    assert geo == international
