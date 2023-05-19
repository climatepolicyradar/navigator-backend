from app.core.ingestion.cclw.ingest_row_cclw import CCLWDocumentIngestRow
from app.core.ingestion.utils import CCLWIngestContext, ResultType
from app.core.ingestion.validator import validate_cclw_document_row
from app.core.organisation import get_organisation_taxonomy

from tests.core.ingestion.helpers import (
    get_doc_ingest_row_data,
    populate_for_ingest,
)


def test_validate_row__fails_bad_geography_iso(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.geography_iso = "XXX"

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR
    assert context.results[0].details == "Row 1: Geography XXX found in db"


def test_validate_row__fails_empty_geography_iso(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.geography_iso = ""

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR
    assert context.results[0].details == "Row 1: Geography is empty."


def test_validate_row__consistent_family_and_collection(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)
    context.results = []
    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.OK


def test_validate_row__family_name_change(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)
    context.results = []
    row.family_name = "changed"
    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR
    assert "name" in context.results[0].details
    assert context.results[0].details.startswith("Family")


def test_validate_row__family_summary_change(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)
    context.results = []
    row.family_summary = "changed"
    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR
    assert "summary" in context.results[0].details
    assert context.results[0].details.startswith("Family")


def test_validate_row__collection_name_change(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)
    context.results = []
    row.collection_name = "changed"
    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR
    assert "name" in context.results[0].details
    assert context.results[0].details.startswith("Collection")


def test_validate_row__collection_summary_change(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)
    context.results = []
    row.collection_summary = "changed"
    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR
    assert "summary" in context.results[0].details
    assert context.results[0].details.startswith("Collection")


def test_validate_row__good_data(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.OK


def test_validate_row__bad_data(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.sectors = ["fish"]

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR


def test_validate_row__resolvable_data(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.sectors = ["TranSPORtation"]

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.RESOLVED


def test_validate_row__bad_document_type(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.document_type = "fish"

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR


def test_validate_row__good_document_type(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.document_type = "Order"

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.OK


def test_validate_row__bad_document_role(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.document_role = "fish"

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR


def test_validate_row__good_document_role(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.document_role = "MAIN"

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.OK


def test_validate_row__bad_document_variant(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.document_variant = "fish"

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR


def test_validate_row__good_document_variant(test_db):
    context = CCLWIngestContext()
    populate_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.document_variant = "Translation"

    validate_cclw_document_row(test_db, context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.OK
