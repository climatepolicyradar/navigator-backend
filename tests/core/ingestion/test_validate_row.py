from app.core.ingestion.ingest_row import DocumentIngestRow
from app.core.ingestion.utils import IngestContext, ResultType
from app.core.ingestion.validator import validate_document_row
from app.core.organisation import get_organisation_taxonomy

from tests.core.ingestion.helpers import (
    get_doc_ingest_row_data,
    init_for_ingest,
)


def test_validate_row__good_data(test_db):
    context = IngestContext(org_id=1, results=[])
    init_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))

    validate_document_row(context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.OK


def test_validate_row__bad_data(test_db):
    context = IngestContext(org_id=1, results=[])
    init_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.sectors = ["fish"]

    validate_document_row(context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR


def test_validate_row__resolvable_data(test_db):
    context = IngestContext(org_id=1, results=[])
    init_for_ingest(test_db)
    _, taxonomy = get_organisation_taxonomy(test_db, context.org_id)
    row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.sectors = ["TranSPORtation"]

    validate_document_row(context=context, row=row, taxonomy=taxonomy)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.RESOLVED
