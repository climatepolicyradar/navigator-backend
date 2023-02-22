from app.core.ingestion.ingest_row import IngestRow
from app.core.ingestion.utils import IngestContext, ResultType
from app.core.ingestion.validator import validate_row

from tests.core.ingestion.helpers import (
    get_ingest_row_data,
    init_for_ingest,
)


def test_validate_row__good_data(test_db):
    context = IngestContext(org_id=1, results=[])
    init_for_ingest(test_db)
    row = IngestRow.from_row(1, get_ingest_row_data(0))

    validate_row(test_db, context, row)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.OK


def test_validate_row__bad_data(test_db):
    context = IngestContext(org_id=1, results=[])
    init_for_ingest(test_db)
    row = IngestRow.from_row(1, get_ingest_row_data(0))
    row.sectors = ["fish"]

    validate_row(test_db, context, row)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR


def test_validate_row__resolvable_data(test_db):
    context = IngestContext(org_id=1, results=[])
    init_for_ingest(test_db)
    row = IngestRow.from_row(1, get_ingest_row_data(0))
    row.sectors = ["TranSPORtation"]

    validate_row(test_db, context, row)

    assert context.results
    assert len(context.results) == 1
    assert context.results[0].type == ResultType.RESOLVED
