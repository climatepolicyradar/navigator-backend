from app.core.ingestion.ingest_row_cclw import CCLWDocumentIngestRow
from app.core.ingestion.processor import get_dfc_ingestor
from app.core.ingestion.reader import read
from app.core.ingestion.utils import IngestContext, ResultType
from app.db.models.law_policy.family import FamilyDocument
from tests.core.ingestion.helpers import (
    THREE_DOC_ROWS,
    THREE_DOC_ROWS_2ND_BAD,
    populate_for_ingest,
)


def test_dfc_ingestor__three_good_rows(test_db):
    populate_for_ingest(test_db)
    test_db.commit()
    context = IngestContext()
    document_ingestor = get_dfc_ingestor(test_db)

    read(THREE_DOC_ROWS, context, CCLWDocumentIngestRow, document_ingestor)

    assert len(context.results) == 0
    assert 3 == test_db.query(FamilyDocument).count()


def test_dfc_ingestor__second_bad_row(test_db):
    populate_for_ingest(test_db)
    test_db.commit()
    context = IngestContext()
    document_ingestor = get_dfc_ingestor(test_db)

    read(THREE_DOC_ROWS_2ND_BAD, context, CCLWDocumentIngestRow, document_ingestor)

    assert len(context.results) == 1
    assert context.results[0].type == ResultType.ERROR
    assert context.results[0].details.startswith("Row 2:")

    docs = test_db.query(FamilyDocument).order_by(FamilyDocument.import_id).all()
    assert 2 == len(docs)
    assert docs[0].import_id == "CCLW.executive.1001.0"
    assert docs[1].import_id == "CCLW.executive.1003.0"
