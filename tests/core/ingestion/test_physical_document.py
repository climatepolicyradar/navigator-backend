from sqlalchemy.orm import Session

from app.core.ingestion.ingest_row import IngestRow
from app.core.ingestion.physical_document import physical_document_from_row
from app.db.models.deprecated.document import Document
from app.db.models.document import PhysicalDocument
from app.db.models.document.physical_document import PhysicalDocumentLanguage
from tests.core.ingestion.helpers import (
    DOCUMENT_IMPORT_ID,
    DOCUMENT_TITLE,
    get_ingest_row_data,
    init_for_ingest,
)


def test_physical_document_from_row(test_db: Session):
    init_for_ingest(test_db)
    row = IngestRow.from_row(1, get_ingest_row_data(0))
    row.language = "English"
    result = {}

    doc = (
        test_db.query(Document)
        .filter(Document.import_id == DOCUMENT_IMPORT_ID)
        .one_or_none()
    )
    phys_doc = physical_document_from_row(test_db, row, doc, result)
    test_db.flush()

    assert phys_doc
    actual_keys = set(result.keys())
    expected_keys = set(
        [
            "physical_document",
            "physical_document_language",
            "language",
        ]
    )
    assert actual_keys.symmetric_difference(expected_keys) == set([])

    # Check objects were created ...
    assert test_db.query(PhysicalDocument).filter_by(title=DOCUMENT_TITLE).one()
    assert (
        test_db.query(PhysicalDocumentLanguage).filter_by(document_id=phys_doc.id).one()
    )
