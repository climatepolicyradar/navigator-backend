from sqlalchemy.orm import Session

from app.core.ingestion.cclw.ingest_row_cclw import CCLWDocumentIngestRow
from app.core.ingestion.cclw.physical_document import create_physical_document_from_row
from app.db.models.document import PhysicalDocument
from app.db.models.document.physical_document import PhysicalDocumentLanguage
from tests.core.ingestion.helpers import (
    DOCUMENT_TITLE,
    get_doc_ingest_row_data,
    populate_for_ingest,
)


def test_physical_document_from_row(test_db: Session):
    populate_for_ingest(test_db)
    row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    row.language = ["English", "German"]
    result = {}

    phys_doc = create_physical_document_from_row(test_db, row, result)
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
        len(
            test_db.query(PhysicalDocumentLanguage)
            .filter_by(document_id=phys_doc.id)
            .all()
        )
        == 2
    )

    assert len(phys_doc.languages) == 2
    assert set([lang.language_code for lang in phys_doc.languages]) == {"eng", "deu"}
