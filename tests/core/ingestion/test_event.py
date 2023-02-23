from datetime import datetime

import pytest  # noqa: F401
from sqlalchemy.orm import Session

from app.core.ingestion.event import family_event_from_row
from app.core.ingestion.family import family_from_row
from app.core.ingestion.ingest_row import DocumentIngestRow, EventIngestRow
from app.db.models.deprecated import Document
from app.db.models.law_policy.family import Family, FamilyEvent
from tests.core.ingestion.helpers import (
    DOCUMENT_IMPORT_ID,
    EVENT_IMPORT_ID,
    FAMILY_IMPORT_ID,
    get_doc_ingest_row_data,
    get_event_ingest_row_data,
    init_for_ingest,
)


def test_family_event_from_row(test_db: Session):
    init_for_ingest(test_db)
    doc_row = DocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    event_row = EventIngestRow.from_row(1, get_event_ingest_row_data(0))

    doc = (
        test_db.query(Document)
        .filter(Document.import_id == DOCUMENT_IMPORT_ID)
        .one_or_none()
    )

    result = {}
    family = family_from_row(test_db, doc_row, doc, org_id=1, result=result)
    event = family_event_from_row(test_db, event_row, result=result)

    assert "family_events" in result

    new_family = test_db.query(Family).filter_by(import_id=FAMILY_IMPORT_ID).one()
    new_event = test_db.query(FamilyEvent).filter_by(import_id=EVENT_IMPORT_ID).one()

    assert family == new_family
    assert event == new_event

    assert new_family.published_date == datetime(2019, 12, 25)
    assert new_family.last_updated_date == datetime(2019, 12, 25)
