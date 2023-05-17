from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.core.ingestion.event import family_event_from_row
from app.core.ingestion.family import handle_family_from_row
from app.core.ingestion.ingest_row_cclw import CCLWDocumentIngestRow, EventIngestRow
from app.db.models.law_policy.family import Family, FamilyEvent
from tests.core.ingestion.helpers import (
    EVENT_IMPORT_ID,
    FAMILY_IMPORT_ID,
    get_doc_ingest_row_data,
    get_event_ingest_row_data,
    populate_for_ingest,
)


def test_family_event_from_row(test_db: Session):
    populate_for_ingest(test_db)
    doc_row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    event_row = EventIngestRow.from_row(1, get_event_ingest_row_data(0))

    result = {}
    family = handle_family_from_row(test_db, doc_row, org_id=1, result=result)
    event = family_event_from_row(test_db, event_row, result=result)

    assert "family_events" in result

    new_family = test_db.query(Family).filter_by(import_id=FAMILY_IMPORT_ID).one()
    new_event = test_db.query(FamilyEvent).filter_by(import_id=EVENT_IMPORT_ID).one()

    assert family == new_family
    assert event == new_event

    assert new_family.published_date == datetime(2019, 12, 25, tzinfo=timezone.utc)
    assert new_family.last_updated_date == datetime(2019, 12, 25, tzinfo=timezone.utc)


def test_family_multiple_events_from_row(test_db: Session):
    populate_for_ingest(test_db)
    doc_row = CCLWDocumentIngestRow.from_row(1, get_doc_ingest_row_data(0))
    event_row_1 = EventIngestRow.from_row(1, get_event_ingest_row_data(0))
    event_row_2 = EventIngestRow.from_row(2, get_event_ingest_row_data(1))

    result = {}
    handle_family_from_row(test_db, doc_row, org_id=1, result=result)
    family_event_from_row(test_db, event_row_1, result=result)

    assert "family_events" in result
    new_family = test_db.query(Family).filter_by(import_id=FAMILY_IMPORT_ID).one()
    assert new_family.published_date == datetime(2019, 12, 25, tzinfo=timezone.utc)
    assert new_family.last_updated_date == datetime(2019, 12, 25, tzinfo=timezone.utc)

    family_event_from_row(test_db, event_row_2, result=result)
    test_db.refresh(new_family)
    assert new_family.published_date == datetime(2019, 12, 25, tzinfo=timezone.utc)
    assert new_family.last_updated_date == datetime(2021, 12, 25, tzinfo=timezone.utc)
