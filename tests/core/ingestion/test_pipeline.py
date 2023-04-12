import json
from datetime import datetime, timezone
from typing import Optional

import pytest

from sqlalchemy.orm import Session
from app.core.ingestion.ingest_row import DocumentIngestRow, EventIngestRow
from app.core.ingestion.pipeline import generate_pipeline_ingest_input
from app.core.ingestion.processor import get_dfc_ingestor, get_event_ingestor
from app.core.ingestion.reader import read
from app.core.ingestion.utils import IngestContext
from tests.core.ingestion.helpers import (
    FIVE_EVENT_ROWS,
    THREE_DOC_ROWS,
    get_doc_ingest_row_data,
    get_event_ingest_row_data,
    populate_for_ingest,
)


def _populate_db_for_test(
    test_db: Session,
    ingest_doc_content: str,
    ingest_event_content: Optional[str] = None,
) -> None:
    populate_for_ingest(test_db)
    test_db.commit()
    context = IngestContext()
    document_ingestor = get_dfc_ingestor(test_db)
    event_ingestor = get_event_ingestor(test_db)

    read(ingest_doc_content, context, DocumentIngestRow, document_ingestor)
    if ingest_event_content is not None:
        read(ingest_event_content, context, EventIngestRow, event_ingestor)

    test_db.commit()
    test_db.flush()


def _get_published_date_for_id(id: str, event_content: str) -> datetime:
    row = 0
    while event_data := get_event_ingest_row_data(0 + row, event_content):
        row += 1
        if (
            event_data.get("CPR Family ID") == id
            and event_data["Event type"] == "Passed/Approved"
        ):
            return datetime.strptime(event_data["Date"], "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )

    raise Exception(f"No published date found for '{id}'")


def test_generate_pipeline_input_document_count(test_db: Session):
    _populate_db_for_test(test_db, THREE_DOC_ROWS)

    documents = generate_pipeline_ingest_input(test_db)
    assert len(documents) == 3


def test_generate_pipeline_input_document_content(test_db: Session):
    _populate_db_for_test(test_db, THREE_DOC_ROWS)

    documents = generate_pipeline_ingest_input(test_db)
    documents_s3_object = {doc.import_id: doc for doc in documents}

    count = 0
    while csv_doc_row := get_doc_ingest_row_data(0 + count, contents=THREE_DOC_ROWS):
        count += 1
        document = documents_s3_object[csv_doc_row["CPR Document ID"]]
        assert document.import_id == csv_doc_row["CPR Document ID"]
        assert document.category == csv_doc_row["Category"].title()
        assert document.description == csv_doc_row["Family summary"]
        assert document.geography == csv_doc_row["Geography ISO"]
        assert document.languages == (
            [csv_doc_row["Language"]] if csv_doc_row["Language"] else []
        )
        assert document.name == csv_doc_row["Family name"]
        assert document.publication_ts == datetime(1900, 1, 1, tzinfo=timezone.utc)
        assert document.slug == csv_doc_row["CPR Document Slug"]
        assert document.source == "CCLW"
        assert document.source_url == csv_doc_row["Documents"].split("|")[0]

    assert count == len(documents)


def test_generate_pipeline_input_document_content_with_events(test_db: Session):
    _populate_db_for_test(test_db, THREE_DOC_ROWS, FIVE_EVENT_ROWS)

    documents = generate_pipeline_ingest_input(test_db)
    documents_s3_object = {doc.import_id: doc for doc in documents}

    count = 0
    while csv_doc_row := get_doc_ingest_row_data(0 + count, contents=THREE_DOC_ROWS):
        count += 1
        document = documents_s3_object[csv_doc_row["CPR Document ID"]]
        assert document.import_id == csv_doc_row["CPR Document ID"]
        assert document.name == csv_doc_row["Family name"]
        assert document.description == csv_doc_row["Family summary"]
        assert document.publication_ts == _get_published_date_for_id(
            csv_doc_row["CPR Family ID"], FIVE_EVENT_ROWS
        )

    assert count == len(documents)


@pytest.mark.parametrize("input_content", [THREE_DOC_ROWS])
def test_generate_pipeline_input_json(input_content, test_db):
    _populate_db_for_test(test_db, input_content)

    documents = generate_pipeline_ingest_input(test_db)
    json_content = json.dumps(
        {d.import_id: d.to_json() for d in documents},
        indent=2,
    )
    assert json_content
