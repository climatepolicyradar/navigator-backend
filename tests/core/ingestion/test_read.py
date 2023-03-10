from unittest.mock import MagicMock
import pytest
from app.core.ingestion.ingest_row import DocumentIngestRow
from app.core.ingestion.reader import read
from app.core.ingestion.utils import IngestContext
from app.core.validation.types import ImportSchemaMismatchError
from tests.core.ingestion.helpers import (
    ALPHABETICAL_DOC_COLUMNS,
    THREE_DOC_ROWS,
    THREE_DOC_ROWS_MISSING_FIELD,
)


def test_read__raises_with_no_contents():
    context = IngestContext(org_id=1, results=[])
    process = MagicMock()
    with pytest.raises(ImportSchemaMismatchError) as e_info:
        contents = ""
        read(contents, context, DocumentIngestRow, process)

    assert len(context.results) == 0
    assert (
        e_info.value.message
        == "Bulk import file failed schema validation: No fields in CSV!"
    )
    assert e_info.value.details == {}


def test_read__raises_with_wrong_fields():
    context = IngestContext(org_id=1, results=[])
    process = MagicMock()
    with pytest.raises(ImportSchemaMismatchError) as e_info:
        contents = """a,b,c
        1,2,3"""
        read(contents, context, DocumentIngestRow, process)

    assert len(context.results) == 0
    assert (
        e_info.value.message
        == "Bulk import file failed schema validation: Field names in CSV did not validate"
    )
    assert e_info.value.details == {"missing": ALPHABETICAL_DOC_COLUMNS}


def test_read__raises_with_missing_field():
    context = IngestContext(org_id=1, results=[])
    process = MagicMock()
    with pytest.raises(ImportSchemaMismatchError) as e_info:
        read(THREE_DOC_ROWS_MISSING_FIELD, context, DocumentIngestRow, process)

    assert len(context.results) == 0
    assert (
        e_info.value.message
        == "Bulk import file failed schema validation: Field names in CSV did not validate"
    )
    assert e_info.value.details == {"missing": ["CPR Document Slug"]}


def test_read__processes_all_rows():
    context = IngestContext(org_id=1, results=[])
    process = MagicMock()
    read(THREE_DOC_ROWS, context, DocumentIngestRow, process)

    expected_rows = 3
    assert process.call_count == expected_rows

    start_n = n = 1

    for context_arg, row_arg in [x[0] for x in process.call_args_list]:
        assert context_arg == context
        assert row_arg.row_number == n
        assert row_arg.document_title == f"Title{n}"
        n = n + 1

    assert n - start_n == expected_rows
