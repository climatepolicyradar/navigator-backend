import csv
from io import StringIO
from typing import Type

from fastapi import UploadFile
from app.core.ingestion.ingest_row_base import BaseIngestRow, validate_csv_columns
from app.core.ingestion.processor import ProcessFunc

from app.core.ingestion.utils import IngestContext
from app.core.validation.types import ImportSchemaMismatchError


def get_file_contents(csv_upload: UploadFile) -> str:
    """
    Gets the file contents from an UploadFile.

    :param [UploadFile] csv_upload: The UploadFile from an HTTP request.
    :return [str]: The contents of the file.
    """
    return csv_upload.file.read().decode("utf8")


def read(
    file_contents: str,
    context: IngestContext,
    row_type: Type[BaseIngestRow],
    process: ProcessFunc,
) -> None:
    """
    Read a CSV file and call process() for each row.

    :param [str] file_contents: the content of the imported CSV file.
    :param [IngestContext] context: a context to use during import.
    :param [Type[BaseIngestRow]] row_type: the type of row expected from the CSV.
    :param [ProcessFunc] process: the function to call to process a single row.
    """
    reader = csv.DictReader(StringIO(initial_value=file_contents))
    if reader.fieldnames is None:
        raise ImportSchemaMismatchError("No fields in CSV!", {})

    missing_columns = validate_csv_columns(
        reader.fieldnames,
        row_type.VALID_COLUMNS,
    )
    if missing_columns:
        raise ImportSchemaMismatchError(
            "Field names in CSV did not validate", {"missing": missing_columns}
        )
    row_count = 0

    for row in reader:
        row_count += 1
        process(context, row_type.from_row(row_count, row))
