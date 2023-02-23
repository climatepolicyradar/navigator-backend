import csv
from io import StringIO

from fastapi import UploadFile
from app.core.ingestion.ingest_row import IngestRow, validate_csv_columns
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


def read(file_contents: str, context: IngestContext, process: ProcessFunc) -> None:
    """
    Read a CSV file and call process() for each row.

    :param [Path] csv_file_path: the filename of the CSV file.
    :param [ProcessFunc] process: the function to call to process a single row.
    """
    reader = csv.DictReader(StringIO(initial_value=file_contents))
    if reader.fieldnames is None:
        raise ImportSchemaMismatchError("No fields in CSV!", {})

    missing_columns = validate_csv_columns(reader.fieldnames)
    if missing_columns:
        raise ImportSchemaMismatchError(
            "Field names in CSV did not validate", {"missing": missing_columns}
        )
    row_count = 0

    for row in reader:
        row_count += 1
        process(context, IngestRow.from_row(row_count, row))
