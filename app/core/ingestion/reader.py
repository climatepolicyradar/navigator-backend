import csv
from io import StringIO

from fastapi import UploadFile
from app.core.ingestion.ingest_row import IngestRow, validate_csv_columns
from app.core.ingestion.processor import ProcessFunc

from app.core.ingestion.utils import IngestContext


def get_file_contents(csv_upload: UploadFile) -> str:
    return csv_upload.file.read().decode("utf8")


def read(file_contents: str, context: IngestContext, process: ProcessFunc) -> None:
    """
    Read a CSV file and call process() for each row.

    :csv_file_path [Path]: the filename of the CSV file.
    :process [ProcessFunc]: the function to call to process a single row.
    """
    reader = csv.DictReader(StringIO(initial_value=file_contents))
    if reader.fieldnames is None:
        raise ValueError("No fields in CSV!")

    if not validate_csv_columns(reader.fieldnames):
        raise ValueError(f"Field names in CSV did not validate: {reader.fieldnames}")
    row_count = 0

    for row in reader:
        row_count += 1
        process(context, IngestRow.from_row(row_count, row))
