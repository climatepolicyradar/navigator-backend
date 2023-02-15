#!/usr/bin/env python3
"""
Ingests a CSV into the new schema.

Takes a single argument that is the filename of the CSV to import.
"""

import csv
import sys
from pathlib import Path


from scripts.ingest_dfc.dfc.processor import (
    ProcessFunc,
    ValidateFunc,
    get_dfc_processor,
)
from scripts.ingest_dfc.utils import DfcRow, validate_csv_columns


def read(csv_file_path: Path, process: ProcessFunc) -> None:
    """
    Reads a CSV file and calls process() for each row.

    :csv_file_path [Path]: the filename of the CSV file.
    :process [ProcessFunc]: the function to call to process a single row.
    """
    with open(csv_file_path) as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            print("No fields in CSV!")
            sys.exit(11)
        assert validate_csv_columns(reader.fieldnames)
        row_count = 0
        errors = False

        for row in reader:
            row_count += 1
            row_object = DfcRow.from_row(row_count, row)
            process(row_object)

        if errors:
            sys.exit(2)


if __name__ == "__main__":
    print("")
    print("Ingesting to new schema...")
    validate: ValidateFunc
    process: ProcessFunc

    validate, process = get_dfc_processor()

    print("Checking database is in a valid state...")
    if not validate():
        print(" *** Database not in correct state to perform the script!")
        exit(1)

    print("Checking arguments...")
    if len(sys.argv) != 2:
        print(" *** Need to supply one CSV file as an argument")
        exit(1)

    filename = Path(sys.argv[1])
    print(f"Reading CSV file {filename}...")

    read(filename, process)
    print("Complete")
    validate()
