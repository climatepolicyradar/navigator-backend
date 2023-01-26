import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Callable
from uuid import uuid4

REQUIRED_COLUMNS = [
    "ID",
    "Document ID",
    "CCLW Description",
    "Part of collection?",
    "Create new family/ies?",
    "Collection ID",
    "Collection name",
    "Collection summary",
    "Document title",
    "Family name",
    "Family summary",
    "Family ID",
    "Document role",
    "Applies to ID",
    "Geography ISO",
    "Documents",
    "Category",
    "Events",
    "Sectors",
    "Instruments",
    "Frameworks",
    "Responses",
    "Natural Hazards",
    "Document Type",
    "Year",
    "Language",
    "Keywords",
    "Geography",
    "Parent Legislation",
    "Comment",
    "CPR Document ID",
    "CPR Family ID",
    "CPR Collection ID",
    "CPR Family Slug",
    "CPR Document Slug",
]

Processor = Callable[[int, dict], bool]


class Row:
    def __init__(self, row_number: int, row=None):
        if row is not None:
            setattr(self, "row_number", row_number)
            for key, value in row.items():
                k = key.lower().replace(" ", "_")
                setattr(self, k, value)


def read(csv_file_path: Path, process: Processor) -> None:
    # First pass to load existing IDs/Slugs
    with open(csv_file_path) as csv_file:
        reader = csv.DictReader(csv_file)
        assert set(REQUIRED_COLUMNS).issubset(set(reader.fieldnames))
        row_count = 0
        errors = False

        for row in reader:
            row_count += 1
            if not process(Row(row_count, row)):
                break

        if errors:
            sys.exit(10)
