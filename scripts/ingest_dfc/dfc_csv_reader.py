import csv
import sys
from pprint import pprint
from pathlib import Path
from typing import Callable

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


class Row:
    row_number: int = 0
    id: str = ""
    document_id: str = ""
    cclw_description: str = ""
    part_of_collection: str = ""
    create_new_familyies: str = ""
    collection_id: str = ""
    collection_name: str = ""
    collection_summary: str = ""
    document_title: str = ""
    family_name: str = ""
    family_summary: str = ""
    family_id: str = ""
    document_role: str = ""
    applies_to_id: str = ""
    geography_iso: str = ""
    documents: str = ""
    category: str = ""
    events: list[str] = ""
    sectors: list[str] = []
    instruments: list[str] = ""
    frameworks: list[str] = ""
    responses: list[str] = ""
    natural_hazards: str = ""
    document_type: str = ""
    year: int = 0
    language: str = ""
    keywords: list[str] = ""
    geography: str = ""
    parent_legislation: str = ""
    comment: str = ""
    cpr_document_id: str = ""
    cpr_family_id: str = ""
    cpr_collection_id: str = ""
    cpr_family_slug: str = ""
    cpr_document_slug: str = ""

    def __init__(self, row_number: int, row=None):
        if row is not None:
            setattr(self, "row_number", row_number)
            for key, value in row.items():
                k = key.lower().replace(" ", "_").replace("?", "").replace("/", "")
                self._set_value(k, value)

    def _set_value(self, key: str, value: str):
        if not hasattr(self, key):
            print(f"Received an unknown column: {key}")
            sys.exit(10)
        if value.lower() == "n/a":
            value = ""
        if key == "sectors":
            value = value.split(";")
        if key == "instruments" or key == "events" or key == "documents":
            value = value.split("|")
        if key == "keywords" or key == "frameworks" or key == "responses":
            value = value.split(";")

        if key == "row_number" or key == "year":
            value = int(value) if value else 0

        setattr(self, key, value)


Processor = Callable[[Row], bool]


def read(csv_file_path: Path, process: Processor) -> None:
    # FIXME: remove row_start before release
    # The row_start is to aid development and debugging
    row_start = 1 #190
    with open(csv_file_path) as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            print("No fields in CSV!")
            sys.exit(11)
        assert set(REQUIRED_COLUMNS).issubset(set(reader.fieldnames))
        row_count = 0
        errors = False

        for row in reader:
            row_count += 1
            row_object = Row(row_count, row)
            if row_count >= row_start:
                if not process(row_object):
                    break

        if errors:
            sys.exit(10)
