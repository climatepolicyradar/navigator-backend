import sys
from typing import Sequence

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

VALID_COLUMN_NAMES = set(REQUIRED_COLUMNS)


def validate_csv_columns(column_names: Sequence[str]) -> bool:
    return VALID_COLUMN_NAMES.issubset(set(column_names))


class DfcRow:
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
    events: list[str] = []
    sectors: list[str] = []
    instruments: list[str] = []
    frameworks: list[str] = []
    responses: list[str] = []
    natural_hazards: str = ""
    document_type: str = ""
    year: int = 0
    language: str = ""
    keywords: list[str] = []
    geography: str = ""
    parent_legislation: str = ""
    comment: str = ""
    cpr_document_id: str = ""
    cpr_family_id: str = ""
    cpr_collection_id: str = ""
    cpr_family_slug: str = ""
    cpr_document_slug: str = ""

    _semicolon_delimited_array_keys = ["sectors", "frameworks", "keywords", "responses"]
    _bar_delimited_array_keys = ["instruments", "events", "documents"]

    _int_keys = ["row_number", "year"]

    def __init__(self, row: dict = {}):
        """Creates a Row given a row in the CSV.

        This does the translation of column name to field name and also sets its value.

        Args:
            row_number (int): the row number of the CSV row.
            row (dict, optional): the dict of the row in the CSV. Defaults to None.
        """
        if row is not {}:
            for key, value in row.items():
                # translate the column names to useful field names...
                k = key.lower().replace(" ", "_").replace("?", "").replace("/", "")
                # now set it
                self._set_value(k, value)

    def _set_value(self, key: str, value: str):
        """Sets the value given the type.

        This does the splitting of separated values into arrays.
        Any key that is not recognized causes the script to bail.
        """
        if not hasattr(self, key):
            print(f"Received an unknown column: {key}")
            sys.exit(10)

        if value.lower() == "n/a":
            setattr(self, key, value)
        elif key in self._semicolon_delimited_array_keys:
            setattr(self, key, value.split(";"))
        elif key in self._bar_delimited_array_keys:
            setattr(self, key, value.split("|"))
        elif key in self._int_keys:
            setattr(self, key, int(value) if value else 0)
        else:
            setattr(self, key, value)
