import csv
import sys
from io import StringIO
from typing import Sequence
import logging
from fastapi import UploadFile

_LOGGER = logging.getLogger(__name__)

ID = "ID"
DOCUMENT_ID = "Document ID"
CCLW_DESCRIPTION = "CCLW Description"
PART_OF_COLLECTION = "Part of collection?"
CREATE_NEW_FAMILYIES = "Create new family/ies?"
COLLECTION_ID = "Collection ID"
COLLECTION_NAME = "Collection name"
COLLECTION_SUMMARY = "Collection summary"
DOCUMENT_TITLE = "Document title"
FAMILY_NAME = "Family name"
FAMILY_SUMMARY = "Family summary"
FAMILY_ID = "Family ID"
DOCUMENT_ROLE = "Document role"
APPLIES_TO_ID = "Applies to ID"
GEOG_ISO = "Geography ISO"
DOCUMENTS = "Documents"
CATEGORY = "Category"
EVENTS = "Events"
SECTORS = "Sectors"
INSTRUMENTS = "Instruments"
FRAMEWORKS = "Frameworks"
RESPONSES = "Responses"
NATURAL_HAZARDS = "Natural Hazards"
DOCUMENT_TYPE = "Document Type"
YEAR = "Year"
LANGUAGE = "Language"
KEYWORDS = "Keywords"
GEOG = "Geography"
PARENT_LEGISLATION = "Parent Legislation"
COMMENT = "Comment"
CPR_DOCUMENT_ID = "CPR Document ID"
CPR_FAMILY_ID = "CPR Family ID"
CPR_COLLECTION_IF = "CPR Collection ID"
CPR_FAMILY_SLUG = "CPR Family Slug"
CPR_DOCUMENT_SLUG = "CPR Document Slug"
CPR_DOCUMENT_STATUS = "CPR Document Status"

REQUIRED_COLUMNS = [
    ID,
    DOCUMENT_ID,
    CCLW_DESCRIPTION,
    PART_OF_COLLECTION,
    CREATE_NEW_FAMILYIES,
    COLLECTION_ID,
    COLLECTION_NAME,
    COLLECTION_SUMMARY,
    DOCUMENT_TITLE,
    FAMILY_NAME,
    FAMILY_SUMMARY,
    FAMILY_ID,
    DOCUMENT_ROLE,
    APPLIES_TO_ID,
    GEOG_ISO,
    DOCUMENTS,
    CATEGORY,
    EVENTS,
    SECTORS,
    INSTRUMENTS,
    FRAMEWORKS,
    RESPONSES,
    NATURAL_HAZARDS,
    DOCUMENT_TYPE,
    YEAR,
    LANGUAGE,
    KEYWORDS,
    GEOG,
    PARENT_LEGISLATION,
    COMMENT,
    CPR_DOCUMENT_ID,
    CPR_FAMILY_ID,
    CPR_COLLECTION_IF,
    CPR_FAMILY_SLUG,
    CPR_DOCUMENT_SLUG,
    CPR_DOCUMENT_STATUS,
]

VALID_COLUMN_NAMES = set(REQUIRED_COLUMNS)


def validate_csv_columns(column_names: Sequence[str]) -> bool:
    return VALID_COLUMN_NAMES.issubset(set(column_names))


class CCLWImportRow:
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
    cpr_document_status: str = ""

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

    def _to_pipeline_input(self) -> dict:
        """Returns a dict that can be used as the input to the pipeline."""
        # TODO validate that these are correct
        return {
            "publication_ts": f"{self.year}-01-01T00:00:00",
            "name": self.family_name,
            "description": self.family_summary,
            "source_url": self.documents,
            "type": self.document_type,
            "source": "CCLW",
            "import_id": self.cpr_document_id,
            "category": self.category,
            "frameworks": [],
            "geography": self.geography,
            "hazards": self.natural_hazards,
            "instruments": self.instruments,
            "keywords": self.keywords,
            "languages": self.language,
            "sectors": self.sectors,
            "topics": self.responses,
            "events": self.events,
            "slug": self.cpr_document_slug,
        }


class CCLWImportRowGenerator:
    """A generator for CCLW import row objects for a given csv file contents."""

    def __init__(self, law_policy_csv: UploadFile):
        file_contents = law_policy_csv.file.read().decode("utf8")
        self.reader = csv.DictReader(StringIO(initial_value=file_contents))

        if self.reader.fieldnames is None:
            _LOGGER.info("No fields in CSV!")
            sys.exit(11)
        assert validate_csv_columns(self.reader.fieldnames)

    def get_rows(self):
        """Generate row objects for the csv source."""
        for row in self.reader:
            yield CCLWImportRow(row)
