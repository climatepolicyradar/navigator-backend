from dataclasses import fields
from typing import Any, Sequence

from pydantic import ConfigDict, Extra
from pydantic.dataclasses import dataclass

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


@dataclass(config=ConfigDict(validate_assignment=True, extra=Extra.forbid))
class DfcRow:
    row_number: int
    id: str
    document_id: str
    cclw_description: str
    part_of_collection: str
    create_new_families: str
    collection_id: str
    collection_name: str
    collection_summary: str
    document_title: str
    family_name: str
    family_summary: str
    family_id: str
    document_role: str
    applies_to_id: str
    geography_iso: str
    documents: str
    category: str
    events: list[str]
    sectors: list[str]
    instruments: list[str]
    frameworks: list[str]
    responses: list[str]
    natural_hazards: list[str]
    keywords: list[str]
    document_type: str
    year: int
    language: str
    geography: str
    parent_legislation: str
    comment: str
    cpr_document_id: str
    cpr_family_id: str
    cpr_collection_id: str
    cpr_family_slug: str
    cpr_document_slug: str

    @classmethod
    def from_row(cls, row_number: int, data: dict[str, str]):
        """Parse a row from a CSV into the DfcRow type"""
        field_info = cls.field_info()
        return cls(
            row_number=row_number,
            **{
                cls._key(k): cls._parse_str(cls._key(k), v, field_info)
                for (k, v) in data.items()
            },
        )

    @classmethod
    def field_info(cls) -> dict[str, type]:
        return {field.name: field.type for field in fields(cls)}

    @classmethod
    def _parse_str(cls, key: str, value: str, field_info: dict[str, type]) -> Any:
        if key not in field_info:
            # Let pydantic deal with unexpected fields
            return value

        if field_info[key] == list[str]:
            return [e.strip() for e in value.split(";")]

        if field_info[key] == int:
            return int(value) if value else 0

        if field_info[key] == str:
            if (na := str(value).lower()) == "n/a":
                return na
            else:
                return value

        raise Exception(f"Unhandled type '{cls.field_info()[key]}' in row parsing")

    @staticmethod
    def _key(key: str) -> str:
        return key.lower().replace(" ", "_").replace("?", "").replace("y/", "")


######### OLD ###############
# class DfcRow:
#     row_number: int = 0
#     id: str = ""
#     document_id: str = ""
#     cclw_description: str = ""
#     part_of_collection: str = ""
#     create_new_familyies: str = ""
#     collection_id: str = ""
#     collection_name: str = ""
#     collection_summary: str = ""
#     document_title: str = ""
#     family_name: str = ""
#     family_summary: str = ""
#     family_id: str = ""
#     document_role: str = ""
#     applies_to_id: str = ""
#     geography_iso: str = ""
#     documents: str = ""
#     category: str = ""
#     events: list[str] = []
#     sectors: list[str] = []
#     instruments: list[str] = []
#     frameworks: list[str] = []
#     responses: list[str] = []
#     natural_hazards: str = ""
#     document_type: str = ""
#     year: int = 0
#     language: str = ""
#     keywords: list[str] = []
#     geography: str = ""
#     parent_legislation: str = ""
#     comment: str = ""
#     cpr_document_id: str = ""
#     cpr_family_id: str = ""
#     cpr_collection_id: str = ""
#     cpr_family_slug: str = ""
#     cpr_document_slug: str = ""

#     _semicolon_delimited_array_keys = ["sectors", "frameworks", "keywords", "responses"]
#     _bar_delimited_array_keys = [ "instruments", "events", "documents" ]

#     _int_keys = ["row_number", "year"]

#     def __init__(self, row_number: int, row:dict[str, str]):
#         """Creates a Row given a row in the CSV.

#         This does the translation of column name to field name and also sets its value.

#         Args:
#             row_number (int): the row number of the CSV row.
#             row (dict, optional): the dict of the row in the CSV. Defaults to None.
#         """
#         if row:
#             setattr(self, "row_number", row_number)
#             for key, value in row.items():
#                 # translate the column names to useful field names...
#                 k = key.lower().replace(" ", "_").replace("?", "").replace("/", "")
#                 # now set it
#                 self._set_value(k, value)


#     def _set_value(self, key: str, value: str):
#         """Sets the value given the type.

#         This does the splitting of separated values into arrays.
#         Any key that is not recognized causes the script to bail.
#         """
#         if not hasattr(self, key):
#             print(f"Received an unknown column: {key}")
#             sys.exit(10)

#         if value.lower() == "n/a":
#             setattr(self, key, value)
#         elif key in self._semicolon_delimited_array_keys:
#             setattr(self, key, value.split(";"))
#         elif key in self._bar_delimited_array_keys:
#             setattr(self, key, value.split("|"))
#         elif key in self._int_keys:
#             setattr(self, key, int(value) if value else 0)
#         else:
#             setattr(self, key, value)
