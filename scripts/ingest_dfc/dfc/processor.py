from dataclasses import fields
from typing import Any, Callable, Sequence, Tuple, cast

from pydantic import ConfigDict, Extra
from pydantic.dataclasses import dataclass
from sqlalchemy.orm import Session

from app.db.models.app.users import Organisation
from app.db.models.deprecated import Document
from app.db.models.document import PhysicalDocument

from scripts.ingest_dfc.dfc.collection import collection_from_row
from scripts.ingest_dfc.dfc.family import family_from_row
from scripts.ingest_dfc.utils import DfcRow, get_or_create, to_dict




ValidateFunc = Callable[[], bool]
ProcessFunc = Callable[[DfcRow], bool]


def ingest_row(db: Session, row: DfcRow) -> dict:
    """Creates the constituent elements in the database that will represent this row.

    Args:
        db (Session): the connection to the database.
        row (DfcRow): the DfcRow object of the current CSV row

    Returns:
        dict: _description_
    """
    result = {}
    import_id = row.cpr_document_id

    print("- Creating organisation")
    organisation = get_or_create(db, Organisation, name="CCLW")
    result["organisation"] = to_dict(organisation)

    print(f"- Creating FamilyDocument for import {import_id}")
    result["family"] = {}
    family = family_from_row(db, row, cast(int, organisation.id), result)

    print(f"- Creating Collection if required for import {import_id}")
    result["collection"] = {}
    collection_from_row(
        db, row, cast(int, organisation.id), cast(int, family.id), result["collection"]
    )

    return result


def get_dfc_processor(db: Session) -> Tuple[ValidateFunc, ProcessFunc]:
    """Gets the validation and process function for ingesting a CSV.

    Args:
        db (Session): the connection to the database

    Returns:
        Tuple[ValidateFunc, ProcessFunc]: A tuple of functions
    """

    def validate() -> bool:
        """Returns if we should be processing - there used to be a lot more to this."""
        num_new_documents = db.query(PhysicalDocument).count()
        num_old_documents = db.query(Document).count()
        print(
            f"Found {num_new_documents} new documents and {num_old_documents} old documents"
        )
        return True  # num_new_documents == 0 and num_old_documents > 0

    def process(row: DfcRow) -> bool:
        """Processes the row into the db."""
        print(f"Processing row: {row.row_number}")

        # No need to start transaction as there is one already started.

        result = ingest_row(db, row=row)
        # mypprint(result)

        # Return False for now so we just process one element
        # FIXME: Change this return value
        return True  # rows_processed < 2

    return validate, process


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
