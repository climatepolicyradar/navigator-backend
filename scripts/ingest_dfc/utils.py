import enum
from dataclasses import fields
from datetime import datetime
from typing import Any, Sequence, TypeVar

from pydantic import ConfigDict, Extra
from pydantic.dataclasses import dataclass
from sqlalchemy.orm import Session

from app.db.session import Base


class PublicationDateAccuracy(enum.IntEnum):
    """
    To be used in the microsecond field of a datetime to record its accuracy.
    """

    NOT_DEFINED = 000000
    YEAR_ACCURACY = 100000
    MONTH_ACCURACY = 200000
    DAY_ACCURACY = 300000
    HOUR_ACCURACY = 400000
    MINUTE_ACCURACY = 500000
    SECOND_ACCURACY = 600000


"""An undefined datetime"""
# FIXME: We may choose to set this to `None` instead & make the date field nullable
UNDEFINED_DATA_TIME = datetime(1900, 1, 1, 0, 0, 0, PublicationDateAccuracy.NOT_DEFINED)


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
    category: str               # METADATA - make an enum, remove from tax
    events: list[str]
    sectors: list[str]          # METADATA
    instruments: list[str]      # METADATA
    frameworks: list[str]       # METADATA
    responses: list[str]        # METADATA - topics
    natural_hazards: list[str]  # METADATA - hazard
    keywords: list[str]
    document_type: str          # METADATA ? 
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

    def get_first_url(self) -> str:
        """Gets the first URL from the 'documents' attribute.

        FIXME: This could/should be written with more validation.
        """
        documents = self.documents.split(";")
        if len(documents) != 1:
            raise ValueError(f"Expected 1 document to be parsed from: {self.documents}")

        first_url = documents[0].split("|")[0]
        return first_url


DbModel = TypeVar("DbModel", bound=Base)


def get_or_create(db: Session, model: DbModel, **kwargs) -> DbModel:
    """Gets or Creates a row represented by model, and described by kwargs.

    Args:
        db (Session): connection to the database.
        model (_type_): the model (table) you are querying.
        kwargs: a list of attributes to describe the row you are interested in.
        NOTE:
            - if kwargs contains an `extra` key then this will be used during
            creation.
            - if kwargs contains an `after_create` key then the value should
            be a callback function that is called after an object is created.

    Returns:
        Base : The object that was either created or retrieved, or None
    """
    # Remove any extra kwargs before we do the search
    extra = {}
    after_create = None
    if "extra" in kwargs.keys():
        extra = kwargs["extra"]
        del kwargs["extra"]
    if "after_create" in kwargs.keys():
        after_create = kwargs["after_create"]
        del kwargs["after_create"]

    instance = db.query(model).filter_by(**kwargs).first()

    if instance is not None:
        return instance

    # Add the extra args in for creation
    for k, v in extra.items():
        kwargs[k] = v
    instance = model(**kwargs)
    db.add(instance)
    db.commit()
    if after_create:
        after_create(instance)
    return instance


def _sanitize(value: str) -> str:
    """Sanitizes a string by parsing out the class name and truncating.

    Used by `to_dict()`
    Args:
        value (str): the string to be sanitized.

    Returns:
        str: the sanitized string.
    """
    s = str(value)
    if s.startswith("<class"):
        # Magic parsing of class name
        return s[8:-2].split(".")[-1]
    if len(s) > 80:
        return s[:80] + "..."
    return s


def to_dict(base_object: Base) -> dict:
    """Returns a dict of the attributes of the db Base object.

    This also adds the class name too.
    """
    extra = ["__class__"]
    return dict(
        (col, _sanitize(getattr(base_object, col)))
        for col in base_object.__table__.columns.keys() + extra
    )


def mypprint(dict_to_print: dict, indent: int = 0) -> None:
    """Prints a prettier for of a dict than pprint can."""

    def print_item(k, v, indent):
        indent += 4
        if type(v) == dict:
            print(" " * indent + f"{k}: ")
            mypprint(v, indent)
        else:
            print(" " * indent + f"{k}: {v}")

    print(" " * indent + "{")
    sorted_d = dict(sorted(dict_to_print.items()))
    for k, v in sorted_d.items():
        print_item(k, v, indent)
    print(" " * indent + "}")
