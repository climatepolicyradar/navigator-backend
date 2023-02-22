import json
from typing import Sequence

from sqlalchemy.orm import Session
from app.db.models.app.users import Organisation

from app.db.models.law_policy.metadata import MetadataOrganisation, MetadataTaxonomy

from .utils import has_rows

"""At the moment taxonomy is kept simple, and only supports string validation for enums

For Example:

{
    "topic": {
       allowed_values: [],
       allow_blanks: false,
    },
    ...
}

"""

TAXONOMY_DATA = [
    {
        "key": "topic",
        "filename": "app/data_migrations/data/topic_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
    {
        "key": "sector",
        "filename": "app/data_migrations/data/sector_data.json",
        "file_key_path": "node.name",
        "allow_blanks": True,
    },
    {
        "key": "keyword",
        "filename": "app/data_migrations/data/keyword_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
    {
        "key": "instrument",
        "filename": "app/data_migrations/data/instrument_data.json",
        "file_key_path": "node.name",
        "allow_blanks": True,
    },
    {
        "key": "hazard",
        "filename": "app/data_migrations/data/hazard_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
    {
        "key": "framework",
        "filename": "app/data_migrations/data/framework_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
    {
        "key": "document_type",
        "filename": "app/data_migrations/data/document_type_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
]


def dot_dref(obj: dict, dotted_key: str):
    if "." not in dotted_key:
        return obj[dotted_key]
    keys = dotted_key.split(".", 1)
    return dot_dref(obj[keys[0]], keys[1])


def load_metadata_type(filename: str, key_path: str) -> Sequence[str]:
    with open(filename) as file:
        data = json.load(file)
    return [dot_dref(obj, key_path) for obj in data]


def get_default_taxonomy():
    taxonomy = {}
    for data in TAXONOMY_DATA:
        taxonomy = {
            **taxonomy,
            data["key"]: {
                "allowed_values": load_metadata_type(
                    data["filename"], data["file_key_path"]
                ),
                "allow_blanks": data["allow_blanks"],
            },
        }

    # Remove unwanted values for new taxonomy
    if "Transportation" in taxonomy["sector"]["allowed_values"]:
        taxonomy["sector"]["allowed_values"].remove("Transportation")

    return taxonomy


def populate_taxonomy(db: Session) -> None:
    """Populates the taxonomy from the data."""

    if has_rows(db, MetadataTaxonomy) or has_rows(db, Organisation):
        return

    db.add(
        MetadataTaxonomy(
            id=1,
            description="CCLW loaded values",
            valid_metadata=get_default_taxonomy(),
        )
    )
    db.add(
        Organisation(
            id=1,
            name="CCLW",
            description="Climate Change Laws of the World",
            organisation_type="Academic",
        )
    )
    db.flush()
    db.add(
        MetadataOrganisation(
            taxonomy_id=1,
            organisation_id=1,
        )
    )
