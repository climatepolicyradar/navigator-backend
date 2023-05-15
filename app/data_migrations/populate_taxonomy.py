import json
from typing import Sequence

from sqlalchemy.orm import Session

from app.db.models.app.users import Organisation
from app.db.models.law_policy.metadata import MetadataOrganisation, MetadataTaxonomy

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
        "filename": "app/data_migrations/data/cclw/topic_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
    {
        "key": "sector",
        "filename": "app/data_migrations/data/cclw/sector_data.json",
        "file_key_path": "node.name",
        "allow_blanks": True,
    },
    {
        "key": "keyword",
        "filename": "app/data_migrations/data/cclw/keyword_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
    {
        "key": "instrument",
        "filename": "app/data_migrations/data/cclw/instrument_data.json",
        "file_key_path": "node.name",
        "allow_blanks": True,
    },
    {
        "key": "hazard",
        "filename": "app/data_migrations/data/cclw/hazard_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
    {
        "key": "framework",
        "filename": "app/data_migrations/data/cclw/framework_data.json",
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


def get_cclw_taxonomy():
    taxonomy = {}
    for data in TAXONOMY_DATA:
        taxonomy.update(
            {
                data["key"]: {
                    "allowed_values": load_metadata_type(
                        data["filename"], data["file_key_path"]
                    ),
                    "allow_blanks": data["allow_blanks"],
                },
            }
        )

    # Remove unwanted values for new taxonomy
    if (
        "sector" in taxonomy
        and "Transportation" in taxonomy["sector"]["allowed_values"]
    ):
        taxonomy["sector"]["allowed_values"].remove("Transportation")

    return taxonomy


def populate_org_taxonomy(
    db: Session, org_name: str, org_type: str, description: str, fn_get_taxonomy
) -> None:
    """Populates the taxonomy from the data."""

    # First the org
    org = db.query(Organisation).filter(Organisation.name == org_name).one_or_none()
    if org is None:
        org = Organisation(
            name=org_name, description=description, organisation_type=org_type
        )
        db.add(org)
        db.flush()

    metadata_org = (
        db.query(MetadataOrganisation)
        .filter(MetadataOrganisation.organisation_id == org.id)
        .one_or_none()
    )
    if metadata_org is None:
        # Now add the taxonomy
        tax = MetadataTaxonomy(
            description=f"{org_name} loaded values",
            valid_metadata=fn_get_taxonomy(),
        )
        db.add(tax)
        db.flush()
        # Finally the link between the org and the taxonomy.
        db.add(
            MetadataOrganisation(
                taxonomy_id=tax.id,
                organisation_id=org.id,
            )
        )
        db.flush()


def populate_taxonomy(db: Session) -> None:
    populate_org_taxonomy(
        db,
        org_name="CCLW",
        org_type="Academic",
        description="Climate Change Laws of the World",
        fn_get_taxonomy=get_cclw_taxonomy,
    )
