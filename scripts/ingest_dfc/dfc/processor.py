import sys
from typing import Callable, Tuple, cast

from sqlalchemy.orm import Session

from app.db.models.app.users import Organisation
from app.db.models.deprecated import Document
from app.db.models.document import PhysicalDocument
from app.db.models.law_policy.metadata import MetadataOrganisation

from app.db.session import SessionLocal
from scripts.ingest_dfc.dfc.collection import collection_from_row
from scripts.ingest_dfc.dfc.family import family_from_row
from scripts.ingest_dfc.utils import DfcRow, get_or_create, to_dict


ValidateFunc = Callable[[], bool]
ProcessFunc = Callable[[DfcRow], None]


def ingest_row(db: Session, row: DfcRow) -> dict:
    """
    Create the constituent elements in the database that will represent this row.

    :param [Session] db: the connection to the database.
    :param [DfcRow] row: the DfcRow object of the current CSV row
    :returns [dict[str, Any]]: a result dictionary describing what was created
    """
    result = {}
    import_id = row.cpr_document_id

    existing_document = (
        db.query(Document).filter(Document.import_id == import_id).first()
    )
    if existing_document is None:
        # If there does not already exist a document with the given import_id, do not
        # attempt to migrate
        print("skipping!")
        return result
    print("processing")

    organisation = create_organisation(db, result)
    org_id = cast(int, organisation.id)

    print(f"- Creating FamilyDocument for import {import_id}")
    family = family_from_row(
        db,
        row,
        existing_document,
        org_id,
        result,
    )

    print(f"- Creating Collection if required for import {import_id}")
    collection_from_row(
        db,
        row,
        cast(int, organisation.id),
        cast(str, family.import_id),
        result,
    )

    return result


def create_organisation(db: Session, result: dict):
    def add_default_metadata(org: Organisation):
        db.add(MetadataOrganisation(taxonomy_name="default", organisation_id=org.id))

    print("- Creating organisation")
    organisation = get_or_create(
        db, Organisation, name="CCLW", after_create=add_default_metadata
    )
    result["organisation"] = to_dict(organisation)
    return organisation


def get_dfc_processor() -> Tuple[ValidateFunc, ProcessFunc]:
    """
    Get the validation and process function for ingesting a CSV.

    :return [Tuple[ValidateFunc, ProcessFunc]]: A tuple of functions
    """

    def validate() -> bool:
        """Returns True if we should process the row."""
        db = SessionLocal()
        num_new_documents = db.query(PhysicalDocument).count()
        num_old_documents = db.query(Document).count()
        print(
            f"Found {num_new_documents} new documents and {num_old_documents} old "
            "documents"
        )
        return True  # num_new_documents == 0 and num_old_documents > 0

    def process(row: DfcRow) -> None:
        """Processes the row into the db."""
        sys.stdout.write(f"Processing row: {row.row_number}: ")

        # Beginning a transaction here would create this issue:
        # https://stackoverflow.com/a/58991792
        # Sessions are meant to be short-lived - see https://docs.sqlalchemy.org/en/13/orm/session_basics.html
        db = SessionLocal()
        ingest_row(db, row=row)
        db.commit()
        sys.stdout.flush()

    return validate, process
