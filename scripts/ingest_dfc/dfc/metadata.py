import json
from typing import cast
from sqlalchemy.orm import Session

from app.db.models.law_policy.metadata import (
    FamilyMetadata,
    MetadataOrganisation,
    MetadataTaxonomy,
)
from scripts.ingest_dfc.utils import DfcRow

MAP = {
    "sector": "sectors",
    "instrument": "instruments",
    "framework": "frameworks",
    "topic": "responses",
    "hazard": "natural_hazards",
    "keyword": "keywords",
}


def add_metadata(
    db: Session, family_import_id: str, taxonomy: dict, taxonomy_name: str, row: DfcRow
):
    metadata = {}

    validate_metadata(taxonomy, row)
    # document_type: str          # METADATA - a list of types is stored in metadata
    db.add(
        FamilyMetadata(
            family_import_id=family_import_id,
            taxonomy_name=taxonomy_name,
            value=metadata,
        )
    )


def validate_metadata(taxonomy, row):
    for tax_key, row_key in MAP.items():
        validate_metadata_field(taxonomy, row, tax_key, row_key)


def validate_metadata_field(taxonomy: dict, row: DfcRow, tax_key: str, row_key: str):
    row_set = set(getattr(row, row_key))
    allowed_set = set(taxonomy[tax_key]["allowed_values"])
    allow_blanks = cast(bool, taxonomy[tax_key]["allow_blanks"])

    if len(row_set) == 0:
        if not allow_blanks:
            raise ValueError(
                f"Row {row.row_number} is blank for {tax_key} - which is not allowed."
            )
        return  # field is blank and allowed

    if not row_set.issubset(allowed_set):
        raise ValueError(
            f"Row {row.row_number} has a value for {tax_key} that is "
            f"unrecognised: '{row_set.difference(allowed_set)}' is not in {allowed_set}"
        )

    return  # Nothing raised so all OK
