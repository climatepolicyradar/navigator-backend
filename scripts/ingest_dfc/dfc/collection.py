from typing import Any, Optional

from sqlalchemy.orm import Session

from app.db.models.law_policy import Collection
from app.db.models.law_policy.collection import CollectionFamily, CollectionOrganisation

from scripts.ingest_dfc.utils import DfcRow, get_or_create, to_dict


def collection_from_row(
    db: Session, row: DfcRow, org_id: int, family_import_id: str, result: dict[str, Any]
) -> Optional[Collection]:
    """Creates the collection part of the schema from the row.

    Args:
        db (Session): connection to the database.
        org_id (int): the organisation id associated with this row.
        row (DfcRow): the row built from the CSV.
        family_id (int): the family id associated with this row.

    Returns:
        dict : a created dictionary to describe what was created.
    """
    if not row.cpr_collection_id or row.cpr_collection_id == "n/a":
        return None

    collection = get_or_create(
        db,
        Collection,
        import_id=row.cpr_collection_id,
        title=row.collection_name,
        extra={"description": row.collection_summary},
    )
    result["collection"] = to_dict(collection)

    collection_organisation = get_or_create(
        db,
        CollectionOrganisation,
        collection_import_id=collection.import_id,
        organisation_id=org_id,
    )
    result["collection_organisation"] = to_dict(collection_organisation)

    collection_family = get_or_create(
        db,
        CollectionFamily,
        collection_import_id=collection.import_id,
        family_import_id=family_import_id,
    )
    result["collection_family"] = to_dict(collection_family)

    return collection
