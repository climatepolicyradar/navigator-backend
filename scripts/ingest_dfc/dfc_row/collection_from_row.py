from app.db.models.law_policy.collection import CollectionFamily, CollectionOrganisation
from sqlalchemy.orm import Session
from scripts.ingest_dfc.dfc_row.dfc_row import DfcRow
from scripts.ingest_dfc.utils import get_or_create, to_dict
from app.db.models.law_policy import Collection


def collection_from_row(db: Session, org_id: int, row: DfcRow, family_id: int) -> dict:
    """Creates the collection part of the schema from the row.

    Args:
        db (Session): connection to the database.
        org_id (int): the organisation id associated with this row.
        row (DfcRow): the row built from the CSV.
        family_id (int): the family id associated with this row.

    Returns:
        dict : a created dictionary to describe what was created.
    """
    result = {}
    if row.part_of_collection.upper() == "FALSE":
        return result

    def create_collection_links(collection: Collection):
        collection_organisation = CollectionOrganisation(
            collection_id=collection.id,
            organisation_id=org_id
        )
        db.add(collection_organisation)
        db.commit()

        result["collection_organisation"] = to_dict(collection_organisation)
        collection_family = CollectionFamily(
            collection_id=collection.id,
            family_id=family_id
            
        )

        db.add(collection_family)
        db.commit()

        result["collection_family"] = to_dict(collection_family)
        
    collection = get_or_create(db, Collection, title=row.collection_name, extra={ "description": row.collection_summary },
                               after_create=create_collection_links)

    result["collection"] = to_dict(collection)


    return result