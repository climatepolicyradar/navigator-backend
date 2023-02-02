from app.db.models.law_policy.collection import CollectionFamily, CollectionOrganisation
from dfc_csv_reader import Row
from sqlalchemy.orm import Session
from scripts.ingest_dfc.utils import get_or_create, to_dict
from app.db.models.law_policy import Collection


def collection_from_row(db: Session, org_id: int, row: Row, family_id: int):
    result = {}
    if row.part_of_collection.upper() == "FALSE":
        return result

    def attach_to_org(collection):
        collection_organisation = CollectionOrganisation(
            collection_id=collection.id,
            organisation_id=org_id
        )
        db.add(collection_organisation)
        db.commit()

        result["collection_organisation"] = to_dict(collection_organisation)
        
    collection = get_or_create(db, Collection, title=row.collection_name, extra={ "description": row.collection_summary },
                               after_create=attach_to_org)

    result["collection"] = to_dict(collection)

    collection_family = CollectionFamily(
        collection_id=collection.id,
        family_id=family_id
        
    )

    db.add(collection_family)
    db.commit()

    result["collection_family"] = to_dict(collection_family)

    return result