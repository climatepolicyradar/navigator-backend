from typing import Any, Optional

from sqlalchemy.orm import Session
from app.core.ingestion.params import IngestParameters
from app.core.ingestion.unfccc.ingest_row_unfccc import CollectionIngestRow
from app.core.ingestion.utils import (
    create,
    to_dict,
    update_if_changed,
)

from app.db.models.law_policy import Collection
from app.db.models.law_policy.collection import CollectionFamily, CollectionOrganisation


def create_collection(
    db: Session,
    row: CollectionIngestRow,
    org_id: int,
    result: dict[str, Any],
) -> Optional[Collection]:
    # First check for the actual collection
    existing_collection = (
        db.query(Collection)
        .filter(Collection.import_id == row.cpr_collection_id)
        .one_or_none()
    )

    if existing_collection is None:
        collection = create(
            db,
            Collection,
            import_id=row.cpr_collection_id,
            title=row.collection_name,
            extra={"description": row.collection_summary},
        )

        collection_organisation = create(
            db,
            CollectionOrganisation,
            collection_import_id=collection.import_id,
            organisation_id=org_id,
        )

        result["collection_organisation"] = to_dict(collection_organisation)
        result["collection"] = to_dict(collection)

        return collection

    if existing_collection is not None:
        # Check it matches
        # FIXME: also check for the collection-organisation relationship?
        collection = (
            db.query(Collection)
            .filter(Collection.title == row.collection_name)
            .filter(Collection.description == row.collection_summary)
            .filter(Collection.import_id == row.collection_summary)
            .one_or_none()
        )
        if collection:
            return collection

    raise ValueError(
        f"Collection {row.cpr_collection_id} is pre-exiting, and mis-matches"
    )


def handle_collection_and_link(
    db: Session,
    params: IngestParameters,
    org_id: int,
    family_import_id: str,
    result: dict[str, Any],
) -> Optional[Collection]:
    """
    Creates or Updates the collection part of the schema from the row if needed.

    NOTE: This determines the operation CREATE/UPDATE independently of the
    operation being performed on the Family/FamilyDocument structures.

    :param [Session] db: connection to the database.
    :param [DocumentIngestRow] row: the row built from the CSV.
    :param [int] org_id: the organisation id associated with this row.
    :param [str] family_import_id: the family id associated with this row.
    :param [dict[str, Any]]: a result dict in which to record what was created.
    :return [Collection | None]: A collection if one was created, otherwise None.
    """
    if not params.cpr_collection_id or params.cpr_collection_id == "n/a":
        return None

    # First check for the actual collection
    existing_collection = (
        db.query(Collection)
        .filter(Collection.import_id == params.cpr_collection_id)
        .one_or_none()
    )

    if existing_collection is None:
        if params.create_collections is False:
            id = params.cpr_collection_id
            msg = f"Collection {id} is not pre-exsiting so not linking"
            raise ValueError(msg)

        collection = create(
            db,
            Collection,
            import_id=params.cpr_collection_id,
            title=params.collection_name,
            extra={"description": params.collection_summary},
        )

        collection_organisation = create(
            db,
            CollectionOrganisation,
            collection_import_id=collection.import_id,
            organisation_id=org_id,
        )

        result["collection_organisation"] = to_dict(collection_organisation)
        result["collection"] = to_dict(collection)
    else:
        collection = existing_collection
        updated = {}
        update_if_changed(updated, "title", params.collection_name, collection)
        update_if_changed(updated, "description", params.collection_summary, collection)
        if len(updated) > 0:
            result["collection"] = updated
            db.add(collection)
            db.flush()

    # Second check for the family - collection link
    existing_link = (
        db.query(CollectionFamily)
        .filter_by(
            collection_import_id=params.cpr_collection_id,
            family_import_id=params.cpr_family_id,
        )
        .one_or_none()
    )

    if existing_link is None:
        collection_family = create(
            db,
            CollectionFamily,
            collection_import_id=collection.import_id,
            family_import_id=family_import_id,
        )
        result["collection_family"] = to_dict(collection_family)

    return collection
