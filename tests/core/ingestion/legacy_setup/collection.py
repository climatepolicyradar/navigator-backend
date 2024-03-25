from typing import Any, Optional, cast

from sqlalchemy.orm import Session
from tests.core.ingestion.legacy_setup.params import IngestParameters
from tests.core.ingestion.legacy_setup.unfccc.ingest_row_unfccc import (
    CollectionIngestRow,
)
from tests.core.ingestion.legacy_setup.utils import (
    create,
    to_dict,
    update_if_changed,
)

from db_client.models.dfce import Collection
from db_client.models.dfce.collection import (
    CollectionFamily,
    CollectionOrganisation,
)


def handle_cclw_collection_and_link(
    db: Session,
    params: IngestParameters,
    org_id: int,
    family_import_id: str,
    result: dict[str, Any],
) -> Optional[Collection]:
    collection_id = params.cpr_collection_ids[0]  # Only ever one for CCLW

    collection = handle_create_collection(
        db,
        collection_id,
        params.collection_name,
        params.collection_summary,
        org_id,
        result,
    )

    handle_link_family_to_one_collection(
        db, collection_id, cast(str, family_import_id), result
    )
    return collection


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
        # Check it belongs to the same organisation
        existing_collection_organisation = (
            db.query(CollectionOrganisation)
            .filter(
                CollectionOrganisation.collection_import_id == row.cpr_collection_id
            )
            .filter(CollectionOrganisation.organisation_id == org_id)
            .one_or_none()
        )

        if not existing_collection_organisation:
            raise ValueError(
                f"This collection {row.cpr_collection_id}"
                + " belongs to another org or none."
            )

        # Check values match
        collection = (
            db.query(Collection)
            .filter(Collection.title == row.collection_name)
            .filter(Collection.description == row.collection_summary)
            .filter(Collection.import_id == row.cpr_collection_id)
            .one_or_none()
        )
        if collection:
            return collection
        raise ValueError(f"Collection {row.collection_name} has incompatible values")

    raise ValueError(
        f"Collection {row.cpr_collection_id} is pre-exiting, and mis-matches"
    )


def is_a_collection_id(collection_id: str) -> bool:
    return len(collection_id) > 0 and collection_id.lower() != "n/a"


def handle_create_collection(
    db: Session,
    collection_id: str,
    collection_name: str,
    collection_summary: str,
    org_id: int,
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

    if not is_a_collection_id(collection_id):
        return None

    # First check for the actual collection
    existing_collection = (
        db.query(Collection).filter(Collection.import_id == collection_id).one_or_none()
    )

    if existing_collection is None:
        collection = create(
            db,
            Collection,
            import_id=collection_id,
            title=collection_name,
            extra={"description": collection_summary},
        )

        collection_organisation = create(
            db,
            CollectionOrganisation,
            collection_import_id=collection_id,
            organisation_id=org_id,
        )

        result["collection_organisation"] = to_dict(collection_organisation)
        result["collection"] = to_dict(collection)
    else:
        collection = existing_collection
        updated = {}
        update_if_changed(updated, "title", collection_name, collection)
        update_if_changed(updated, "description", collection_summary, collection)
        if len(updated) > 0:
            result["collection"] = updated
            db.add(collection)
            db.flush()

    return collection


def handle_link_collection_to_family(
    db: Session,
    collection_ids: list[str],
    family_import_id: str,
    result: dict[str, Any],
) -> None:
    # TODO: PDCT-167 remove all links not to this collection_id
    # then if we don't have a link to this collection_id then add it
    for collection_id in collection_ids:
        existing_link = (
            db.query(CollectionFamily)
            .filter_by(
                collection_import_id=collection_id,
                family_import_id=family_import_id,
            )
            .one_or_none()
        )

        if existing_link is None:
            collection_family = create(
                db,
                CollectionFamily,
                collection_import_id=collection_id,
                family_import_id=family_import_id,
            )
            result["collection_family"] = to_dict(collection_family)


def handle_link_family_to_one_collection(
    db: Session,
    collection_id: str,
    family_import_id: str,
    result: dict[str, Any],
) -> None:
    existing_links = (
        db.query(CollectionFamily)
        .filter_by(
            family_import_id=family_import_id,
        )
        .all()
    )

    if len(existing_links) > 0:
        if collection_id in [link.collection_import_id for link in existing_links]:
            # Nothing to do as its already part of the collection
            return
        else:
            # Remove any links (enforce one collection per family)
            for link in existing_links:
                db.delete(link)

    # Now we need to add the link to the correct collection
    if is_a_collection_id(collection_id):
        collection_family = create(
            db,
            CollectionFamily,
            collection_import_id=collection_id,
            family_import_id=family_import_id,
        )
        result["collection_family"] = to_dict(collection_family)
