import os
from typing import Optional, cast

from db_client.models.dfce.collection import Collection, CollectionFamily
from db_client.models.dfce.family import Family, Slug
from sqlalchemy import bindparam, text
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm import Session
from sqlalchemy.types import ARRAY, String

from app.models.document import CollectionOverviewResponse, LinkableFamily
from app.repository.helpers import get_query_template


def get_id_from_slug(
    db: Session, slug: str, allowed_corpora: Optional[list[str]] = None
) -> str | None:
    """Match the slug name to a FamilyDocument or Family import ID.

    This function also contains logic to only get the import ID for the
    family or document if the slug given is associated with a family
    that belongs to the list of allowed corpora.

    :param Session db: connection to db
    :param str slug: slug name to match
    :param Optional[list[str]] allowed_corpora: The corpora IDs to look
        for the slugged object in.
    :return tuple[Optional[str], Optional[str]]: the Collection import_id.
    """
    if allowed_corpora not in [None, []]:
        query_template = text(
            get_query_template(
                os.path.join("app", "repository", "sql", "slug_lookup.sql")
            )
        )

        query_template = query_template.bindparams(
            bindparam("slug_name", type_=String),
            bindparam(
                "allowed_corpora_ids", value=allowed_corpora, type_=ARRAY(String)
            ),
        )
        query = db.execute(
            query_template, {"slug_name": slug, "allowed_corpora_ids": allowed_corpora}
        )
    else:
        query = db.query(Slug.collection_import_id).filter(Slug.name == slug)

    result = query.one_or_none()
    if result is None:
        return None

    COLLECTION_INDEX = 0
    collection_id = (
        cast(str, result[COLLECTION_INDEX])
        if result[COLLECTION_INDEX] is not None
        else None
    )

    return collection_id


def get_collection(
    db: Session, collection_import_id: str
) -> CollectionOverviewResponse | None:
    """
    Get a collection along with its associated families.

    :param Session db: connection to db
    :param str collection_import_id: id of collection
    :return CollectionOverviewResponse: response object containing collection details and its families
    """

    try:
        collection = (
            db.query(Collection)
            .filter(Collection.import_id == collection_import_id)
            .one()
        )
    except NoResultFound:
        raise ValueError(f"No collection found for import_id: {collection_import_id}")
    except MultipleResultsFound:
        raise ValueError(
            f"Multiple collections found for import_id: {collection_import_id}"
        )

    families = _get_families_for_collection(db, collection_import_id)

    return CollectionOverviewResponse(
        title=collection.title,
        description=collection.description,
        import_id=collection.import_id,
        families=families,
    )


def _get_families_for_collection(
    db: Session, collection_import_id: str
) -> list[LinkableFamily]:
    """
    Get all families associated with a specific collection.

    :param Session db: connection to db
    :param str collection_import_id: id of collection
    :return list[LinkableFamily]: list of families in the collection
    """
    families = (
        db.query(Slug.name, Family.title, Family.description)
        .select_from(CollectionFamily)
        .join(Family, Family.import_id == CollectionFamily.family_import_id)
        .join(Slug, Slug.family_import_id == Family.import_id)
        .filter(CollectionFamily.collection_import_id == collection_import_id)
        .all()
    )

    return [
        LinkableFamily(slug=data[0], title=data[1], description=data[2])
        for data in families
    ]
