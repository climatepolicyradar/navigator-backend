import os
from typing import Optional

from db_client.models.dfce.collection import Collection, CollectionFamily
from db_client.models.dfce.family import Family, Slug
from sqlalchemy import bindparam, select, text
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm import Session
from sqlalchemy.types import ARRAY, String

from app.models.document import CollectionOverviewResponse, LinkableFamily
from app.repository.helpers import get_query_template


def get_id_from_slug(
    db: Session, slug: str, allowed_corpora: Optional[list[str]] = None
) -> str | None:
    """Match the slug name to a Collection Import ID

    This function also contains logic to only get the import ID for the
    family or document if the slug given is associated with a family
    that belongs to the list of allowed corpora.

    :param Session db: connection to db
    :param str slug: slug name to match
    :param Optional[list[str]] allowed_corpora: The corpora IDs to look
        for the slugged object in.
    :return str | None : the Collection import_id or None if not found
    :raises Exception: if multiple rows are found for the slug
    """
    try:
        if allowed_corpora not in [None, []]:
            query_template = text(
                get_query_template(
                    os.path.join(
                        "app", "repository", "sql", "slug_lookup_collections.sql"
                    )
                )
            )

            query_template = query_template.bindparams(
                bindparam("slug_name", type_=String),
                bindparam(
                    "allowed_corpora_ids", value=allowed_corpora, type_=ARRAY(String)
                ),
            )
            result = db.execute(
                query_template,
                {"slug_name": slug, "allowed_corpora_ids": allowed_corpora},
            ).one_or_none()
        else:
            stmt = select(Slug.collection_import_id).where(Slug.name == slug)
            result = db.execute(stmt).one_or_none()

        # result return a tuple with the collection import id
        return str(result[0]) if result is not None else None

    except MultipleResultsFound:
        raise Exception(f"Multiple rows found for slug: {slug}. Expected only one.")


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
        stmt = select(Collection).where(Collection.import_id == collection_import_id)
        collection = db.execute(stmt).unique().scalar_one()
    except NoResultFound:
        raise ValueError(f"No collection found for import_id: {collection_import_id}")
    except MultipleResultsFound:
        raise ValueError(
            f"Multiple collections found for import_id: {collection_import_id}"
        )

    families = _get_families_for_collection(db, collection_import_id)
    collection_slug = get_collection_slug_from_import_id(db, collection_import_id)

    return CollectionOverviewResponse(
        title=collection.title,
        description=collection.description,
        import_id=collection.import_id,
        families=families,
        slug=collection_slug,
    )


def get_collection_slug_from_import_id(
    db: Session, collection_import_id: str
) -> str | None:
    """
    Get the slug for a collection based on its import ID.

    :param Session db: connection to db
    :param str collection_import_id: id of collection
    :return str | None: slug of the collection or None if not found
    """
    stmt = (
        select(Slug.name)
        .where(Slug.collection_import_id == collection_import_id)
        .where(Slug.family_import_id.is_(None))
        .where(Slug.family_document_import_id.is_(None))
    )
    return db.execute(stmt).scalar_one_or_none()


def _get_families_for_collection(
    db: Session, collection_import_id: str
) -> list[LinkableFamily]:
    """
    Get all families associated with a specific collection.

    :param Session db: connection to db
    :param str collection_import_id: id of collection
    :return list[LinkableFamily]: list of families in the collection
    """
    stmt = (
        select(Slug.name, Family.title, Family.description)
        .select_from(CollectionFamily)
        .join(Family, Family.import_id == CollectionFamily.family_import_id)
        .join(Slug, Slug.family_import_id == Family.import_id)
        .where(CollectionFamily.collection_import_id == collection_import_id)
    )
    families = db.execute(stmt).all()

    return [
        LinkableFamily(slug=str(data[0]), title=str(data[1]), description=str(data[2]))
        for data in families
    ]
