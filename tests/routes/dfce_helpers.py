from typing import List, Tuple, cast
from sqlalchemy.orm import Session
from slugify import slugify
from uuid import uuid4

from db_client.models.dfce import (
    Collection,
    Family,
    CollectionOrganisation,
    FamilyOrganisation,
    CollectionFamily,
    FamilyDocument,
    FamilyEvent,
    Slug,
    FamilyMetadata,
)
from db_client.models.document.physical_document import (
    PhysicalDocument,
    PhysicalDocumentLanguage,
    Language,
)


def generate_slug(
    db: Session,
    title: str,
    attempts: int = 100,
    suffix_length: int = 4,
) -> str:
    """
    Generates a slug that doesn't already exists.

    :param Session db: The connection to the db
    :param str title: The title or name of the object you wish to slugify
    :param int attempts: The number of attempt to generate a unique slug before
    failing, defaults to 100
    :param int suffix_length: The suffix to produce uniqueness, defaults to 4
    :raises RuntimeError: If we cannot produce a unique slug
    :return str: the slug generated
    """
    lookup = set([cast(str, n) for n in db.query(Slug.name).all()])
    base = slugify(str(title))

    # TODO: try to extend suffix length if attempts are exhausted
    suffix = str(uuid4())[:suffix_length]
    count = 0
    while (slug := f"{base}_{suffix}") in lookup:
        count += 1
        suffix = str(uuid4())[:suffix_length]
        if count > attempts:
            raise RuntimeError(
                f"Failed to generate a slug for {base} after {attempts} attempts."
            )
    lookup.add(slug)
    return slug


def add_collections(db: Session, collections, org_id=1):
    for c in collections:
        db.add(
            Collection(
                import_id=c["import_id"],
                title=c["title"],
                description=c["description"],
            )
        )
    db.flush()
    for c in collections:
        db.add(
            CollectionOrganisation(
                collection_import_id=c["import_id"], organisation_id=org_id
            )
        )
    db.commit()


def add_families(db: Session, families, org_id=1):
    for f in families:
        db.add(
            Family(
                import_id=f["import_id"],
                title=f["title"],
                description=f["description"],
                geography_id=f["geography_id"],
                family_category=f["category"],
            )
        )
        db.flush()
        # name = generate_slug(db, f["title"])
        new_slug = Slug(
            family_import_id=f["import_id"],
            family_document_import_id=None,
            name=f["slug"],
        )
        db.add(new_slug)
        for d in f["documents"]:
            add_document(db, f["import_id"], d)

        db.add(
            FamilyMetadata(
                family_import_id=f["import_id"],
                taxonomy_id=1,
                value={},
            )
        )

    db.commit()
    for f in families:
        db.add(
            FamilyOrganisation(family_import_id=f["import_id"], organisation_id=org_id)
        )
    db.commit()


def add_event(db: Session, family_import_id, family_document_import_id, e):
    """Adds an Event"""

    # Note: should we be checking the "Eventable Type",
    #       e.g. Legislation from family category?

    db.add(
        FamilyEvent(
            family_import_id=family_import_id,
            family_document_import_id=family_document_import_id,
            import_id=e["import_id"],
            title=e["title"],
            date=e["date"],
            event_type_name=e["type"],
            status=e["status"],
        )
    )


def add_document(db: Session, family_import_id, d):
    pd = PhysicalDocument(
        title=d["title"],
        md5_sum=d["md5_sum"],
        cdn_object=None,
        source_url=d["url"],
        content_type=d["content_type"],
    )

    db.add(pd)
    db.flush()

    db.add(
        FamilyDocument(
            family_import_id=family_import_id,
            physical_document_id=pd.id,
            import_id=d["import_id"],
            variant_name=d["language_variant"],
            document_status=d["status"],
            document_type=d["type"],
            document_role=d["role"],
        )
    )
    db.flush()
    for lang in d["languages"]:
        db_lang = db.query(Language).filter(Language.language_code == lang).one()
        db.add(
            PhysicalDocumentLanguage(
                language_id=db_lang.id, document_id=pd.id, source="User", visible=True
            )
        )

    new_slug = Slug(
        family_import_id=None,
        family_document_import_id=d["import_id"],
        name=d["slug"],
    )
    db.add(new_slug)

    for e in d["events"]:
        add_event(db, family_import_id, d["import_id"], e)


def link_collection_family(db: Session, links: List[Tuple[str, str]]):
    for collection_import_id, family_import_id in links:
        db.add(
            CollectionFamily(
                collection_import_id=collection_import_id,
                family_import_id=family_import_id,
            )
        )
    db.flush()
