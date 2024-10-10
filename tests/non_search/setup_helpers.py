from typing import Any, Dict, Optional

from db_client.functions.dfce_helpers import (
    add_collections,
    add_families,
    link_collection_family,
)
from db_client.models.organisation.corpus import Corpus, CorpusType
from sqlalchemy.orm import Session


def get_default_collections():
    collection1 = {
        "import_id": "CPR.Collection.1.0",
        "title": "Collection1",
        "description": "CollectionSummary1",
    }

    collection2 = {
        "import_id": "CPR.Collection.2.0",
        "title": "Collection2",
        "description": "CollectionSummary2",
    }
    return collection1, collection2


def get_default_documents():
    document1 = {
        "title": "Document1",
        "slug": "DocSlug1",
        "md5_sum": "111",
        "url": "http://somewhere1",
        "content_type": "application/pdf",
        "import_id": "CCLW.executive.1.2",
        "language_variant": "Original Language",
        "status": "PUBLISHED",
        "metadata": {"role": ["MAIN"], "type": ["Plan"]},
        "languages": ["eng"],
        "events": [
            {
                "import_id": "CPR.Event.1.0",
                "title": "Published",
                "date": "2019-12-25",
                "type": "Passed/Approved",
                "status": "OK",
            }
        ],
    }

    document2 = {
        "title": "Document2",
        "slug": "DocSlug2",
        "md5_sum": None,
        "url": "http://another_somewhere",
        "content_type": None,
        "import_id": "CCLW.executive.2.2",
        "language_variant": None,
        "status": "PUBLISHED",
        "metadata": {"role": ["MAIN"], "type": ["Order"]},
        "languages": [],
        "events": [
            {
                "import_id": "CPR.Event.2.0",
                "title": "Published",
                "date": "2019-12-25",
                "type": "Passed/Approved",
                "status": "OK",
            }
        ],
    }
    return document1, document2


def get_default_families():
    family1 = {
        "import_id": "CCLW.family.1001.0",
        "corpus_import_id": "CCLW.corpus.i00000001.n0000",
        "title": "Fam1",
        "slug": "FamSlug1",
        "description": "Summary1",
        "geography_id": 1,
        "category": "Executive",
        "documents": [],
        "metadata": {
            "size": "big",
            "color": "pink",
        },
    }

    family2 = {
        "import_id": "CCLW.family.2002.0",
        "corpus_import_id": "CCLW.corpus.i00000001.n0000",
        "title": "Fam2",
        "slug": "FamSlug2",
        "description": "Summary2",
        "geography_id": 2,
        "category": "Executive",
        "documents": [],
        "metadata": {
            "size": "small",
            "color": "blue",
        },
    }
    return family1, family2


def setup_with_docs(db: Session):
    # Collection
    collection1, collection2 = get_default_collections()
    add_collections(db, collections=[collection1])

    # Family + Document + events
    document1, document2 = get_default_documents()
    family1, family2 = get_default_families()
    family1["documents"] = [document1]

    add_families(db, families=[family1])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
        ],
    )


def setup_with_two_docs(db: Session):
    """Creates 2 DFCEs"""

    # Collection
    collection1, collection2 = get_default_collections()
    add_collections(db, collections=[collection1])

    # Family + Document + events
    document1, document2 = get_default_documents()
    family1, family2 = get_default_families()
    family1["documents"] = [document1]
    family2["documents"] = [document2]
    add_families(db, families=[family1, family2])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
            (collection1["import_id"], family2["import_id"]),
        ],
    )


def setup_with_documents_large_with_families(
    documents_large: list[Dict[str, Any]],
    db: Session,
):
    # Collection
    collection1, collection2 = get_default_collections()
    add_collections(db, collections=[collection1])

    # Family + Document + events
    family1, family2 = get_default_families()

    family1["metadata"] = {
        "topic": "Mitigation",
        "sector": "Economy-wide",
    }

    split_index = len(documents_large) // 2

    family1["documents"] = documents_large[:split_index]
    family2["documents"] = documents_large[split_index:]
    add_families(db, families=[family1, family2])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
        ],
    )


def setup_with_two_docs_one_family(db: Session):
    # Collection
    collection1, collection2 = get_default_collections()
    add_collections(db, collections=[collection1])

    # Family + Document + events
    document1, document2 = get_default_documents()
    family1, family2 = get_default_families()
    family1["documents"] = [document1, document2]
    add_families(db, families=[family1])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
        ],
    )


def setup_with_two_docs_multiple_languages(db: Session):
    # Collection
    collection1, collection2 = get_default_collections()
    add_collections(db, collections=[collection1])

    # Family + Document + events
    document1, document2 = get_default_documents()
    document1["languages"] = ["fra", "eng"]

    family1, family2 = get_default_families()
    family1["documents"] = [document1]
    family2["documents"] = [document2]
    add_families(db, families=[family1, family2])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
            (collection1["import_id"], family2["import_id"]),
        ],
    )


def setup_with_two_docs_bad_ids(db: Session):
    # Collection
    collection1, collection2 = get_default_collections()
    add_collections(db, collections=[collection1])

    # Family + Document + events
    document1, document2 = get_default_documents()
    document1["import_id"] = "CCLW.executive.12"
    document2["import_id"] = "UNFCCC.s.ill.y.2.2"
    family1, family2 = get_default_families()
    family1["documents"] = [document1]
    family2["documents"] = [document2]
    add_families(db, families=[family1, family2])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
            (collection1["import_id"], family2["import_id"]),
        ],
    )


def setup_with_two_unpublished_docs(db: Session):
    # Collection
    collection1, collection2 = get_default_collections()
    add_collections(db, collections=[collection1])

    # Family + Document + events
    document1, document2 = get_default_documents()
    document1["status"] = "CREATED"
    document2["status"] = "DELETED"
    family1, family2 = get_default_families()
    family1["documents"] = [document1]
    family2["documents"] = [document2]
    add_families(db, families=[family1, family2])

    # Collection - Family
    link_collection_family(
        db,
        [
            (collection1["import_id"], family1["import_id"]),
            (collection1["import_id"], family2["import_id"]),
        ],
    )


def setup_docs_with_two_orgs(db: Session):
    document1, document2 = get_default_documents()
    document2["import_id"] = "UNFCCC.non-party.2.2"
    family1, family2 = get_default_families()
    family1["documents"] = [document1]
    family2["documents"] = [document2]
    # Family + Document + events
    add_families(db, families=[family1], org_id=1)
    add_families(db, families=[family2], org_id=2)


def setup_docs_with_two_orgs_no_langs(db: Session):
    document1, document2 = get_default_documents()
    document2["import_id"] = "UNFCCC.non-party.2.2"
    document1["languages"] = []
    document2["languages"] = []

    # Family + Document + events
    family1, family2 = get_default_families()
    family1["documents"] = [document1]
    family2["documents"] = [document2]
    add_families(db, families=[family1], org_id=1)
    add_families(db, families=[family2], org_id=2)


def setup_new_corpus(
    db: Session,
    title: str,
    description: str,
    corpus_text: Optional[str],
    corpus_image_url: Optional[str],
    organisation_id: int = 1,
    corpus_type_name: str = "Intl. agreements",
) -> CorpusType:
    c = Corpus(
        import_id="name",
        title=title,
        description=description,
        corpus_text=corpus_text,
        corpus_image_url=corpus_image_url,
        organisation_id=organisation_id,
        corpus_type_name=corpus_type_name,
    )
    db.add(c)
    db.commit()
    return c
