from typing import Optional

from db_client.functions.dfce_helpers import add_collections, add_document, add_families
from fastapi import status
from sqlalchemy.orm import Session

from tests.non_search.setup_helpers import (
    get_default_collections,
    get_default_documents,
)

WORLD_MAP_STATS_ENDPOINT = "/api/v1/geographies"
TEST_HOST = "http://localhost:3000/"


def _make_world_map_lookup_request(
    client,
    token,
    expected_status_code: int = status.HTTP_200_OK,
    origin: Optional[str] = TEST_HOST,
):
    headers = (
        {"app-token": token}
        if origin is None
        else {"app-token": token, "origin": origin}
    )

    response = client.get(f"{WORLD_MAP_STATS_ENDPOINT}", headers=headers)
    assert response.status_code == expected_status_code, response.text
    return response.json()


def _add_published_fams_and_docs(db: Session):
    # Collection
    collection1, _ = get_default_collections()
    add_collections(db, collections=[collection1])

    # Family + Document + events
    document0 = {
        "title": "UnfcccDocument0",
        "slug": "UnfcccDocSlug0",
        "md5_sum": None,
        "url": "http://another_somewhere",
        "content_type": None,
        "import_id": "UNFCCC.executive.3.3",
        "language_variant": None,
        "status": "PUBLISHED",
        "metadata": {"role": ["MAIN"], "type": ["Order"]},
        "languages": [],
        "events": [
            {
                "import_id": "UNFCCC.Event.3.0",
                "title": "Published",
                "date": "2019-12-25",
                "type": "Passed/Approved",
                "status": "OK",
                "valid_metadata": {
                    "event_type": ["Passed/Approved"],
                    "datetime_event_name": ["Passed/Approved"],
                },
            }
        ],
    }
    document1, document2 = get_default_documents()
    document3 = {
        "title": "Document3",
        "slug": "DocSlug3",
        "md5_sum": None,
        "url": "http://another_somewhere",
        "content_type": None,
        "import_id": "CCLW.executive.3.3",
        "language_variant": None,
        "status": "PUBLISHED",
        "metadata": {"role": ["MAIN"], "type": ["Order"]},
        "languages": [],
        "events": [
            {
                "import_id": "CPR.Event.3.0",
                "title": "Published",
                "date": "2019-12-25",
                "type": "Passed/Approved",
                "status": "OK",
                "valid_metadata": {
                    "event_type": ["Passed/Approved"],
                    "datetime_event_name": ["Passed/Approved"],
                },
            }
        ],
    }

    unfccc_fam = {
        "import_id": "UNFCCC.family.0000.0",
        "corpus_import_id": "UNFCCC.corpus.i00000001.n0000",
        "title": "UnfcccFam0",
        "slug": "UnfcccFamSlug0",
        "description": "Summary0",
        "geography_id": [2, 5],
        "category": "UNFCCC",
        "documents": [],
        "metadata": {
            "size": "small",
            "color": "blue",
        },
    }
    family0 = {
        "import_id": "CCLW.family.0000.0",
        "corpus_import_id": "CCLW.corpus.i00000001.n0000",
        "title": "Fam0",
        "slug": "FamSlug0",
        "description": "Summary0",
        "geography_id": 5,
        "category": "UNFCCC",
        "documents": [],
        "metadata": {
            "size": "small",
            "color": "blue",
        },
    }
    family1 = {
        "import_id": "CCLW.family.1001.0",
        "corpus_import_id": "CCLW.corpus.i00000001.n0000",
        "title": "Fam1",
        "slug": "FamSlug1",
        "description": "Summary1",
        "geography_id": 5,
        "category": "Executive",
        "documents": [],
        "metadata": {
            "size": "small",
            "color": "blue",
        },
    }
    family2 = {
        "import_id": "CCLW.family.2002.0",
        "corpus_import_id": "CCLW.corpus.i00000001.n0000",
        "title": "Fam2",
        "slug": "FamSlug2",
        "description": "Summary2",
        "geography_id": 5,
        "category": "Legislative",
        "documents": [],
        "metadata": {
            "size": "small",
            "color": "blue",
        },
    }
    family3 = {
        "import_id": "CCLW.family.3003.0",
        "corpus_import_id": "CCLW.corpus.i00000001.n0000",
        "title": "Fam3",
        "slug": "FamSlug3",
        "description": "Summary3",
        "geography_id": [2, 5],
        "category": "UNFCCC",
        "documents": [],
        "metadata": {
            "size": "small",
            "color": "blue",
        },
    }

    unfccc_fam["documents"] = [document0]
    family0["documents"] = []
    family1["documents"] = [document1]
    family2["documents"] = [document2]
    family3["documents"] = [document3]
    add_families(db, families=[family0, family1, family2, family3, unfccc_fam])


def setup_all_docs_published_world_map(db: Session):
    _add_published_fams_and_docs(db)


def setup_mixed_doc_statuses_world_map(db: Session):
    _add_published_fams_and_docs(db)

    # Family + Document + events
    document0 = {
        "title": "Document0",
        "slug": "DocSlug0",
        "md5_sum": None,
        "url": "http://another_somewhere",
        "content_type": None,
        "import_id": "CCLW.executive.0.0",
        "language_variant": None,
        "status": "CREATED",
        "metadata": {"role": ["MAIN"], "type": ["Order"]},
        "languages": [],
        "events": [
            {
                "import_id": "CPR.Event.0.0",
                "title": "Published",
                "date": "2019-12-25",
                "type": "Passed/Approved",
                "status": "OK",
                "valid_metadata": {
                    "event_type": ["Passed/Approved"],
                    "datetime_event_name": ["Passed/Approved"],
                },
            }
        ],
    }
    document4 = {
        "title": "Document4",
        "slug": "DocSlug4",
        "md5_sum": None,
        "url": "http://another_somewhere",
        "content_type": None,
        "import_id": "CCLW.executive.4.4",
        "language_variant": None,
        "status": "DELETED",
        "metadata": {"role": ["MAIN"], "type": ["Order"]},
        "languages": [],
        "events": [
            {
                "import_id": "CPR.Event.4.0",
                "title": "Published",
                "date": "2019-12-25",
                "type": "Passed/Approved",
                "status": "OK",
                "valid_metadata": {
                    "event_type": ["Passed/Approved"],
                    "datetime_event_name": ["Passed/Approved"],
                },
            }
        ],
    }
    document5 = {
        "title": "Document5",
        "slug": "DocSlug5",
        "md5_sum": None,
        "url": "http://another_somewhere",
        "content_type": None,
        "import_id": "CCLW.executive.5.5",
        "language_variant": None,
        "status": "CREATED",
        "metadata": {"role": ["MAIN"], "type": ["Order"]},
        "languages": [],
        "events": [
            {
                "import_id": "CPR.Event.5.0",
                "title": "Published",
                "date": "2019-12-25",
                "type": "Passed/Approved",
                "status": "OK",
                "valid_metadata": {
                    "event_type": ["Passed/Approved"],
                    "datetime_event_name": ["Passed/Approved"],
                },
            }
        ],
    }
    document6 = {
        "title": "Document6",
        "slug": "DocSlug6",
        "md5_sum": None,
        "url": "http://another_somewhere",
        "content_type": None,
        "import_id": "CCLW.executive.6.6",
        "language_variant": None,
        "status": "PUBLISHED",
        "metadata": {"role": ["MAIN"], "type": ["Order"]},
        "languages": [],
        "events": [
            {
                "import_id": "CPR.Event.6.0",
                "title": "Published",
                "date": "2019-12-25",
                "type": "Passed/Approved",
                "status": "OK",
                "valid_metadata": {
                    "event_type": ["Passed/Approved"],
                    "datetime_event_name": ["Passed/Approved"],
                },
            }
        ],
    }
    document7 = {
        "title": "Document7",
        "slug": "DocSlug7",
        "md5_sum": None,
        "url": "http://another_somewhere",
        "content_type": None,
        "import_id": "CCLW.executive.7.7",
        "language_variant": None,
        "status": "PUBLISHED",
        "metadata": {"role": ["MAIN"], "type": ["Order"]},
        "languages": [],
        "events": [
            {
                "import_id": "CPR.Event.7.0",
                "title": "Published",
                "date": "2019-12-25",
                "type": "Passed/Approved",
                "status": "OK",
                "valid_metadata": {
                    "event_type": ["Passed/Approved"],
                    "datetime_event_name": ["Passed/Approved"],
                },
            }
        ],
    }

    for import_id, docs in [
        ("CCLW.family.3003.0", [document4, document5, document6]),
        ("CCLW.family.0000.0", [document0, document7]),
    ]:
        for doc in docs:
            add_document(db, import_id, doc)
