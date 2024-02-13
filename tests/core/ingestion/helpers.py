from sqlalchemy.orm import Session

from db_client.data_migrations import (
    populate_document_role,
    populate_document_variant,
    populate_document_type,
    populate_event_type,
    populate_geography,
    populate_taxonomy,
    populate_language,
)

from tests.routes.setup_search_tests import (
    _create_family,
    _create_family_event,
    _create_family_metadata,
    _create_document,
    _create_organisation,
)


def populate_for_ingest(test_db):
    populate_taxonomy(test_db)
    populate_geography(test_db)
    populate_document_type(test_db)
    populate_event_type(test_db)
    populate_language(test_db)
    populate_document_role(test_db)
    populate_document_variant(test_db)
    test_db.flush()


def add_family_document(test_db: Session, family_name: str):
    new_family = {
        "id": "id:doc_search:family_document::CCLW.executive.111.222",
        "fields": {
            "family_source": "CCLW",
            "family_name": family_name,
            "family_slug": "testfamily-1",
            "family_category": "Executive",
            "document_languages": ["French"],
            "document_import_id": "CCLW.executive.111.222",
            "family_description": "",
            "family_geography": "CAN",
            "family_publication_ts": "2011-08-01T00:00:00+00:00",
            "family_import_id": "CCLW.family.111.0",
        },
    }
    new_doc = {"id": "CCLW.executive.111.222.333", "fields": {}}
    _create_organisation(test_db)
    _create_family(test_db, new_family)
    _create_family_event(test_db, new_family)
    _create_family_metadata(test_db, new_family)
    _create_document(test_db, new_doc, new_family)
