from sqlalchemy.orm import Session

from app.core.ingestion.pipeline import (
    flatten_pipeline_metadata,
    generate_pipeline_ingest_input,
)
from tests.non_search.setup_helpers import (
    setup_docs_with_two_orgs,
    setup_with_two_docs_one_family,
    setup_with_two_unpublished_docs,
)


def test_generate_pipeline_ingest_input(data_db: Session):
    setup_with_two_docs_one_family(data_db)

    state_rows = generate_pipeline_ingest_input(data_db)
    assert len(state_rows) == 2
    # Sort to ensure order is consistent across tests
    sorted(state_rows, key=lambda d: d.import_id)

    # Now test one field from each table we've queried
    # Check family title
    assert state_rows[0].name == "Fam1"
    assert state_rows[1].name == "Fam1"

    # Check family_document import_id
    assert state_rows[0].import_id == "CCLW.executive.2.2"
    assert state_rows[1].import_id == "CCLW.executive.1.2"

    # Check family_metadata
    assert state_rows[0].metadata["family.size"] == "big"

    # Check geography
    assert state_rows[0].geographies == ["South Asia"]

    # Check organisation
    assert state_rows[0].source == "CCLW"

    # Check corpus
    assert state_rows[0].corpus_import_id == "CCLW.corpus.i00000001.n0000"

    # Check physical_document
    assert state_rows[0].document_title == "Document2"

    # Check collection
    assert state_rows[0].collection_title == "Collection1"


def test_generate_pipeline_ingest_input_no_collection_family_link(data_db: Session):
    setup_docs_with_two_orgs(data_db)

    state_rows = generate_pipeline_ingest_input(data_db)
    assert len(state_rows) == 2


def test_generate_pipeline_ingest_input__deleted(data_db: Session):
    setup_with_two_unpublished_docs(data_db)

    documents = generate_pipeline_ingest_input(data_db)
    assert len(documents) == 1
    assert documents[0].name == "Fam1"
    assert documents[0].import_id == "CCLW.executive.1.2"


def test_flatten_pipeline_metadata():
    family_metadata = {"a": ["1"], "b": ["2"]}
    doc_metadata = {"a": ["3"], "b": ["4"]}
    result = flatten_pipeline_metadata(family_metadata, doc_metadata)

    assert len(result) == 4
    assert result["family.a"] == ["1"]
    assert result["family.b"] == ["2"]
    assert result["document.a"] == ["3"]
    assert result["document.b"] == ["4"]
