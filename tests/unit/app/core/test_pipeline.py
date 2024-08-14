from sqlalchemy.orm import Session

from app.core.ingestion.pipeline import generate_pipeline_ingest_input
from tests.non_search.setup_helpers import setup_with_two_docs_one_family, setup_with_two_unpublished_docs


def test_generate_pipeline_ingest_input(data_db: Session):
    setup_with_two_docs_one_family(data_db)

    documents = generate_pipeline_ingest_input(data_db)
    assert len(documents) == 2
    assert documents[0].name == "Fam1"
    assert documents[0].import_id == "CCLW.executive.2.2"
    assert documents[1].name == "Fam1"
    assert documents[1].import_id == "CCLW.executive.1.2"


def test_generate_pipeline_ingest_input__deleted(data_db: Session):
    setup_with_two_unpublished_docs(data_db)

    documents = generate_pipeline_ingest_input(data_db)
    assert len(documents) == 1
    assert documents[0].name == "Fam1"
    assert documents[0].import_id == "CCLW.executive.1.2"

