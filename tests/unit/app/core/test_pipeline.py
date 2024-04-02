from sqlalchemy.orm import Session

from app.core.ingestion.pipeline import generate_pipeline_ingest_input
from tests.routes.setup_helpers import setup_with_two_docs_one_family


def test_generate_pipeline_ingest_input(data_db: Session):
    setup_with_two_docs_one_family(data_db)

    documents = generate_pipeline_ingest_input(data_db)

    assert len(documents) == 2
    assert documents[0].name == "Fam1"
    assert documents[1].name == "Fam1"
