from sqlalchemy.orm import Session

from app.core.ingestion.pipeline import generate_pipeline_ingest_input
from tests.core.ingestion.helpers import populate_for_ingest, add_family_document


def test_generate_pipeline_ingest_input(test_db: Session):
    family_name = "test_family_name"

    populate_for_ingest(test_db)
    add_family_document(
        test_db,
        family_name=family_name,
    )

    documents = generate_pipeline_ingest_input(test_db)

    assert len(documents) == 1
    assert documents[0].name == family_name
