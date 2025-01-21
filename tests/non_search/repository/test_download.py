from datetime import datetime, timedelta

from db_client.models.dfce.family import Corpus

from app.repository.download import get_whole_database_dump
from tests.non_search.setup_helpers import setup_with_two_unpublished_docs


def test_get_whole_db_dump_does_not_return_data_for_documents_that_are_not_published(
    data_db,
):
    setup_with_two_unpublished_docs(data_db)
    all_corpora = [corpus.import_id for corpus in data_db.query(Corpus).all()]
    ingest_cycle_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    db_dump = get_whole_database_dump(
        ingest_cycle_date,
        all_corpora,
        data_db,
    )

    assert db_dump.empty
