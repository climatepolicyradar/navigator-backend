from sqlalchemy.orm import Session

from app.core.organisation import get_corpora_for_org
from tests.non_search.setup_helpers import setup_with_docs

METADATA_KEYS = set(
    ["topic", "hazard", "sector", "keyword", "framework", "instrument", "event_type"]
)


def test_get_corpora_for_org__has_metadata_keys(data_db: Session):
    setup_with_docs(data_db)

    copora = get_corpora_for_org(data_db, "CCLW")
    taxonomy = copora[0].taxonomy

    assert taxonomy
    actual_keys = set(taxonomy.keys())

    assert actual_keys.symmetric_difference(METADATA_KEYS) == set([])


def test_get_corpora_for_org__empty_when_missing(data_db: Session):
    setup_with_docs(data_db)

    copora = get_corpora_for_org(data_db, "MISSING_ORG_NAME")
    assert len(copora) == 0
