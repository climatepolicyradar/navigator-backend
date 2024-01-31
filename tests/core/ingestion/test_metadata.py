import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
from app.core.organisation import get_organisation_taxonomy
from tests.core.ingestion.helpers import (
    populate_for_ingest,
)

METADATA_KEYS = set(["topic", "hazard", "sector", "keyword", "framework", "instrument"])


def test_get_org_taxonomy__has_metadata_keys(test_db: Session):
    populate_for_ingest(test_db)

    id, taxonomy = get_organisation_taxonomy(test_db, org_id=1)

    assert id
    assert taxonomy
    actual_keys = set(taxonomy.keys())

    assert actual_keys.symmetric_difference(METADATA_KEYS) == set([])


def test_get_org_taxonomy__raises_on_no_organisation(test_db: Session):
    ORG_ID_NOT_EXISTS = 2234
    populate_for_ingest(test_db)

    with pytest.raises(NoResultFound):
        get_organisation_taxonomy(test_db, org_id=ORG_ID_NOT_EXISTS)
