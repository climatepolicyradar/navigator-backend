import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
from app.core.organisation import get_organisation_taxonomy
from tests.routes.setup_helpers import setup_with_docs

METADATA_KEYS = set(["topic", "hazard", "sector", "keyword", "framework", "instrument"])


def test_get_org_taxonomy__has_metadata_keys(data_db: Session):
    setup_with_docs(data_db)

    id, taxonomy = get_organisation_taxonomy(data_db, org_id=1)

    assert id
    assert taxonomy
    actual_keys = set(taxonomy.keys())

    assert actual_keys.symmetric_difference(METADATA_KEYS) == set([])


def test_get_org_taxonomy__raises_on_no_organisation(data_db: Session):
    ORG_ID_NOT_EXISTS = 2234
    setup_with_docs(data_db)

    with pytest.raises(NoResultFound):
        get_organisation_taxonomy(data_db, org_id=ORG_ID_NOT_EXISTS)
