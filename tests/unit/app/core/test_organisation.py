from typing import cast

import pytest
from sqlalchemy.orm import Session

from app.core.organisation import get_all_organisations, get_corpora_for_org
from tests.non_search.setup_helpers import setup_with_docs

CCLW_EXPECTED_NUM_CORPORA = 1
CCLW_METADATA_KEYS = set(
    [
        "topic",
        "hazard",
        "sector",
        "keyword",
        "framework",
        "instrument",
        "_document",
        "event_type",
    ]
)
UNFCCC_EXPECTED_NUM_CORPORA = 1
UNFCCC_METADATA_KEYS = set(
    [
        "author",
        "author_type",
        "_document",
        "event_type",
    ]
)
EXPECTED_NUM_ORGS = 2


def test_expected_organisations_present(data_db: Session):
    orgs = get_all_organisations(data_db)
    assert len(orgs) == EXPECTED_NUM_ORGS

    org_names = set([cast(str, org.name) for org in orgs])
    expected_org_names = set(["CCLW", "UNFCCC"])
    assert org_names.symmetric_difference(expected_org_names) == set([])


@pytest.mark.parametrize(
    "org_name, expected_num_corpora, expected_tax_keys",
    [
        ("CCLW", CCLW_EXPECTED_NUM_CORPORA, CCLW_METADATA_KEYS),
        ("UNFCCC", UNFCCC_EXPECTED_NUM_CORPORA, UNFCCC_METADATA_KEYS),
    ],
)
def test_get_corpora_for_org__has_metadata_keys(
    data_db: Session,
    org_name: str,
    expected_num_corpora: int,
    expected_tax_keys: set[str],
):
    setup_with_docs(data_db)

    corpora = get_corpora_for_org(data_db, org_name)
    assert len(corpora) == expected_num_corpora
    taxonomy = corpora[0].taxonomy

    assert isinstance(taxonomy, dict)

    actual_keys = set(taxonomy.keys())
    assert actual_keys.symmetric_difference(expected_tax_keys) == set([])


def test_get_corpora_for_org__empty_when_missing(data_db: Session):
    setup_with_docs(data_db)

    corpora = get_corpora_for_org(data_db, "MISSING_ORG_NAME")
    assert len(corpora) == 0
