from http.client import OK
from typing import Any
from unittest.mock import MagicMock

import pytest
from db_client.models.dfce.family import Family, FamilyCategory, FamilyCorpus
from db_client.models.organisation import Corpus, Organisation

from app.core.util import tree_table_to_json
from app.db.session import SessionLocal

LEN_ORG_CONFIG = 3
EXPECTED_CCLW_TAXONOMY = {
    "instrument",
    "keyword",
    "sector",
    "topic",
    "framework",
    "hazard",
    "event_types",
}
EXPECTED_CCLW_EVENTS = [
    "Amended",
    "Appealed",
    "Closed",
    "Declaration Of Climate Emergency",
    "Dismissed",
    "Entered Into Force",
    "Filing",
    "Granted",
    "Implementation Details",
    "International Agreement",
    "Net Zero Pledge",
    "Other",
    "Passed/Approved",
    "Repealed/Replaced",
    "Set",
    "Settled",
    "Updated",
]


EXPECTED_UNFCCC_TAXONOMY = {"author", "author_type", "event_types"}


def _add_family(test_db, import_id: str, cat: FamilyCategory, corpus_import_id):
    test_db.add(
        Family(
            title="f1",
            import_id=import_id,
            description="",
            geography_id=1,
            family_category=cat,
        )
    )
    test_db.add(
        FamilyCorpus(family_import_id=import_id, corpus_import_id=corpus_import_id)
    )


def test_config_endpoint_content(data_client, data_db):
    """Tests whether we get the expected content when the /config endpoint is called."""
    # TODO: this test is fragile, we should look into validation according to the
    #       supporting data, rather than counts & fixed lists
    url_under_test = "/api/v1/config"

    response = data_client.get(
        url_under_test,
    )

    response_json = response.json()

    assert response.status_code == OK
    assert len(response_json) == 6

    assert "geographies" in response_json
    assert len(response_json["geographies"]) == 8
    assert "languages" in response_json
    assert len(response_json["languages"]) == 7893
    assert "fra" in response_json["languages"]
    assert all(len(key) == 3 for key in response_json["languages"])
    assert "document_roles" in response_json
    assert len(response_json["document_roles"]) == 10
    assert "MAIN" in response_json["document_roles"]
    assert "document_types" in response_json
    assert len(response_json["document_types"]) == 76
    assert "Adaptation Communication" in response_json["document_types"]
    assert "document_variants" in response_json
    assert len(response_json["document_variants"]) == 2
    assert "Original Language" in response_json["document_variants"]

    # Now test organisations
    assert "organisations" in response_json

    assert "CCLW" in response_json["organisations"]
    cclw_org = response_json["organisations"]["CCLW"]
    assert len(cclw_org) == LEN_ORG_CONFIG

    # Test the counts are there (just CCLW)
    assert cclw_org["total"] == 0
    assert cclw_org["count_by_category"] == {
        "Executive": 0,
        "Legislative": 0,
        "UNFCCC": 0,
    }

    assert "UNFCCC" in response_json["organisations"]
    unfccc_org = response_json["organisations"]["UNFCCC"]
    assert len(unfccc_org) == LEN_ORG_CONFIG

    cclw_corpora = cclw_org["corpora"]
    assert len(cclw_corpora) == 1
    assert cclw_corpora[0]["corpus_import_id"] == "CCLW.corpus.i00000001.n0000"
    assert cclw_corpora[0]["corpus_type"] == "Laws and Policies"
    assert (
        cclw_corpora[0]["image_url"]
        == "https://cdn.climatepolicyradar.org/corpora/CCLW.corpus.i00000001.n0000/logo.png"
    )
    assert "Grantham Research Institute" in cclw_corpora[0]["text"]
    assert cclw_corpora[0]["corpus_type_description"] == "Laws and policies"
    assert cclw_corpora[0]["description"] == "CCLW national policies"
    assert cclw_corpora[0]["title"] == "CCLW national policies"
    assert set(cclw_corpora[0]["taxonomy"]) ^ EXPECTED_CCLW_TAXONOMY == set()


def test_config_endpoint_cclw_stats(data_client, data_db):
    url_under_test = "/api/v1/config"

    cclw = (
        data_db.query(Corpus)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .filter(Organisation.name == "CCLW")
        .one()
    )
    unfccc = (
        data_db.query(Corpus)
        .join(Organisation, Organisation.id == Corpus.organisation_id)
        .filter(Organisation.name == "UNFCCC")
        .one()
    )
    unfccc = data_db.query(Corpus).filter(Corpus.organisation_id == 1).one()

    # Add some data here
    _add_family(data_db, "T.0.0.1", FamilyCategory.EXECUTIVE, cclw.import_id)
    _add_family(data_db, "T.0.0.2", FamilyCategory.EXECUTIVE, cclw.import_id)
    _add_family(data_db, "T.0.0.3", FamilyCategory.EXECUTIVE, cclw.import_id)
    _add_family(data_db, "T.0.0.4", FamilyCategory.LEGISLATIVE, cclw.import_id)
    _add_family(data_db, "T.0.0.5", FamilyCategory.LEGISLATIVE, cclw.import_id)
    _add_family(data_db, "T.0.0.6", FamilyCategory.UNFCCC, unfccc.import_id)
    data_db.flush()

    response = data_client.get(
        url_under_test,
    )

    response_json = response.json()

    org_config = response_json["organisations"]["CCLW"]
    assert len(org_config) == LEN_ORG_CONFIG
    assert org_config["total"] == 6

    laws = org_config["count_by_category"]["Legislative"]
    policies = org_config["count_by_category"]["Executive"]
    unfccc = org_config["count_by_category"]["UNFCCC"]
    assert laws == 2
    assert policies == 3
    assert unfccc == 1

    assert org_config["total"] == laws + policies + unfccc


class _MockColumn:
    def __init__(self, name):
        self.name = name


class _MockTable:
    def __init__(self, columns: list[str]):
        self.columns = [_MockColumn(c) for c in columns]


class _MockRow:
    def __init__(self, data: dict[str, Any]):
        self.__table__ = _MockTable(list(data.keys()))
        for key, value in data.items():
            setattr(self, key, value)


class _MockQuery:
    def __init__(self, query_response_data):
        self.query_response_data = query_response_data

    def all(self):
        return [_MockRow(rd) for rd in self.query_response_data]


_DATA_1 = {"id": 1, "parent_id": None, "name": "root", "data": 1}
_DATA_2 = {"id": 2, "parent_id": 1, "name": "two", "data": 2}
_DATA_3 = {"id": 3, "parent_id": 2, "name": "three", "data": 3}
TREE_TABLE_DATA_1 = [_DATA_1, _DATA_2, _DATA_3]
_EX_THREE = {"node": _DATA_3, "children": []}
_EX_TWO = {"node": _DATA_2, "children": [_EX_THREE]}
_EX_ONE = {"node": _DATA_1, "children": [_EX_TWO]}
EXPECTED_TREE_1 = [_EX_ONE]


@pytest.mark.parametrize("data,expected", [(TREE_TABLE_DATA_1, EXPECTED_TREE_1)])
def test_tree_table_to_json(data, expected):
    db = MagicMock(spec=SessionLocal)
    db_query_mock = MagicMock()
    db_query_mock.order_by = lambda _: _MockQuery(data)
    db.query = lambda _: db_query_mock

    table_mock = MagicMock()
    table_mock.id = 1
    processed_data = tree_table_to_json(table_mock, db)

    assert processed_data == expected
