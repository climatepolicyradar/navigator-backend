from http.client import OK
from typing import Any
from unittest.mock import MagicMock
from db_client.models.law_policy.family import (
    Family,
    FamilyCategory,
    FamilyOrganisation,
)
import pytest

from app.core.util import tree_table_to_json
from db_client.data_migrations import (
    populate_document_role,
    populate_document_type,
    populate_document_variant,
    populate_event_type,
    populate_geography,
    populate_language,
    populate_taxonomy,
)
from app.db.session import SessionLocal


def _add_family(test_db, import_id: str, cat: FamilyCategory):
    test_db.add(
        Family(
            title="f1",
            import_id=import_id,
            description="",
            geography_id=1,
            family_category=cat,
        )
    )
    test_db.add(FamilyOrganisation(organisation_id=1, family_import_id=import_id))


def test_config_endpoint_content(client, test_db):
    """Tests whether we get the expected content when the /config endpoint is called."""
    # TODO: this test is fragile, we should look into validation according to the
    #       supporting data, rather than counts & fixed lists
    url_under_test = "/api/v1/config"
    populate_document_role(test_db)
    populate_document_type(test_db)
    populate_document_variant(test_db)
    populate_event_type(test_db)
    populate_geography(test_db)
    populate_language(test_db)
    populate_taxonomy(test_db)
    test_db.flush()

    response = client.get(
        url_under_test,
    )

    response_json = response.json()

    assert response.status_code == OK
    assert len(response_json) == 7

    assert "geographies" in response_json
    assert len(response_json["geographies"]) == 8

    assert "taxonomies" in response_json

    assert "CCLW" in response_json["taxonomies"]
    cclw_taxonomy = response_json["taxonomies"]["CCLW"]
    assert set(cclw_taxonomy) == {
        "instrument",
        "keyword",
        "sector",
        "topic",
        "framework",
        "hazard",
        "event_types",
    }
    cclw_taxonomy_event_types = cclw_taxonomy["event_types"]["allowed_values"]
    cclw_expected_event_types = [
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
    assert set(cclw_taxonomy_event_types) ^ set(cclw_expected_event_types) == set()

    assert "UNFCCC" in response_json["taxonomies"]
    unfccc_taxonomy = response_json["taxonomies"]["UNFCCC"]
    assert set(unfccc_taxonomy) == {"author", "author_type", "event_types"}
    assert set(unfccc_taxonomy["author_type"]["allowed_values"]) == {
        "Party",
        "Non-Party",
    }

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

    stats = response_json["cclw_stats"]
    assert len(stats) == 3
    assert stats["total"] == 0
    assert stats["laws"] == 0
    assert stats["policies"] == 0


def test_config_endpoint_cclw_stats(client, test_db):
    url_under_test = "/api/v1/config"
    populate_document_role(test_db)
    populate_document_type(test_db)
    populate_document_variant(test_db)
    populate_event_type(test_db)
    populate_geography(test_db)
    populate_language(test_db)
    populate_taxonomy(test_db)
    test_db.flush()

    # Add some data here
    _add_family(test_db, "T.0.0.1", FamilyCategory.EXECUTIVE)
    _add_family(test_db, "T.0.0.2", FamilyCategory.EXECUTIVE)
    _add_family(test_db, "T.0.0.3", FamilyCategory.EXECUTIVE)
    _add_family(test_db, "T.0.0.4", FamilyCategory.LEGISLATIVE)
    _add_family(test_db, "T.0.0.5", FamilyCategory.LEGISLATIVE)
    test_db.flush()

    response = client.get(
        url_under_test,
    )

    response_json = response.json()

    stats = response_json["cclw_stats"]
    assert len(stats) == 3
    assert stats["total"] == 5
    assert stats["laws"] == 2
    assert stats["policies"] == 3
    assert stats["total"] == stats["laws"] + stats["policies"]


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
