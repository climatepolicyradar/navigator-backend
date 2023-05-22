from http.client import OK
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.core.util import tree_table_to_json
from app.data_migrations import (
    populate_event_type,
    populate_geography,
    populate_taxonomy,
)
from app.db.session import SessionLocal


def test_endpoint_returns_taxonomy(client, test_db):
    """Tests whether we get the taxonomy when the /config endpoint is called."""
    url_under_test = "/api/v1/config"
    populate_taxonomy(test_db)
    populate_geography(test_db)
    populate_event_type(test_db)
    test_db.flush()

    response = client.get(
        url_under_test,
    )

    response_json = response.json()

    assert response.status_code == OK
    assert len(response_json) == 2

    assert "geographies" in response_json
    assert len(response_json["geographies"]) == 9

    assert "taxonomies" in response_json
    assert "CCLW" in response_json["taxonomies"]
    tax = response_json["taxonomies"]["CCLW"]

    assert set(tax) == {
        "instrument",
        "keyword",
        "sector",
        "topic",
        "framework",
        "hazard",
        "event_types",
    }
    taxonomy_event_types = tax["event_types"]["allowed_values"]
    expected_event_types = [
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
    assert set(taxonomy_event_types).symmetric_difference(
        set(expected_event_types)
    ) == set([])


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
