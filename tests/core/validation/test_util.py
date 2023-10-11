import datetime
import json
from typing import Collection, Sequence
from unittest import mock

import pytest

from app.api.api_v1.schemas.document import DocumentParserInput
from app.core.validation import IMPORT_ID_MATCHER, PIPELINE_BUCKET

from app.core.validation.util import (
    _flatten_maybe_tree,
    write_documents_to_s3,
)


NOT_A_TREE_1 = [{"name": 1}, {"name": 2}, {"name": 3}]
NOT_A_TREE_2 = [
    {"name": 1, "value": 2},
    {"name": 2, "value": 3},
    {"name": 3, "value": 4},
]
NOT_A_TREE_3 = [{"value": 2}, {"value": 3}, {"name": 4}]
NOT_A_TREE_4 = []


@pytest.mark.parametrize(
    "not_a_tree",
    [NOT_A_TREE_1, NOT_A_TREE_2, NOT_A_TREE_3, NOT_A_TREE_4],
)
def test__flatten_maybe_tree_not_a_tree(not_a_tree: Sequence):
    """Just test that we get values from JSON that does not describe a tree"""
    assert [min(d.values()) for d in not_a_tree] == _flatten_maybe_tree(not_a_tree)


IS_A_TREE_1 = [{"node": {"name": "dave"}, "children": []}]
IS_A_TREE_2 = [
    {
        "node": {"name": "dave"},
        "children": [{"node": {"name": "steve"}, "children": []}],
    }
]
IS_A_TREE_3 = [
    {
        "node": {"name": "dave"},
        "children": [
            {"node": {"name": "steve"}, "children": []},
            {
                "node": {"name": "othello"},
                "children": [
                    {"node": {"name": "ally", "value": "ignored"}, "children": []},
                ],
            },
        ],
    },
    {"node": {"value": "stewart"}, "children": []},
]
IS_A_TREE_4 = []
IS_A_TREE_1_EXPECTED = ["dave"]
IS_A_TREE_2_EXPECTED = ["dave", "steve"]
IS_A_TREE_3_EXPECTED = ["dave", "steve", "othello", "ally", "stewart"]
IS_A_TREE_4_EXPECTED = []


@pytest.mark.parametrize(
    "is_a_tree,expected",
    [
        (IS_A_TREE_1, IS_A_TREE_1_EXPECTED),
        (IS_A_TREE_2, IS_A_TREE_2_EXPECTED),
        (IS_A_TREE_3, IS_A_TREE_3_EXPECTED),
        (IS_A_TREE_4, IS_A_TREE_4_EXPECTED),
    ],
)
def test__flatten_maybe_tree_is_a_tree(is_a_tree: Sequence, expected: Collection):
    """Test that we get values from JSON that does describe a tree"""
    assert _flatten_maybe_tree(is_a_tree) == expected


def test_write_documents_to_s3(test_s3_client, mocker):
    """Really simple check that values are passed to the s3 client correctly"""
    d = DocumentParserInput(
        publication_ts=datetime.datetime(year=2008, month=12, day=25),
        name="name",
        description="description",
        source_url=None,
        download_url=None,
        type="executive",
        source="CCLW",
        import_id="1234-5678",
        slug="geo_2008_name_1234_5678",
        family_import_id="family_1234-5678",
        family_slug="geo_2008_family_1234_5679",
        category="category",
        geography="GEO",
        languages=[],
        metadata={},
    )

    upload_file_mock = mocker.patch.object(test_s3_client, "upload_fileobj")
    datetime_mock = mocker.patch("app.core.validation.util.datetime")
    every_now = datetime.datetime(year=2001, month=12, day=25)
    datetime_mock.now.return_value = every_now

    expected_folder_name = every_now.isoformat().replace(":", ".")
    test_s3_prefix = f"input/{expected_folder_name}"
    write_documents_to_s3(test_s3_client, test_s3_prefix, documents=[d])
    upload_file_mock.assert_called_once_with(
        bucket=PIPELINE_BUCKET,
        key=f"{test_s3_prefix}/db_state.json",
        content_type="application/json",
        fileobj=mock.ANY,
    )
    uploaded_json_documents = json.loads(
        upload_file_mock.mock_calls[0].kwargs["fileobj"].read().decode("utf8")
    )
    assert uploaded_json_documents == {"documents": {d.import_id: d.to_json()}}


@pytest.mark.parametrize(
    "valid_id",
    [
        "CCLW.exec.1.2",
        "CCLW-2.exec.1.2",
        "CCLW_2.exec-1-2.1.2",
        "CCLW.exec_leg-3.1-2-3-4.2",
        "UNFCCC_p1-d3.exec.1.2-3-4-5-6",
        "UNFCCC.exec-1-2_3_4-5.1.2",
    ],
)
def test_valid_import_ids(valid_id):
    assert IMPORT_ID_MATCHER.match(valid_id)


@pytest.mark.parametrize(
    "invalid_id",
    [
        "CCLW.exec.1.2.3",  # too many elements
        "CCLW-2.exec.-1.2",  # element starts with -
        "_2.exec-1-2.1.2",  # element starts with _
        "CCLW.exec__leg-3.1-2-3-4.2",  # chained -_ chars
        "UNFCCC_p1-d3.exec.1.2-3-4-5--6",  # chained -_ chars
        "UNFCCC.1.2",  # too few elements
        "CCLW..1.2",  # empty elements
        "CCLW...2",  # many empty elements
        "CCLW.%25.1.2",  # invalid character
        "CCLW.%25.1%20%25.2",  # many invalid characters
        "CCLW.^25.1'25.2",  # many invalid characters
    ],
)
def test_invalid_import_ids(invalid_id):
    assert IMPORT_ID_MATCHER.match(invalid_id) is None
