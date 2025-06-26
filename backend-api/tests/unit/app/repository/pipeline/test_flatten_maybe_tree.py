from typing import Collection, Sequence

import pytest

from app.service.pipeline import _flatten_maybe_tree

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
