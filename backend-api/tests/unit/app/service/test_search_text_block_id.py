import pytest
from app.service.search import _parse_text_block_id


@pytest.mark.parametrize(
    "text_block_id,expected",
    [
        # p{page}_b{block} format
        ("p0_b0", (0, 0)),
        ("p0_b1", (0, 1)),
        ("p1_b0", (1, 0)),
        ("p1_b10", (1, 10)),
        ("p2_b5", (2, 5)),
        ("p100_b1000", (100, 1000)),
        ("p1000000_b999999", (1000000, 999999)),
        # b{block} format
        ("b1", (None, 1)),
        ("b123", (None, 123)),
        ("b0", (None, 0)),
        ("b999999999", (None, 999999999)),
        # {block} format
        ("1", (None, 1)),
        ("123", (None, 123)),
        ("0", (None, 0)),
        # block_{block} format
        ("block_1", (None, 1)),
        ("block_123", (None, 123)),
        ("block_0", (None, 0)),
        # Invalid formats should return (0, 0)
        ("invalid", (None, 0)),
        ("p1_b", (None, 0)),
        ("b", (None, 0)),
        ("block_", (None, 0)),
        ("", (None, 0)),
        ("1a", (None, 0)),
        (None, (None, 0)),
    ],
)
def test_parse_text_block_id(text_block_id, expected):
    assert _parse_text_block_id(text_block_id) == expected
