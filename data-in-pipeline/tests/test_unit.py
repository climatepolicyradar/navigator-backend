import pytest
from src.main import add


@pytest.mark.parametrize("x, y, expected", [(1, 1, 2), (2, 2, 4), (3, 3, 6)])
def test_add_flow(x, y, expected):
    assert add.fn(x, y) == expected
