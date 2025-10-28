import pytest

from app.flow import process_document_updates


@pytest.mark.parametrize("ids, expected", [(["11", "22", "33"], ["11", "22", "33"])])
@pytest.mark.skip(reason="Not implemented")
def test_process_document_updates_flow(ids: list[str], expected: list[str]):
    assert process_document_updates.fn(ids) == expected
