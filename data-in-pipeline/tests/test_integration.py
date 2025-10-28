from app.flow import process_document_updates


def test_add_flow():
    assert process_document_updates(["11", "22", "33"]) == ["11", "22", "33"]
