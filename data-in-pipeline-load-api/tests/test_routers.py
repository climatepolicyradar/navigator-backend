from app.models import Document
from app.routers import create_document


def test_create_document_returns_201_created_and_a_list_of_created_document_ids_on_success():

    test_document_1 = Document(id="1", title="Test doc 1")
    test_document_2 = Document(id="2", title="Test doc 2")

    result = create_document([test_document_1, test_document_2])

    expected_result = [test_document_1.id, test_document_2.id]

    assert result == expected_result
