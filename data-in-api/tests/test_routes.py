from http import HTTPStatus
from unittest.mock import Mock

import pytest
from data_in_models.models import Document as DocumentOutput
from fastapi.testclient import TestClient
from sqlmodel import Session

from app.main import app
from app.session import get_db


def test_health_check_returns_ok_status(session: Session):
    """Test health check endpoint returns correct status and version."""

    def get_db_override():
        yield session

    app.dependency_overrides[get_db] = get_db_override
    client = TestClient(app)

    try:
        response = client.get("/db-health-check")
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["status"] == "ok"
        assert "version" in data
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def client():
    """Provide a TestClient with overridden get_db dependency."""
    mock_session = Mock()

    def get_db_override():
        yield mock_session

    app.dependency_overrides[get_db] = get_db_override
    yield TestClient(app)
    app.dependency_overrides.clear()


DOCUMENTS_LIST = [
    DocumentOutput(
        id="doc_1",
        title="Test Document 1",
        description="Description 1",
        labels=[],
        items=[],
        relationships=[],
    ),
    DocumentOutput(
        id="doc_2",
        title="Test Document 2",
        description=None,
        labels=[],
        items=[],
        relationships=[],
    ),
]

SINGLE_DOCUMENT = DocumentOutput(
    id="doc_123",
    title="Test Document",
    description="Test description",
    labels=[],
    items=[],
    relationships=[],
)


def test_list_documents_returns_all_documents(client, monkeypatch):
    """Test GET /documents returns list of documents."""
    mock_get_all = Mock(return_value=DOCUMENTS_LIST)
    monkeypatch.setattr("app.main.get_all_documents", mock_get_all)

    response = client.get("/documents?page=1&page_size=20")
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data["total"] == len(DOCUMENTS_LIST)
    assert len(data["data"]) == len(DOCUMENTS_LIST)
    assert data["data"][0]["id"] == "doc_1"
    assert data["data"][1]["id"] == "doc_2"


def test_list_documents_with_pagination(client, monkeypatch):
    """Test GET /documents respects pagination parameters."""
    documents = [
        DocumentOutput(
            id=f"doc_{i}",
            title=f"Document {i}",
            description=None,
            labels=[],
            items=[],
            relationships=[],
        )
        for i in range(10)
    ]
    mock_get_all = Mock(return_value=documents)
    monkeypatch.setattr("app.main.get_all_documents", mock_get_all)

    page = 2
    page_size = 5

    response = client.get(f"/documents?page={page}&page_size={page_size}")
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data["page"] == page
    assert data["page_size"] == page_size


def test_get_document_returns_single_document(client, monkeypatch):
    """Test GET /documents/{id} returns single document."""
    mock_get_by_id = Mock(return_value=SINGLE_DOCUMENT)
    monkeypatch.setattr("app.main.get_document_by_id", mock_get_by_id)

    response = client.get("/documents/doc_123")
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data["data"]["id"] == "doc_123"
    assert data["data"]["title"] == "Test Document"
    assert data["data"]["description"] == "Test description"


def test_get_document_returns_404_when_not_found(client, monkeypatch):
    """Test GET /documents/{id} returns 404 for non-existent document."""
    mock_get_by_id = Mock(return_value=None)
    monkeypatch.setattr("app.main.get_document_by_id", mock_get_by_id)

    response = client.get("/documents/nonexistent_id")
    assert response.status_code == HTTPStatus.NOT_FOUND
    data = response.json()
    assert "not found" in data["detail"].lower()


def test_list_documents_returns_empty_list(client, monkeypatch):
    """Test GET /documents returns empty list when no documents exist."""
    mock_get_all = Mock(return_value=[])
    monkeypatch.setattr("app.main.get_all_documents", mock_get_all)

    response = client.get("/documents")
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data["total"] == 0
    assert data["data"] == []
