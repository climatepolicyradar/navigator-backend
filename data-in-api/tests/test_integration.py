from http import HTTPStatus

import pytest
from data_in_models.db_models import Document as DBDocument
from fastapi.testclient import TestClient
from sqlmodel import Session

from app.main import app
from app.session import get_db


@pytest.fixture
def client(session: Session):
    def get_db_override():
        yield session

    app.dependency_overrides[get_db] = get_db_override

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


def test_list_documents_allows_filtering_by_status(
    session: Session, client: TestClient
):
    session.add(
        DBDocument(
            id="doc1",
            title="Document 1",
            description="First doc",
            attributes={"status": "published"},
        )
    )
    session.add(
        DBDocument(
            id="doc2",
            title="Document 2",
            description="Second doc",
            attributes={"status": "deleted"},
        )
    )
    session.commit()

    response = client.get(
        "/data-in/documents?page=1&page_size=20&attributes.status=published"
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json()["data"] == [
        {
            "id": "doc1",
            "title": "Document 1",
            "description": "First doc",
            "labels": [],
            "documents": [],
            "items": [],
            "attributes": {"status": "published"},
        }
    ]
