from http import HTTPStatus

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
