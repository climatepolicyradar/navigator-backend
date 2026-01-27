from http import HTTPStatus
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlmodel import Session

from app.main import app
from app.session import get_db, get_engine


def test_health_check_returns_ok_status(session: Session):
    """Test health check endpoint returns correct status and version."""

    def get_db_override():
        yield session

    app.dependency_overrides[get_db] = get_db_override
    client = TestClient(app)

    try:
        response = client.get("/load/health")
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["status"] == "ok"
        assert "version" in data
    finally:
        app.dependency_overrides.clear()


def test_run_migrations_success(blank_engine):
    """Test successful migration run."""

    def get_engine_override():
        return blank_engine

    app.dependency_overrides[get_engine] = get_engine_override
    client = TestClient(app)

    try:
        response = client.post("/load/run-migrations")
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["status"] == "ok"
        assert data["detail"] == "Migrations ran successfully"
    finally:
        app.dependency_overrides.clear()


def test_run_migrations_failure(blank_engine):
    """Test migration failure handling."""

    def get_engine_override():
        return blank_engine

    app.dependency_overrides[get_engine] = get_engine_override
    client = TestClient(app)

    with patch("app.routers.run_migrations") as mock_run_migrations:
        mock_run_migrations.side_effect = Exception("Migration error")

        try:
            response = client.post("/load/run-migrations")
            assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
            data = response.json()
            assert data["detail"] == "Failed to run migrations"
        finally:
            app.dependency_overrides.clear()
