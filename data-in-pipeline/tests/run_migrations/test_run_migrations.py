import os
from unittest.mock import MagicMock, patch

import pytest

from app.run_db_migrations.run_db_migrations import run_db_migrations


@patch.dict(
    os.environ, {"DATA_IN_PIPELINE_LOAD_API_URL": "https://test-api.example.com"}
)
@patch("app.run_db_migrations.run_db_migrations.requests.post")
@patch("app.run_db_migrations.run_db_migrations.requests.get")
def test_run_migrations_when_versions_match(mock_get, mock_post):
    """Test run_migrations returns early when current_version equals head_version."""

    mock_response = MagicMock()
    mock_response.json.return_value = {
        "current_version": "1.0.0",
        "head_version": "1.0.0",
    }
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    run_db_migrations()

    mock_get.assert_called_once_with(
        url="https://test-api.example.com/load/schema-info", timeout=2
    )
    mock_response.raise_for_status.assert_called_once()
    mock_post.assert_not_called()


@patch.dict(
    os.environ, {"DATA_IN_PIPELINE_LOAD_API_URL": "https://test-api.example.com"}
)
@patch("app.run_db_migrations.run_db_migrations.requests.post")
@patch("app.run_db_migrations.run_db_migrations.requests.get")
def test_run_migrations_when_versions_differ(mock_get, mock_post):
    """Test run_migrations calls run-migrations endpoint when versions differ."""

    mock_get_response = MagicMock()
    mock_get_response.json.return_value = {
        "current_version": "1.0.0",
        "head_version": "2.0.0",
    }
    mock_get_response.raise_for_status.return_value = None
    mock_get.return_value = mock_get_response

    mock_post_response = MagicMock()
    mock_post_response.raise_for_status.return_value = None
    mock_post.return_value = mock_post_response

    run_db_migrations()

    mock_get.assert_called_once_with(
        url="https://test-api.example.com/load/schema-info", timeout=2
    )
    mock_post.assert_called_once_with(
        url="https://test-api.example.com/load/run-migrations", timeout=10
    )


@patch.dict(os.environ, {"DATA_IN_PIPELINE_LOAD_API_URL": "test-api.example.com"})
@patch("app.run_db_migrations.run_db_migrations.requests.post")
@patch("app.run_db_migrations.run_db_migrations.requests.get")
def test_run_migrations_adds_https_scheme_when_missing(mock_get, mock_post):
    """Test run_migrations adds https:// scheme to URL when missing."""

    mock_response = MagicMock()
    mock_response.json.return_value = {
        "current_version": "1.0.0",
        "head_version": "1.0.0",
    }
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    run_db_migrations()

    mock_get.assert_called_once_with(
        url="https://test-api.example.com/load/schema-info", timeout=2
    )
    mock_post.assert_not_called()


@patch.dict(
    os.environ, {"DATA_IN_PIPELINE_LOAD_API_URL": "https://test-api.example.com"}
)
@patch("app.run_db_migrations.run_db_migrations.requests.get")
def test_run_migrations_handles_schema_info_error(mock_get):
    """Test run_migrations raises exception when schema-info request fails."""

    mock_get.side_effect = Exception()

    with pytest.raises(Exception):
        run_db_migrations()


@patch.dict(
    os.environ, {"DATA_IN_PIPELINE_LOAD_API_URL": "https://test-api.example.com"}
)
@patch("app.run_db_migrations.run_db_migrations.requests.post")
@patch("app.run_db_migrations.run_db_migrations.requests.get")
def test_run_migrations_handles_run_migrations_error(mock_get, mock_post):
    """Test run_migrations raises exception when run-migrations request fails."""

    mock_get_response = MagicMock()
    mock_get_response.json.return_value = {
        "current_version": "1.0.0",
        "head_version": "2.0.0",
    }
    mock_get_response.raise_for_status.return_value = None
    mock_get.return_value = mock_get_response

    mock_post.side_effect = Exception()

    with pytest.raises(Exception):
        run_db_migrations()
