import os
from unittest.mock import MagicMock, patch

import pytest

from app.run_db_migrations.run_db_migrations import run_db_migrations


@patch.dict(
    os.environ, {"DATA_IN_PIPELINE_LOAD_API_URL": "https://test-api.example.com"}
)
@patch("app.run_db_migrations.run_db_migrations.requests.post")
def test_run_db_migrations_triggers_load_api(mock_post):
    """Test run_db_migrations posts to the run-migrations endpoint."""

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response

    run_db_migrations()

    mock_post.assert_called_once_with(
        url="https://test-api.example.com/load/run-migrations",
        timeout=10,
    )
    mock_response.raise_for_status.assert_called_once()


@patch.dict(os.environ, {"DATA_IN_PIPELINE_LOAD_API_URL": "test-api.example.com"})
@patch("app.run_db_migrations.run_db_migrations.requests.post")
def test_run_db_migrations_adds_https_scheme_when_missing(mock_post):
    """Test run_db_migrations adds https:// scheme when missing."""

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response

    run_db_migrations()

    mock_post.assert_called_once_with(
        url="https://test-api.example.com/load/run-migrations",
        timeout=10,
    )


@patch.dict(
    os.environ, {"DATA_IN_PIPELINE_LOAD_API_URL": "https://test-api.example.com"}
)
@patch("app.run_db_migrations.run_db_migrations.requests.post")
def test_run_db_migrations_raises_on_request_error(mock_post):
    """Test run_db_migrations propagates exceptions from the POST request."""

    mock_post.side_effect = Exception("boom")

    with pytest.raises(Exception):
        run_db_migrations()
