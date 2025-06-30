from unittest.mock import Mock

import pytest
from app.service.health import is_database_online, is_rds_online, is_vespa_online
from sqlalchemy.exc import SQLAlchemyError


def test_is_rds_online_returns_true_when_db_healthy(caplog):
    """Test RDS health check returns True when database is responsive."""
    mock_db = Mock()
    mock_db.execute.return_value = Mock()

    assert is_rds_online(mock_db) is True
    mock_db.execute.assert_called_once_with("SELECT 1")
    assert "🔴 RDS health check failed" not in caplog.text


def test_is_rds_online_returns_false_when_db_error(caplog):
    """Test RDS health check returns False when database throws error."""
    mock_db = Mock()
    mock_db.execute.side_effect = SQLAlchemyError("Database error")

    assert is_rds_online(mock_db) is False
    mock_db.execute.assert_called_once_with("SELECT 1")
    assert "🔴 RDS health check failed: Database error" in caplog.text


@pytest.mark.parametrize("dev_mode", [True, False])
def test_is_vespa_online_returns_false_when_vespa_error(dev_mode, monkeypatch, caplog):
    """Test Vespa health check returns False when Vespa throws error."""
    monkeypatch.setattr("app.service.health.DEVELOPMENT_MODE", dev_mode)

    mock_vespa = Mock()
    mock_vespa.client.query.side_effect = Exception("Vespa error")

    assert is_vespa_online(mock_vespa) is False

    # Check error logging behaviour based on dev mode
    if not dev_mode:
        assert "🔴 Vespa health check failed: Vespa error" in caplog.text
    else:
        assert "🔴 Vespa health check failed: Vespa error" not in caplog.text


@pytest.mark.parametrize(
    "rds_health,vespa_health,expected_health",
    [
        (True, True, True),
        (False, True, False),
        (True, False, False),
        (False, False, False),
    ],
)
def test_is_database_online(rds_health, vespa_health, expected_health, monkeypatch):
    """Test overall database health check returns correct status."""
    mock_db = Mock()
    mock_vespa = Mock()

    # Create mock functions with the expected return values
    mock_rds_check = Mock(return_value=rds_health)
    mock_vespa_check = Mock(return_value=vespa_health)

    # Patch the individual health check functions
    monkeypatch.setattr("app.service.health.is_rds_online", mock_rds_check)
    monkeypatch.setattr("app.service.health.is_vespa_online", mock_vespa_check)

    assert is_database_online(mock_db, mock_vespa) is expected_health

    # Verify the mocked functions were called with correct arguments
    mock_rds_check.assert_called_once_with(mock_db)
    mock_vespa_check.assert_called_once_with(mock_vespa)


def test_is_vespa_online_with_timeout():
    """Test Vespa health check uses correct timeout in query."""
    mock_vespa = Mock()
    mock_vespa.client.query.return_value = Mock()

    assert is_vespa_online(mock_vespa, timeout_seconds=5) is True

    expected_query = {
        "yql": "select * from sources * where true",
        "hits": "0",
        "timeout": "5",
    }
    mock_vespa.client.query.assert_called_once_with(expected_query)


def test_is_vespa_online_with_default_timeout():
    """Test Vespa health check uses default timeout in query."""
    mock_vespa = Mock()
    mock_vespa.client.query.return_value = Mock()

    assert is_vespa_online(mock_vespa) is True

    expected_query = {
        "yql": "select * from sources * where true",
        "hits": "0",
        "timeout": "1",  # default
    }
    mock_vespa.client.query.assert_called_once_with(expected_query)
