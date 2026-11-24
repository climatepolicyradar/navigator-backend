from unittest.mock import Mock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from app.service.health import is_database_online, is_rds_online


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


@pytest.mark.parametrize("rds_health", [True, False])
def test_is_database_online(rds_health, monkeypatch):
    """Test overall database health check reflects RDS health."""
    mock_db = Mock()
    mock_rds_check = Mock(return_value=rds_health)
    monkeypatch.setattr("app.service.health.is_rds_online", mock_rds_check)

    assert is_database_online(mock_db) is rds_health
    mock_rds_check.assert_called_once_with(mock_db)
