"""Check our environment variables have the minimum required values.

This module is used to check that the environment variables are present
and have the correct types.

These environment variables are set by Docker & the ECS task definition
for Prefect tasks. Any field defined here will be validated by pydantic
on instantiation and if it isn't present or is the wrong type, will
raise a ValidationError.
"""

from typing import Any

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    All database connection parameters are required and will be
    validated by pydantic on instantiation.
    """

    # DB connection parameters
    db_master_username: str
    managed_db_password: str
    aurora_writer_endpoint: str
    db_port: str
    db_name: str

    # Connection pool parameters
    statement_timeout: str = "10000"

    # SSL mode for database connections
    # Valid values: disable, allow, prefer, require, verify-ca, verify-full
    # 'prefer' tries SSL but falls back to non-SSL for local dev (validates
    # certs if SSL is used). For production RDS without cert validation,
    # set db_sslmode=require via environment variable.
    db_sslmode: str = "require"


_settings_instance: Settings | None = None


def get_settings() -> Settings:
    """Get the settings instance, creating it on first access.

    Lazy instantiation allows the module to be imported without
    requiring environment variables to be set (e.g., during deployment
    scripts). Settings are only validated when actually accessed at
    runtime.

    :return: The settings instance.
    :rtype: Settings
    """
    global _settings_instance
    if _settings_instance is None:
        # Pyright doesn't recognize that BaseSettings loads from
        # environment variables, so it flags required fields as missing
        # constructor arguments. This is a false positive.
        # @see: https://github.com/pydantic/pydantic/issues/3753
        _settings_instance = Settings()  # pyright: ignore[reportCallIssue]
    return _settings_instance


class _SettingsProxy:
    """Proxy object that lazily accesses settings.

    This is used to allow the settings module to be imported without
    requiring environment variables to be set (e.g., during deployment
    scripts). Settings are only validated when actually accessed at
    runtime.
    """

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the settings instance.

        :param name: Name of the attribute to access.
        :type name: str
        :return: The attribute value from the settings instance.
        :rtype: Any
        """
        return getattr(get_settings(), name)


settings = _SettingsProxy()
