from pydantic import SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    All database connection parameters are required and will be
    validated by pydantic on instantiation.
    """

    github_sha: str = "unknown"

    # DB connection parameters
    db_master_username: str
    managed_db_password: SecretStr
    load_database_url: SecretStr
    db_port: str
    db_name: str

    # Connection pool parameters
    statement_timeout: str = "10000"


# Pydantic settings are set from the env variables passed in via
# docker / apprunner. Missing required fields will raise a
# ValidationError on import.
# pyright: reportCallIssue=false
# Pyright doesn't recognize that BaseSettings loads from environment variables, so it
# flags required fields as missing constructor arguments. This is a false positive.
settings = Settings()
