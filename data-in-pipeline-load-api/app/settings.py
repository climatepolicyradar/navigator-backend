import logging
import sys

from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings

# Configure logging before anything else - this module is imported early
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
    force=True,  # Override any existing configuration
)
_LOGGER = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    All database connection parameters are required and will be
    validated by pydantic on instantiation.
    """

    github_sha: str = "unknown"

    # DB connection parameters
    db_master_username: str
    managed_db_password_secret_arn: str | None = None
    managed_db_password: SecretStr | None = (
        None  # Deprecated: use managed_db_password_secret_arn
    )
    load_database_url: SecretStr
    db_port: str
    db_name: str

    # IAM authentication (if True, uses IAM tokens instead of password)
    db_use_iam_auth: bool = False

    # Connection pool parameters
    statement_timeout: str = "10000"

    # SSL mode for database connections
    # Valid values: disable, allow, prefer, require, verify-ca, verify-full
    # 'prefer' tries SSL but falls back to non-SSL for local dev (validates
    # certs if SSL is used). For production RDS without cert validation,
    # set db_sslmode=require via environment variable.
    db_sslmode: str = "require"

    @model_validator(mode="after")  # pyright: ignore[reportCallIssue]
    def validate_auth_method(self):
        """Validate that authentication credentials are provided.

        :raises ValueError: If password auth is selected but no password
            secret ARN is provided
        :return: Settings instance
        :rtype: Settings
        """
        if not self.db_use_iam_auth and self.managed_db_password_secret_arn is None:
            raise ValueError(
                "🔒 managed_db_password_secret_arn is required when "
                "db_use_iam_auth=False"
            )
        return self


# Pydantic settings are set from the env variables passed in via
# docker / apprunner. Missing required fields will raise a
# ValidationError on import.
# pyright: reportCallIssue=false
# Pyright doesn't recognize that BaseSettings loads from environment variables, so it
# flags required fields as missing constructor arguments. This is a false positive.
# pyright also has issues with model_validator type checking - the validator works
# correctly at runtime despite the type error.
settings = Settings()
