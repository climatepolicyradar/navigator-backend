import logging
import sys

from pydantic import SecretStr, ValidationError
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
try:
    settings = Settings()
    _LOGGER.info("✅ Settings loaded successfully")
except ValidationError as e:
    _LOGGER.error("❌ Failed to load settings: %s", e)
    _LOGGER.error("Missing or invalid environment variables")
    raise
except Exception as e:
    _LOGGER.exception("❌ Unexpected error loading settings: %s", e)
    raise
