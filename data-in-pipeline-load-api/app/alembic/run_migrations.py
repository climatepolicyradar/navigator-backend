import logging
import os

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.engine import Engine

_LOGGER = logging.getLogger(__name__)


def get_library_path() -> str:
    """Return the absolute path of the library already installed"""
    script_path = os.path.realpath(__file__)
    script_directory = os.path.dirname(script_path)
    return script_directory


def run_migrations(engine: Engine) -> None:
    """
    Apply alembic migrations.

    Call through subprocess as opposed to the alembic command function as the server
    startup never completed when using the alembic solution.

    """
    # Path of the library
    script_directory = get_library_path()

    # Path to alembic.ini
    alembic_ini_path = f"{script_directory}/alembic.ini"
    alembic_cfg = Config(alembic_ini_path)

    # Set the script location
    alembic_cfg.set_main_option("script_location", f"{script_directory}/migrations")

    # Run the migration
    with engine.begin() as connection:
        _LOGGER.info("Checking for schema changes...")
        alembic_cfg.attributes["connection"] = connection

        # ---- IDEMPOTENCY CHECK ----
        migration_ctx = MigrationContext.configure(connection)
        current_rev = migration_ctx.get_current_revision()

        script = ScriptDirectory.from_config(alembic_cfg)
        head_rev = script.get_current_head()

        _LOGGER.debug("Current revision=%s, Head revision=%s", current_rev, head_rev)

        if current_rev == head_rev:
            _LOGGER.info("No schema changes detected")
            return

        try:
            _LOGGER.info("Applying migrations...")
            command.upgrade(alembic_cfg, "head")
            _LOGGER.info("Migrations applied successfully")
        except Exception as e:
            _LOGGER.exception("Error creating migration: %s", e)
            raise e
