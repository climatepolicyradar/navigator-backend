import logging
import os

from alembic import command
from alembic.config import Config
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
        try:
            _LOGGER.info("Applying migrations...")
            command.upgrade(alembic_cfg, "head")
            _LOGGER.info("Migrations applied successfully")
        except Exception as e:
            _LOGGER.exception("Error creating migration: %s", e)
            raise e


def create_initial_migration(engine: Engine) -> None:
    """
    Create the initial Alembic migration using auto-generation.

    This function will fail if any Alembic revisions already exist in the
    migrations directory.
    """

    script_directory = get_library_path()

    alembic_ini_path = f"{script_directory}/alembic.ini"
    alembic_cfg = Config(alembic_ini_path)

    alembic_cfg.set_main_option("script_location", f"{script_directory}/migrations")

    script = ScriptDirectory.from_config(alembic_cfg)
    if script.get_heads():
        raise RuntimeError(
            "Initial migration already exists; refusing to create another"
        )

    try:
        with engine.begin() as connection:
            alembic_cfg.attributes["connection"] = connection
            command.revision(
                alembic_cfg,
                autogenerate=True,
                message="Initial migration",
            )
        _LOGGER.info("Initial migration created")

    except Exception as e:
        _LOGGER.exception("Failed to create initial migration : %s", e)
        raise e
