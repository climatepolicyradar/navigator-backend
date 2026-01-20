import logging
import os

from alembic import command
from alembic.config import Config
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

    print(f"script_directory: {script_directory}")

    # Path to alembic.ini
    alembic_ini_path = f"{script_directory}/alembic.ini"
    alembic_cfg = Config(alembic_ini_path)

    print(f"alembic_ini_path: {alembic_ini_path}")

    # Set the script location
    alembic_cfg.set_main_option("script_location", f"{script_directory}/migrations")
    print(f"alembic_cfg: {alembic_cfg}")
    # Run the migration
    with engine.begin() as connection:
        alembic_cfg.attributes["connection"] = connection
        command.upgrade(alembic_cfg, "head")
