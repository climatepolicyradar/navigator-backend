from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine

from app.load.db.base import get_library_path


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
        alembic_cfg.attributes["connection"] = connection
        command.upgrade(alembic_cfg, "head")
