from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect
from sqlmodel import SQLModel


def test_migrations_up_and_down(postgres_container, tmp_path):
    """
    Test that Alembic migrations can run 'upgrade head' without error
    and create the expected tables.

    Uses the `tmp_path` fixture
    @see: https://docs.pytest.org/en/stable/how-to/tmp_path.html
    """

    db_url = postgres_container.get_connection_url()

    base_dir = Path(__file__).parent.parent.parent
    alembic_ini_path = base_dir / "app" / "run_migrations" / "alembic.ini"

    alembic_cfg = Config(str(alembic_ini_path))
    # TODO: https://linear.app/climate-policy-radar/issue/APP-1600/get-a-working-db-url-into-run-migrations
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)

    migrations_location = base_dir / "app" / "run_migrations" / "migrations"
    alembic_cfg.set_main_option("script_location", str(migrations_location))

    version_dir = tmp_path / "versions"
    version_dir.mkdir()
    alembic_cfg.set_main_option("version_locations", str(version_dir))

    # This proves that env.py has the models in context
    command.revision(alembic_cfg, message="test_init", autogenerate=True)

    command.upgrade(alembic_cfg, "head")

    # Verify tables exist
    engine = create_engine(db_url)
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())

    # Check for expected tables
    expected_tables = set(SQLModel.metadata.tables.keys()) | {"alembic_version"}
    assert tables == expected_tables

    # Run Downgrade
    command.downgrade(alembic_cfg, "base")

    # Verify tables are gone
    inspector = inspect(engine)
    tables_after_downgrade = set(inspector.get_table_names())

    tables_remaining = tables_after_downgrade - {"alembic_version"}
    assert (
        not tables_remaining
    ), f"Tables should be removed after downgrade, found: {tables_remaining}"
