import logging
import os
from logging.config import fileConfig
from typing import cast

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.db.base import Base

_LOGGER = logging.getLogger(__name__)

# Alembic Config object
# Provides access to the values within the .ini file in use.
config = context.config

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

# Allow overriding URL via env vars (use writer endpoint in eu-west-1)
db_host = os.getenv("AURORA_WRITER_ENDPOINT", "localhost")
db_name = os.getenv("DB_NAME", "postgres")
db_user = os.getenv("DB_ADMIN_USER", "postgres")  # an admin/superuser for migrations
db_password = os.getenv("DB_ADMIN_PASSWORD", "")
db_port = os.getenv("DB_PORT", "5432")

# Logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Metadata for the database models, means we can rely
# on SQLAlchemy's automatic table discovery and model
# history.
target_metadata = Base.metadata


def get_url() -> str:
    # Standard TLS; IAM tokens apply only to app user, not migrations admin
    # DATABASE_URL is the equivalent of:
    # f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require"
    db_url = cast(str, os.getenv("DATABASE_URL"))
    if db_url is None:
        raise ValueError("Environment variable DATABASE_URL not set")
    config.set_main_option("sqlalchemy.url", db_url)
    return db_url


def generate_incremental_revision_id(context, revision, directives) -> None:
    if getattr(context.config.cmd_opts, "autogenerate", False):
        script = directives[0]
        # current version
        cur_rev = max([int(rev) for rev in context.get_current_heads()], default=0)
        # force new version
        script.rev_id = f"{cur_rev + 1:04d}"
        if script.upgrade_ops.is_empty():
            directives[:] = []
            _LOGGER.info("No changes in schema detected.")
        elif not script.message:
            directives[:] = []
            _LOGGER.info("Message not provided - can not create revision.")
            _LOGGER.info("Run script with -m MESSAGE or --message MESSAGE")


def run_migrations_offline():
    """
    Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.
    Calls to context.execute() here emit the given string to the
    script output.
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """
    Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    connectable = config.attributes.get("connection", None)

    if connectable is None:
        configuration = config.get_section(config.config_ini_section)
        if configuration is None:
            raise RuntimeError("Alembic section of configuration is missing")
        configuration["sqlalchemy.url"] = get_url()
        connectable = engine_from_config(
            configuration,
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            process_revision_directives=generate_incremental_revision_id,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
