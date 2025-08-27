from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from app import config

# Lazy initialisation - created once per worker
_engine = None
SessionLocal = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            config.SQLALCHEMY_DATABASE_URI,
            pool_pre_ping=True,
            poolclass=NullPool,  # Safe for multiprocess
        )
        # OpenTelemetry instrumentation
        SQLAlchemyInstrumentor().instrument(engine=_engine)
    return _engine


def get_db():
    """Get the database session.

    Tries to get a database session. If there is no session, it will
    create one AFTER the uvicorn stuff has started.
    """
    global SessionLocal
    if SessionLocal is None:
        SessionLocal = sessionmaker(
            # Get the engine using thread-safe initialisation.
            autocommit=False,
            autoflush=False,
            bind=get_engine(),
        )
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
