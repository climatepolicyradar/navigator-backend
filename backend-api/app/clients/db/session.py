from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from app import config

# Lazy initialisation - created once per worker
_engine = None
_SessionLocal = None


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


def create_session():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=get_engine(),
        )
    return _SessionLocal


# Export callable for tests
SessionLocal = create_session


def get_db():
    """Get the database session.

    Tries to get a database session. If there is no session, it will
    create one AFTER the uvicorn stuff has started.
    """
    db = create_session()()
    try:
        yield db
    finally:
        db.close()
