from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlmodel import Session, create_engine

from app.settings import settings

navigator_engine = create_engine(
    settings.navigator_database_url.get_secret_value(),
    pool_pre_ping=True,  # Verify connections before use
    pool_size=10,  # Base connection pool size
    max_overflow=100,  # Additional connections when pool exhausted
    pool_recycle=1800,  # Recycle connections after 30 minutes
    pool_timeout=30,  # Wait up to 30s for a connection before error
)

SQLAlchemyInstrumentor().instrument(engine=navigator_engine)


def get_session():
    with Session(navigator_engine) as session:
        yield session
