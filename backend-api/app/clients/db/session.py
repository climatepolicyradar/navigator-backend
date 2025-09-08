"""
Code for DB session management.

Notes: August 27th 2025.

We have been trying to trace segfault issues for months. They're 
our white whale. We identified a hypothesis: the sqlalchemy engine
and session were initialised on module import, before uvicorn 
spawned the worker processes. This meant that the engine and session
were shared across all workers. Ruh roh. SQLALCHEMY ISNT THREAD SAFE.

The following code is a bit hacky, but ensures the engine is lazily
initialised and therefore after uvicorn has spawned the worker 
processes.
"""

import logging
import os
import threading

from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import config

_LOGGER = logging.getLogger(__name__)

# Lazy initialisation - created once per worker
_engine = create_engine(config.SQLALCHEMY_DATABASE_URI)

# OpenTelemetry instrumentation
SQLAlchemyInstrumentor().instrument(engine=_engine)


def create_session():
    return sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=_engine,
    )


# Export callable for tests
SessionLocal = create_session


def get_db():
    """Get the database session.

    Tries to get a database session. If there is no session, it will
    create one AFTER the uvicorn stuff has started.
    """
    _LOGGER.info(
        f"Creating DB session | PID: {os.getpid()} | Main Thread: {threading.current_thread().name}"
    )
    _LOGGER.info(f"Thread count: {threading.active_count()}")
    db = create_session()()
    try:
        yield db
    finally:
        db.close()
