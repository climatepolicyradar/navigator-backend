"""
SQLAlchemy integration for Navigator telemetry.

This module defines `SQLAlchemyTelemetry`, which provides comprehensive
telemetry for SQLAlchemy engines. It tracks both SQL queries (via
SQLAlchemyInstrumentor) and connection pool lifecycle events (connect,
checkout, checkin) to help diagnose connection leaks and monitor database
usage.
"""

import logging
from typing import Any

from opentelemetry import trace
from opentelemetry.instrumentation.sqlalchemy import (
    SQLAlchemyInstrumentor,
)
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import Pool

LOGGER = logging.getLogger(__name__)


class SQLAlchemyTelemetry:
    """Telemetry wiring for SQLAlchemy engines.

    Provides comprehensive database telemetry by tracking both SQL queries
    and connection pool lifecycle events. Use this to monitor database
    performance and diagnose connection leaks.
    """

    def __init__(self, tracer: Any, tracer_provider: Any | None = None) -> None:
        """Initialise SQLAlchemy telemetry.

        :param tracer: OpenTelemetry tracer instance.
        :type tracer: Any
        :param tracer_provider: Optional tracer provider for query
            instrumentation.
        :type tracer_provider: Any, optional
        """
        self.tracer = tracer
        self.tracer_provider = tracer_provider
        self._pool_instrumented = False
        self._query_instrumentor: Any | None = None

    def instrument_pool(self, engine: Any) -> None:
        """Instrument SQLAlchemy engine with connection pool event tracking.

        Sets up event listeners on the engine's connection pool to track
        connection lifecycle events (connect, checkout, checkin) as
        OpenTelemetry events. This helps diagnose connection leaks and
        monitor pool usage.

        :param engine: SQLAlchemy engine to instrument.
        :type engine: Any
        """
        if self.tracer is None:
            LOGGER.debug("🔌 SQLAlchemy pool instrumentation skipped (tracer disabled)")
            return

        if self._pool_instrumented:
            LOGGER.debug("🔌 SQLAlchemy pool already instrumented")
            return

        @event.listens_for(Pool, "connect")
        def _on_connect(dbapi_conn: Any, connection_record: Any) -> None:
            """Track new database connection establishment."""
            span = trace.get_current_span()
            if span and span.is_recording():
                span.add_event(
                    "db.pool.connect",
                    attributes={
                        "db.pool.event": "connect",
                        "db.pool.connection_id": id(dbapi_conn),
                    },
                )

        @event.listens_for(Pool, "checkout")
        def _on_checkout(
            dbapi_conn: Any, connection_record: Any, connection_proxy: Any
        ) -> None:
            """Track connection checkout from pool."""
            span = trace.get_current_span()
            if span and span.is_recording():
                span.add_event(
                    "db.pool.checkout",
                    attributes={
                        "db.pool.event": "checkout",
                        "db.pool.connection_id": id(dbapi_conn),
                    },
                )

        @event.listens_for(Pool, "checkin")
        def _on_checkin(dbapi_conn: Any, connection_record: Any) -> None:
            """Track connection return to pool."""
            span = trace.get_current_span()
            if span and span.is_recording():
                span.add_event(
                    "db.pool.checkin",
                    attributes={
                        "db.pool.event": "checkin",
                        "db.pool.connection_id": id(dbapi_conn),
                    },
                )

        @event.listens_for(Engine, "connect")
        def _on_engine_connect(conn: Any, branch: Any) -> None:
            """Track engine-level connection events."""
            span = trace.get_current_span()
            if span and span.is_recording():
                span.add_event(
                    "db.engine.connect",
                    attributes={
                        "db.pool.event": "engine_connect",
                        "db.pool.branch": str(branch),
                    },
                )

        self._pool_instrumented = True

    def instrument_queries(self, engine: Any) -> None:
        """Instrument SQLAlchemy engine with query tracking.

        Uses `SQLAlchemyInstrumentor` to track SQL queries and operations.
        This complements `instrument_pool()` which tracks connection pool
        events.

        :param engine: SQLAlchemy engine to instrument.
        :type engine: Any
        """
        if self._query_instrumentor is not None:
            LOGGER.debug("🔌 SQLAlchemy queries already instrumented")
            return

        self._query_instrumentor = SQLAlchemyInstrumentor()
        self._query_instrumentor.instrument(
            engine=engine, tracer_provider=self.tracer_provider
        )

    def instrument(self, engine: Any) -> None:
        """Instrument SQLAlchemy engine with both pool and query tracking.

        Convenience method that calls both `instrument_pool()` and
        `instrument_queries()` for comprehensive database telemetry.

        :param engine: SQLAlchemy engine to instrument.
        :type engine: Any
        """
        self.instrument_pool(engine)
        self.instrument_queries(engine)
