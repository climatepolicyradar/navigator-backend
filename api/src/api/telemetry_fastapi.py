"""FastAPI-specific OpenTelemetry wiring."""

import asyncio
import logging
import sys
from types import TracebackType
from typing import Any, Awaitable, Callable, Optional, cast

from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from .telemetry_base import BaseTelemetry

AsyncExceptionHandler = Callable[[asyncio.AbstractEventLoop, dict[str, Any]], None]

__all__ = ["FastAPITelemetry", "ExceptionHandlingTelemetryRoute"]

LOGGER = logging.getLogger(__name__)


class FastAPITelemetry(BaseTelemetry):
    """Telemetry wiring specialised for FastAPI services.

    :raises RuntimeError: Propagated if base setup hook fails.
    """

    def setup_exception_hook(self) -> None:
        """Install FastAPI-specific exception hooks.

        :raises RuntimeError: Propagated if base hook setup fails.
        """
        super().setup_exception_hook()
        self.install_async_exception_hooks()

    def instrument_fastapi(self, app: "FastAPI") -> None:
        """Instrument a FastAPI application instance.

        :param app: FastAPI application to instrument.
        :type app: FastAPI
        :raises ValueError: If the application is already instrumented.
        """
        if hasattr(app.state, "telemetry"):
            raise ValueError("FastAPI application already instrumented.")
        FastAPIInstrumentor.instrument_app(
            app,
            tracer_provider=self.tracer_provider,
            excluded_urls="/health",
        )
        app.state.telemetry = self

    @staticmethod
    def wrap_route_handler(
        original_handler: Callable[[Request], Awaitable[Any]],
    ) -> Callable[[Request], Awaitable[Any]]:
        """Wrap a FastAPI route handler to enrich spans on exceptions.

        :param original_handler: Original FastAPI route handler.
        :type original_handler: Callable[[Request], Awaitable[Any]]
        :raises AssertionError: If telemetry is not attached to the app.
        :return: Awaitable route handler that enriches spans.
        :rtype: Callable[[Request], Awaitable[Any]]
        """

        async def custom_route_handler(request: Request):
            telemetry: Optional[FastAPITelemetry] = None
            try:
                telemetry = getattr(request.app.state, "telemetry", None)
                if telemetry is None:
                    LOGGER.error("🌀 Telemetry missing on FastAPI application state.")
                    raise RuntimeError(
                        "Telemetry missing on FastAPI application state."
                    )

                if not isinstance(telemetry, FastAPITelemetry):
                    LOGGER.error("FastAPITelemetry missing on application state.")
                    raise RuntimeError("FastAPITelemetry missing on application state.")

                tracer = telemetry.get_tracer()
                with tracer.start_as_current_span("route_handler"):
                    return await original_handler(request)
            except Exception as exc:  # noqa: BLE001
                if isinstance(telemetry, FastAPITelemetry):
                    telemetry.add_telemetry_for_exception(exc)
                raise

        return custom_route_handler

    def install_async_exception_hooks(
        self,
        custom_async_excepthook: Optional[AsyncExceptionHandler] = None,
    ) -> None:
        """Install exception hooks for asynchronous exceptions.

        :param custom_async_excepthook: Custom async exception hook to install.
        :type custom_async_excepthook: AsyncExceptionHandler, optional
        :return: None.
        :rtype: None
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            LOGGER.debug("🪄 No event loop; async exception hook installation skipped.")
            return None

        current_hook = loop.get_exception_handler()
        previous_hook: Optional[AsyncExceptionHandler]
        if current_hook is not None:
            if not callable(current_hook):
                raise TypeError("Existing async exception handler is invalid.")
            previous_hook = cast(AsyncExceptionHandler, current_hook)
        else:
            previous_hook = None

        async_hook = custom_async_excepthook or self._make_async_exception_hook(
            previous_hook
        )
        loop.set_exception_handler(async_hook)

    def _make_async_exception_hook(
        self,
        previous_hook: Optional[AsyncExceptionHandler],
    ) -> AsyncExceptionHandler:
        """Create an async exception handler that enriches spans.

        :param previous_hook: Previous exception handler to chain.
        :type previous_hook: AsyncExceptionHandler, optional
        :return: Async exception handler that enriches spans on errors.
        :rtype: AsyncExceptionHandler
        """

        def catch_async_exception(
            loop: asyncio.AbstractEventLoop, context: dict[str, Any]
        ) -> None:
            exception = context.get("exception", None)

            if exception:
                exc_info: tuple[
                    type[BaseException], BaseException, Optional[TracebackType]
                ] = (
                    type(exception),
                    exception,
                    exception.__traceback__,
                )
                if issubclass(exception.__class__, KeyboardInterrupt):
                    sys.__excepthook__(*exc_info)
                    return

                self._enrich_with_exception(*exc_info)

            if previous_hook:
                previous_hook(loop, context)

        return catch_async_exception


class ExceptionHandlingTelemetryRoute(APIRoute):
    """FastAPI route that captures telemetry on exceptions."""

    def get_route_handler(self) -> Callable:
        """Return a route handler wrapped with telemetry enrichment.

        :return: Wrapped route handler callable.
        :rtype: Callable
        """
        original_route_handler = super().get_route_handler()
        return FastAPITelemetry.wrap_route_handler(original_route_handler)
