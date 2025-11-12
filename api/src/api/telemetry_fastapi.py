"""
FastAPI-specific OpenTelemetry wiring.
"""

import asyncio
import logging
import sys
import traceback
from typing import Callable, Optional, Tuple, Type

from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from .telemetry_base import BaseTelemetry

__all__ = ["FastAPITelemetry", "ExceptionHandlingTelemetryRoute"]

LOGGER = logging.getLogger(__name__)


class FastAPITelemetry(BaseTelemetry):
    """Telemetry wiring specialised for FastAPI services."""

    def setup_exception_hook(self) -> None:
        """Install FastAPI-specific exception hooks."""
        super().setup_exception_hook()
        self.install_async_exception_hooks()

    def instrument_fastapi(self, app: "FastAPI") -> None:
        """Instrument a FastAPI application instance.

        :param app: FastAPI application to instrument.
        :type app: FastAPI
        """
        FastAPIInstrumentor.instrument_app(
            app,
            tracer_provider=self.tracer_provider,
            excluded_urls="/health",
        )
        app.state.telemetry = self

    @staticmethod
    def wrap_route_handler(
        original_handler: Callable[[Request], Callable],
    ) -> Callable[[Request], Callable]:
        """Wrap a FastAPI route handler to enrich spans on exceptions."""

        async def custom_route_handler(request: Request):
            try:
                telemetry: FastAPITelemetry = request.app.state.telemetry  # type: ignore[attr-defined]
                tracer = telemetry.get_tracer()
                with tracer.start_as_current_span("route_handler"):
                    return await original_handler(request)
            except Exception as exc:  # noqa: BLE001
                telemetry.add_telemetry_for_exception(exc)
                raise

        return custom_route_handler

    def install_async_exception_hooks(
        self,
        custom_async_excepthook: Optional[
            Callable[[asyncio.AbstractEventLoop, dict], None]
        ] = None,
    ) -> None:
        """Install exception hooks for asynchronous exceptions."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            LOGGER.debug(
                "No event loop running; skipping async exception hook install."
            )
            return

        current_hook = loop.get_exception_handler()
        async_hook = custom_async_excepthook or self._make_async_exception_hook(
            current_hook
        )
        loop.set_exception_handler(async_hook)

    def _make_async_exception_hook(
        self,
        previous_hook: Optional[Callable[[asyncio.AbstractEventLoop, dict], None]],
    ) -> Callable[[asyncio.AbstractEventLoop, dict], None]:
        """Create an async exception handler that enriches spans."""

        def catch_async_exception(
            loop: asyncio.AbstractEventLoop, context: dict
        ) -> None:
            exception = context.get("exception", None)

            if exception:
                exc_info: Tuple[
                    Type[BaseException], BaseException, Optional[traceback]
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
        original_route_handler = super().get_route_handler()
        return FastAPITelemetry.wrap_route_handler(original_route_handler)
