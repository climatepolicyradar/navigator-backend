"""
Exception handling for open telemetry

Context:
- We want to catch all exceptions, handled or not, and enrich the current
span with the exception details. This will support minimizing diagnosis time for
errors.
- This can be a little tricky. You can use the global sys.excepthook pointer,
but this suffers two main problems:
    - If another library is using it, it may get overwritten. It's last come first served.
    This may mean that libraries with a lot of built in magic (like fastapi) may exhibit
unexpected behavior.
    - It doesn't support async contexts; we have to use a different mechanism.

Key details:
- The new excepthook function should return old_excepthook(args) -- so that we don't lose
the old functionality.
- sys.excepthook won't work with IPython. I think this is OK.
- We need a special case for KeyboardInterrupt exceptions, it's OK to not catch them.
- We want to make sure we don't change behaviour for handled exceptions, only raised.
- This is LAST RESORT handling only. We should be intentional in wrapping code
with the provided decorators or context managers to properly instrument observability.
The intention of catching unhandled exceptions is for unknown unknowns.
- To handle async exceptions, we can use the asyncio library.
"""

import asyncio
import sys
import traceback
from typing import Callable

from fastapi import Request
from fastapi.routing import APIRoute
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode


class ExceptionHandlingTelemetryRoute(APIRoute):
    """Used in FastAPI router to add telemetry to exceptions"""

    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request):
            try:
                tracer = request.app.state.telemetry.get_tracer()
                with tracer.start_as_current_span("route_handler"):
                    return await original_route_handler(request)
            except Exception as exc:
                add_telemetry_for_exception(exc)
                raise exc

        return custom_route_handler


def add_telemetry_for_exception(exc):
    """
    Augment current span for given excpetion object with telemetry attributes

    :param exc: The exception to handle
    """
    span = trace.get_current_span()
    span.record_exception(exc)


def _enrich_with_exception(e_type, e_value, e_traceback):
    """
    Enrich the current span with the exception details

    If no current span is found (tut. shouldn't happen in well instrumented code!),
    we create a new span.
    """
    span = trace.get_current_span()
    stacktrace = "".join(traceback.format_exception(e_type, e_value, e_traceback))
    if span:
        span.set_status(Status(StatusCode.ERROR))
        span.add_event(
            name="exception",
            attributes={
                "exception.type": e_type.__name__,
                "exception.message": str(e_value),
                "exception.stacktrace": stacktrace,
            },
        )
    else:
        with trace.start_as_current_span("exception_without_span") as new_span:  # type: ignore
            new_span.set_status(Status(StatusCode.ERROR))
            new_span.add_event(
                name="exception",
                attributes={
                    "exception.type": e_type.__name__,
                    "exception.message": str(e_value),
                    "exception.stacktrace": stacktrace,
                },
            )


def _make_exception_hook(old_excepthook):
    def catch_exception(e_type, e_value, e_traceback):
        """Catch exceptions and set the status to error"""

        # Handle KeyboardInterrupt exceptions
        if issubclass(e_type, KeyboardInterrupt):
            sys.__excepthook__(e_type, e_value, e_traceback)
            return

        # Enrich the current span with the exception details
        _enrich_with_exception(e_type, e_value, e_traceback)

        # Call the old excepthook if it exists
        if old_excepthook:
            return old_excepthook(e_type, e_value, e_traceback)
        else:
            return None

    return catch_exception


def _make_async_exception_hook(old_exception_handler):
    def catch_async_exception(loop, context):
        """Catch async exceptions"""
        print("I CAUGHT A ASYNC EXCEPTION")
        exception = context.get("exception", None)

        if exception:
            exc_info = (type(exception), exception, exception.__traceback__)
            if issubclass(exception.__class__, KeyboardInterrupt):
                sys.__excepthook__(*exc_info)
                return
            _enrich_with_exception(*exc_info)

        return old_exception_handler(loop, context)

    return catch_async_exception


def install_exception_hooks(custom_excepthook=None):
    """
    Install universal exception hooks for exceptions

    :param custom_excepthook: A custom excepthook to use. If not provided, the default
    excepthook will be used.
    """
    if custom_excepthook is None:
        custom_excepthook = _make_exception_hook(sys.excepthook)

    sys.excepthook = custom_excepthook


def install_async_exception_hooks(custom_async_excepthook=None):
    """
    Install universal exception hooks for async exceptions

    Note: This will only work if the event loop is running. If you're running
    this in a test, you may need to run the test in an async context.

    :param custom_async_excepthook: A custom async excepthook to use. If not provided, the default
    async excepthook will be used.
    """
    if custom_async_excepthook is None:
        current_hook = asyncio.get_running_loop().get_exception_handler()
        custom_async_excepthook = _make_async_exception_hook(current_hook)

    try:
        asyncio.get_running_loop().set_exception_handler(custom_async_excepthook)
    except RuntimeError:
        # This will happen if the event loop is not running
        print("No event loop running, skipping async exception hook")
        return
