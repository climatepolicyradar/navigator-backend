import functools
from contextlib import nullcontext
from typing import Callable

from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan


def convert_to_loggable_string(obj: object) -> object:
    """Convert arbitrary objects into OpenTelemetry-compatible values.

    OpenTelemetry enforces strict conventions on what can be logged as
    additional structured data, allowing only one of ['bool', 'str',
    'bytes', 'int', 'float']. This function converts arbitrary objects
    to a string if possible.

    :param obj: Value that should be converted.
    :type obj: object
    :return: The converted value suitable for OTEL structured logging.
    :rtype: object
    """
    if isinstance(obj, (bool, int, float, str, bytes)):
        return obj
    if isinstance(obj, dict):
        return_str = ""
        for key, value in obj.items():
            return_str += f"{key}: {convert_to_loggable_string(value)} \n"
        return return_str
    if isinstance(obj, list):
        return_str = ""
        for item in obj:
            return_str += f"{convert_to_loggable_string(item)} \n"
        return return_str
    return str(obj)


def observe(name: str) -> Callable:
    """Wrap a callable inside an OpenTelemetry span.

    :param name: Span name to emit.
    :type name: str
    :return: Decorator that instruments the wrapped function.
    :rtype: Callable
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wraps(*args, **kwargs):
            if isinstance(trace.get_current_span(), NonRecordingSpan):
                span = nullcontext()
            else:
                span = trace.get_tracer(func.__module__).start_as_current_span(name)
            with span:
                return func(*args, **kwargs)

        return wraps

    return decorator
