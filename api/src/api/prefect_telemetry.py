"""
Prefect integration for Navigator telemetry.

This module defines `PrefectTelemetry`, a lightweight subclass of
`BaseTelemetry` that reuses the shared OpenTelemetry bootstrap whilst
wiring up Prefect worker logging into the same export pipeline for
operational parity with service runtimes.
"""

import logging

from api.base_telemetry import BaseTelemetry


class PrefectTelemetry(BaseTelemetry):
    """Telemetry wiring specialised for Prefect workers."""

    def attach_to_prefect_logger(self, logger_name: str = "prefect") -> logging.Logger:
        """Attach OTLP logging to a Prefect logger.

        :param logger_name: Prefect logger name, defaults to ``"prefect"``.
        :type logger_name: str, optional
        :return: Logger configured for Prefect instrumentation.
        :rtype: logging.Logger
        """
        prefect_logger = logging.getLogger(logger_name)
        prefect_logger.setLevel(self.config.log_level)
        return prefect_logger
