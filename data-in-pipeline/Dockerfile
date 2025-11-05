# We are skipping the health check because it is not needed for the Prefect
# agent since this is a CLI tool and not a service that needs to be monitored
# and also don't have a health endpoint to ping to provide a meaningful health
# check
# checkov:skip=CKV_DOCKER_2
FROM prefecthq/prefect:3.4-python3.13

# Create a non-root user
RUN useradd -m -u 1000 prefect_user

# This ensures that the dependencies are installed at system python level
# without having to activate a venv
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"

# Copy your source code
COPY ./data-in-pipeline /app/data-in-pipeline/
COPY ./api /app/api/
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install uv (fast Python dependency manager)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install dependencies using the lockfile
RUN uv sync --project data-in-pipeline

WORKDIR /app/data-in-pipeline

ARG ENV
ENV ENV=${ENV}

# Set up permissions for prefect_user
RUN mkdir -p /opt/prefect && \
    chown -R prefect_user:prefect_user /opt/prefect && \
    chmod -R 755 /opt/prefect && \
    chown -R prefect_user:prefect_user /home/prefect_user && \
    chown -R prefect_user:prefect_user /usr/local/lib/python3.13/site-packages

# OTEL config
ENV OTEL_SERVICE_NAME="data-in-pipeline" \
    OTEL_TRACES_EXPORTER="otlp" \
    OTEL_METRICS_EXPORTER="otlp" \
    OTEL_LOGS_EXPORTER="otlp" \
    OTEL_EXPORTER_OTLP_ENDPOINT="https://otel.prod.climatepolicyradar.org" \
    OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf" \
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="https://otel.prod.climatepolicyradar.org/v1/traces" \
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="https://otel.prod.climatepolicyradar.org/v1/metrics" \
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT="https://otel.prod.climatepolicyradar.org/v1/logs" \
    OTEL_RESOURCE_ATTRIBUTES="deployment.environment=production,service.namespace=data-fetching"

# If you do not want the extra orchestration spans, set to "False".
# Leave as True to keep Prefect’s built-in flow/task spans.
ENV PREFECT_CLOUD_ENABLE_ORCHESTRATION_TELEMETRY="True"

# Switch to non-root user
USER prefect_user
