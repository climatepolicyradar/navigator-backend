# We are skipping the health check because it is not needed for the Prefect
# agent since this is a CLI tool and not a service that needs to be monitored
# and also don't have a health endpoint to ping to provide a meaningful health
# check
# checkov:skip=CKV_DOCKER_2
FROM prefecthq/prefect:3.4-python3.12

# Create a non-root user
RUN useradd -m -u 1000 prefect_user

# This ensures that the dependencies are installed at system python level
# without having to activate a venv
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"

# Copy your source code
COPY ./data-in-pipeline /app/data-in-pipeline/
COPY ./api /app/api/
COPY ./data-in-models /app/data-in-models/
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
    chown -R prefect_user:prefect_user /usr/local/lib/python3.12/site-packages

# Switch to non-root user
USER prefect_user

ENTRYPOINT ["opentelemetry-instrument"]
