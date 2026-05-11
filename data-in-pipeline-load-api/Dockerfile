FROM python:3.13-slim AS base

# Copy uv from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Build arguments
ARG GITHUB_SHA
ENV GITHUB_SHA=${GITHUB_SHA}

ARG ENV
ENV ENV=${ENV}

ENV SERVICE=data-in-pipeline-load-api
# This ensures that the dependencies are installed at system python level
# without having to activate a venv
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"

# Install system dependencies required for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace root pyproject.toml (required for uv workspace resolution)
COPY pyproject.toml uv.lock /app/

# Copy application code
COPY $SERVICE /app/$SERVICE/
COPY ./api /app/api/
COPY ./data-in-models /app/data-in-models/

WORKDIR /app
RUN uv sync --project $SERVICE

# Expose port
EXPOSE 8080

# For more on multi-stage builds see:
# https://docs.docker.com/build/building/multi-stage/#use-an-external-image-as-a-stage

FROM base AS dev

RUN apt-get update && apt-get install -y --no-install-recommends faketime \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(find /usr/lib -name 'libfaketime.so.1' | head -1)" /usr/local/lib/libfaketime.so.1
ENV LD_PRELOAD=/usr/local/lib/libfaketime.so.1
CMD ["sh", "-c", "uv run fastapi dev ./$SERVICE/app/main.py --port 8080 --host 0.0.0.0"]

FROM base AS prod

CMD ["sh", "-c", "uv run fastapi run ./$SERVICE/app/main.py --port 8080 --host 0.0.0.0"]
