FROM python:3.13-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# most of this is effectively taken from the uv docs
# @see: https://docs.astral.sh/uv/guides/integration/docker/
# there are a whole tonne of optimisations we can make from those docs
# but we're erring for simplicity for now

# Add build argument for GitHub SHA
# @related: GITHUB_SHA_ENV_VAR
ARG GITHUB_SHA
ENV GITHUB_SHA=${GITHUB_SHA}

# @related: ENV_ENV_VAR
ARG ENV
ENV ENV=${ENV}

ENV SERVICE=data-in-api
# This ensures that the dependencies are installed at system python level
# without having to activate a venv
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"

# Install system dependencies required for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install
COPY pyproject.toml uv.lock /app/

# Copy application code
COPY ./$SERVICE /app/$SERVICE/
COPY ./api /app/api/
COPY ./data-in-models /app/data-in-models/

WORKDIR /app
RUN uv sync --project $SERVICE

EXPOSE 8080

# For more on multi-stage builds see:
# https://docs.docker.com/build/building/multi-stage/#use-an-external-image-as-a-stage

FROM base AS dev
CMD ["sh", "-c", "uv run --project $SERVICE python $SERVICE/scripts/dev_entrypoint.py"]

FROM base AS prod
CMD ["sh", "-c", "uv run fastapi run ./$SERVICE/app/main.py --port 8080 --host 0.0.0.0"]
