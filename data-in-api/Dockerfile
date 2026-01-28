FROM python:3.13-slim

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

CMD ["uv", "run", "fastapi", "run", "./$SERVICE/app/main.py", "--port", "8080", "--host", "0.0.0.0"]
