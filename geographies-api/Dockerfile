FROM python:3.12-slim
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

ENV SERVICE=geographies-api

# Install system dependencies required for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install
COPY uv.lock pyproject.toml /app/
COPY ./$SERVICE /app/$SERVICE/
COPY ./api /app/api/

WORKDIR /app
RUN uv sync --project $SERVICE

CMD ["uv", "run", "fastapi", "run", "./geographies-api/app/main.py", "--port", "8080", "--host", "0.0.0.0"]
