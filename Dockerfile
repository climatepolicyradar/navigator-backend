FROM python:3.11-slim

# Taken from https://docs.astral.sh/uv/guides/integration/docker/#installing-uv
COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /uvx /bin/

# git: for downloading the db-client in the pyproject.toml
# gcc: for compiling psycopg2
# libpq-dev: for building psycopg2
RUN apt-get update && \
    apt-get install -y git gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock ./
# We may want to change this to --no-dev and have a testing stage
# to avoid bloat, but this works for now as we use this container for testing
RUN uv sync --frozen --no-install-project --all-extras
# Explicitly install moto[s3] to ensure the s3 extra is available
RUN uv pip install "moto[s3]>=3.0.3"
COPY . .

# Download the sentence transformer model
RUN mkdir -p /models /secrets

# This is needed to allow startup.sh uvicorn
# as we're not using uv there
ENV PATH="/app/.venv/bin:$PATH"

CMD [ "/bin/bash", "/app/startup.sh" ]
