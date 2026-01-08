FROM python:3.12-slim

# Copy uv from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Build arguments
ARG GITHUB_SHA
ENV GITHUB_SHA=${GITHUB_SHA}

ARG ENV
ENV ENV=${ENV}

ENV SERVICE=data-in-pipeline-load-api

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

WORKDIR /app
RUN uv sync --project $SERVICE

# Expose port
EXPOSE 8080

# Run the FastAPI application
CMD ["sh", "-c", "uv run fastapi run ./$SERVICE/app/main.py --port 8080 --host 0.0.0.0"]
