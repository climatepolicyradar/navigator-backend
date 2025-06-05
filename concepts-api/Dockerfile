FROM python:3.12-slim

WORKDIR /app

# @related: GITHUB_SHA_ENV_VAR
ARG GITHUB_SHA
ENV GITHUB_SHA=${GITHUB_SHA}

# Install system dependencies required for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# This is generated via the justfile with `just requirements`
COPY ./concepts-api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Copy shared API utilities
COPY ./api /app/api

# Copy the concepts-api application
COPY ./concepts-api /app/concepts-api

# Set the working directory to concepts-api
WORKDIR /app/concepts-api

CMD ["fastapi", "run", "./app/main.py", "--port", "8080", "--host", "0.0.0.0"]
