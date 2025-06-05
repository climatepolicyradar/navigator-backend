FROM python:3.12-slim

# Add build argument for GitHub SHA
# @related: GITHUB_SHA_ENV_VAR
ARG GITHUB_SHA
ENV GITHUB_SHA=${GITHUB_SHA}

WORKDIR /app

# Install system dependencies required for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# This is generated via the justfile with `just requirements`
COPY ./families-api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Copy shared API utilities
COPY ./api /app/api

# Copy the families-api application
COPY ./families-api /app/families-api

# Set the working directory to families-api
WORKDIR /app/families-api

CMD ["fastapi", "run", "./app/main.py", "--port", "8080", "--host", "0.0.0.0"]
