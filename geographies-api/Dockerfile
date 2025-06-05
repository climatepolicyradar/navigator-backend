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
COPY ./geographies-api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Add the shared API code
COPY ./api /app/api
ENV PYTHONPATH="/app/api/src:$PYTHONPATH"

# Copy the geographies-api application
COPY ./geographies-api /app/geographies-api

# Set the working directory to geographies-api
WORKDIR /app/geographies-api

CMD ["fastapi", "run", "./app/main.py", "--port", "8080", "--host", "0.0.0.0"]
