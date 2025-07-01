FROM python:3.12-slim

WORKDIR /app

# @related: GITHUB_SHA_ENV_VAR
ARG GITHUB_SHA
ENV GITHUB_SHA=${GITHUB_SHA}

# @related: ENV_ENV_VAR
ARG ENV
ENV ENV=${ENV}

ARG SERVICE
ENV SERVICE=${SERVICE}

# Install system dependencies required for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# This is generated via the justfile with `just requirements`
COPY ./$SERVICE/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./$SERVICE /app/

CMD ["fastapi", "run", "./app/main.py", "--port", "8080", "--host", "0.0.0.0"]
