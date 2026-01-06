# trunk-ignore-all(checkov/CKV_DOCKER_3)
# Build Lambda package with dependencies
# Uses AWS Lambda Python 3.12 base image to match runtime environment

FROM public.ecr.aws/lambda/python:3.12

WORKDIR /build

# Install system dependencies required for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt -t . --quiet

# Copy application code
COPY . .

HEALTHCHECK NONE
