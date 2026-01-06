# trunk-ignore-all(checkov/CKV_DOCKER_3)
# Build Lambda package with dependencies
# Uses AWS Lambda Python 3.12 base image to match runtime environment

FROM public.ecr.aws/lambda/python:3.12

# Lambda task root
WORKDIR /build

# Copy requirements first (better layer caching)
COPY requirements.txt .

# Install dependencies into Lambda task root
# psycopg2-binary includes all its dependencies
RUN pip install -r requirements.txt -t . --quiet

# Copy application code
COPY . .

# (Optional) sanity check - this should work without any system dependencies
RUN python - <<'EOF'
import psycopg2
print("psycopg2-binary OK:", psycopg2.__version__)
EOF

# Lambda entrypoint is provided by base image

HEALTHCHECK NONE
