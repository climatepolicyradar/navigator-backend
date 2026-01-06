# trunk-ignore-all(checkov/CKV_DOCKER_3)
# Build Lambda package with dependencies
# Uses AWS Lambda Python 3.12 base image to match runtime environment

FROM public.ecr.aws/lambda/python:3.12

# Install build dependencies required for psycopg2
RUN dnf install -y \
    postgresql-devel \
    gcc \
    postgresql-libs \
 && dnf clean all


ENV LD_LIBRARY_PATH=/usr/lib64:$LD_LIBRARY_PATH

# Lambda task root
WORKDIR /build

# Copy requirements first (better layer caching)
COPY requirements.txt .

# Install dependencies into Lambda task root
RUN pip install -r requirements.txt -t . --quiet

# Copy application code
COPY . .

# (Optional but recommended) sanity check during build
RUN python - <<'EOF'
import psycopg2
print("psycopg2 OK:", psycopg2.__version__)
EOF

# Lambda entrypoint is provided by base image

HEALTHCHECK NONE
