# trunk-ignore-all(checkov/CKV_DOCKER_3)
# Build Lambda package with dependencies
# Uses AWS Lambda Python 3.12 base image to match runtime environment

FROM public.ecr.aws/lambda/python:3.12

WORKDIR /build

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt -t . --quiet

# Copy application code
COPY . .

HEALTHCHECK NONE
