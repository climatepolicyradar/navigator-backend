# Build Lambda package with dependencies
# Uses AWS Lambda Python 3.12 base image to match runtime environment
FROM public.ecr.aws/lambda/python:3.12

WORKDIR /build

# Copy requirements and install dependencies
COPY infra/lambda_code/requirements.txt .
RUN pip install -r requirements.txt -t . --quiet

# Copy application code
COPY infra/lambda_code/ .

HEALTHCHECK NONE
