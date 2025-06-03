FROM python:3.11-slim

WORKDIR /

# Install system dependencies required for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /requirements.txt

RUN pip install --no-cache-dir --upgrade -r /requirements.txt

COPY ./app /app

CMD ["fastapi", "run", "app/main.py", "--port", "8080"]
