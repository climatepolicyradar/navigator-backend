FROM python:3.11-slim

RUN mkdir /cpr-backend
WORKDIR /cpr-backend

# we need libpq-dev gcc as we using the non-binary version of psycopg2
RUN apt update && \
    apt install -y postgresql-client curl git libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# TODO: Remove this once we've debugged 👇
RUN apt update && apt install -y gdb
ENV PYTHONFAULTHANDLER=1
ENV PYTHONMALLOC=debug
ENV MALLOC_CHECK_=3
ENV MALLOC_TRACE=/tmp/malloc.trace
# TODO: Remove this once we've debugged 👆

# Install pip and poetry
RUN pip install --no-cache --upgrade pip
RUN pip install --no-cache "poetry==1.7.1"

# Create layer for dependencies
# TODO: refine this as part of CI & test updates
COPY poetry.lock pyproject.toml ./

RUN poetry config virtualenvs.create false
RUN poetry install --no-root


# Download the sentence transformer model
RUN mkdir /models
RUN mkdir /secrets

# Copy files to image
COPY app ./app
COPY wait_for_port.sh ./scripts
COPY LICENSE.md .
COPY README.md .
COPY startup.sh .
COPY service-manifest.json .

# ENV
ENV PYTHONPATH=/cpr-backend

CMD [ "/bin/bash", "/cpr-backend/startup.sh" ]
