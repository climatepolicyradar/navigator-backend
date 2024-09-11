FROM python:3.10-slim

RUN mkdir /cpr-backend
WORKDIR /cpr-backend

RUN apt update && \
    apt install -y postgresql-client curl git \
    && rm -rf /var/lib/apt/lists/*

# Install pip and poetry
RUN pip install --no-cache --upgrade pip
RUN pip install --no-cache "poetry==1.7.1"

# Create layer for dependencies
# TODO: refine this as part of CI & test updates
COPY poetry.lock pyproject.toml ./

RUN poetry config virtualenvs.create false
RUN poetry install --no-root

# TEMP
RUN pip install pyvespa==0.45.0 \
    && pip install pyyaml==6.0.2 \
    && pip install sentence-transformers==2.2.2 \
    && pip install 'torch>=2.0.0,<=2.2.2' \
    && pip install spacy==3.5.1

# Download the sentence transformer model
RUN mkdir /models
RUN mkdir /secrets
RUN python3 -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('msmarco-distilbert-dot-v5', cache_folder='/models')"

# Copy files to image
COPY app ./app
COPY wait_for_port.sh ./scripts
COPY LICENSE.md .
COPY README.md .
COPY startup.sh .

# ENV
ENV PYTHONPATH=/cpr-backend

CMD [ "/bin/bash", "/cpr-backend/startup.sh" ]
