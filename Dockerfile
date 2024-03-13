FROM python:3.9-slim

RUN mkdir /cpr-backend
WORKDIR /cpr-backend

RUN apt update && \
    apt install -y postgresql-client curl git \
    && rm -rf /var/lib/apt/lists/*

# Install pip and poetry
RUN pip install --no-cache --upgrade pip
RUN pip install --no-cache "poetry==1.3.2"

# Create layer for dependencies
# TODO: refine this as part of CI & test updates
COPY poetry.lock pyproject.toml ./

# Install the export plugin - not strictly required until later versions of poetry
RUN pip3 install --no-cache "poetry-plugin-export"

# Create a requirements file so we can install with minimal caching
# See: https://github.com/python-poetry/poetry-plugin-export
RUN poetry export --with dev \
    | grep -v '\--hash' \
    | grep -v '^torch' \
    | grep -v '^triton' \
    | grep -v '^nvidia' \
    | sed -e 's/ \\$//' \
    | sed -e 's/^[[:alpha:]]\+\[\([[:alpha:]]\+\[[[:alpha:]]\+\]\)\]/\1/' \
    > requirements.txt

# Check for zero length requirements file.
RUN if [ "$(wc -l <requirements.txt)" = "0" ] ; \
    then echo No requirements.txt: check the lock file is up-to-date ; exit 1; \
    fi

# Install torch-cpu with pip
RUN pip3 install --no-cache "torch==2.0.0+cpu" -f https://download.pytorch.org/whl/torch_stable.html

# Install application requirements
RUN pip3 install --no-cache -r requirements.txt

# Download the sentence transformer model
RUN mkdir /models
RUN mkdir /secrets
RUN python3 -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('msmarco-distilbert-dot-v5', cache_folder='/models')"

# Copy files to image
COPY app ./app
COPY scripts ./scripts
COPY LICENSE.md .
COPY README.md .
COPY startup.sh .

# ENV
ENV PYTHONPATH=/cpr-backend

CMD [ "/bin/bash", "/cpr-backend/startup.sh" ]
