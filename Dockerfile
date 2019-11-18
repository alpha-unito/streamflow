FROM python:3.7-slim AS builder

ENV PIPENV_VENV_IN_PROJECT=1

COPY ./streamflow ./Pipfile ./Pipfile.lock /streamflow/

RUN apt-get update -y -q \
    && apt-get install -y -q \
        build-essential \
        libxml2-dev \
        libxslt-dev \
    && pip install pipenv \
    && cd /streamflow \
    && pipenv install \
    && rm -f Pipfile*

FROM python:3.7-slim
LABEL maintainer="iacopo.colonnelli@unito.it"

ENV PYTHONPATH="${PYTHONPATH}:/"

COPY --from=builder /streamflow/ /streamflow/

RUN apt-get update -y -q \
    && apt-get install -y -q \
        libxml2 \
        libxslt1.1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


ENTRYPOINT ["/streamflow/.venv/bin/python", "-m", "streamflow"]
