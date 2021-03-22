FROM python:3.8-alpine AS builder
ARG HELM_VERSION

ENV PIPENV_VENV_IN_PROJECT=1

COPY ./streamflow ./Pipfile ./Pipfile.lock /streamflow/

RUN apk --no-cache add \
        bash \
        cargo \
        curl \
        g++ \
        libffi-dev \
        libressl-dev \
        libxml2-dev \
        libxslt-dev \
        make \
        musl-dev \
        openssl \
    && curl -L https://git.io/get_helm.sh -o /tmp/get_helm.sh \
    && chmod +x /tmp/get_helm.sh \
    && /tmp/get_helm.sh --version ${HELM_VERSION} \
    && pip install pipenv \
    && cd /streamflow \
    && pipenv install \
    && rm -f Pipfile*

FROM python:3.8-alpine
LABEL maintainer="iacopo.colonnelli@unito.it"

ENV PYTHONPATH="${PYTHONPATH}:/"

COPY --from=builder /streamflow/ /streamflow/
COPY --from=builder /usr/local/bin/helm /usr/local/bin/helm

RUN apk --no-cache add \
        libressl \
        libxml2 \
        libxslt \
        nodejs \
    && mkdir -p /streamflow/results

WORKDIR /streamflow/results

ENTRYPOINT ["/streamflow/.venv/bin/python", "-m", "streamflow"]
