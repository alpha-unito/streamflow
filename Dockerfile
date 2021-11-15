FROM python:3.8-alpine3.13 AS builder
ARG HELM_VERSION

ENV PYTHONPATH="${PYTHONPATH}:/build"
ENV PATH="/root/.local/bin:${PATH}"

COPY ./setup.py ./setup.cfg ./pytest.ini ./MANIFEST.in ./LICENSE ./README.md /build/
COPY ./streamflow /build/streamflow

RUN apk --no-cache add \
        bash \
        cargo \
        curl \
        g++ \
        libffi-dev \
        libxml2-dev \
        libxslt-dev \
        make \
        musl-dev \
        openssl \
        openssl-dev \
    && curl -L https://git.io/get_helm.sh -o /tmp/get_helm.sh \
    && chmod +x /tmp/get_helm.sh \
    && /tmp/get_helm.sh --version ${HELM_VERSION} \
    && cd /build \
    && pip install --user .

FROM python:3.8-alpine3.13
LABEL maintainer="iacopo.colonnelli@unito.it"

ENV PATH="/root/.local/bin:${PATH}"
ENV PYTHONPATH="/root/.local:${PYTHONPATH}"

COPY --from=builder "/root/.local/" "/root/.local/"
COPY --from=builder /usr/local/bin/helm /usr/local/bin/helm

RUN apk --no-cache add \
        libxml2 \
        libxslt \
        nodejs \
        openssl \
    && mkdir -p /streamflow/results

WORKDIR /streamflow/results

ENTRYPOINT ["streamflow"]
