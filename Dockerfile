FROM python:3.11-alpine3.16 AS builder
ARG HELM_VERSION

ENV VIRTUAL_ENV="/opt/streamflow"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

COPY ./pyproject.toml ./MANIFEST.in ./LICENSE ./README.md /build/
COPY ./requirements.txt           \
     ./bandit-requirements.txt    \
     ./lint-requirements.txt      \
     ./report-requirements.txt    \
     ./test-requirements.txt      \
     /build/
COPY ./docs/requirements.txt /build/docs
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
    && curl -fsSL \
          --retry 5 \
          --retry-max-time 60 \
          --connect-timeout 5 \
          --max-time 10 \
          https://git.io/get_helm.sh -o /tmp/get_helm.sh \
    && chmod +x /tmp/get_helm.sh \
    && /tmp/get_helm.sh --version ${HELM_VERSION} \
    && cd /build \
    && python -m venv ${VIRTUAL_ENV} \
    && pip install .

FROM python:3.11-alpine3.16
LABEL maintainer="iacopo.colonnelli@unito.it"

ENV VIRTUAL_ENV="/opt/streamflow"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY --from=builder /usr/local/bin/helm /usr/local/bin/helm

RUN apk --no-cache add \
        libxml2 \
        libxslt \
        nodejs \
        openssl \
    && mkdir -p /streamflow/results

WORKDIR /streamflow/results

CMD ["/bin/sh"]
