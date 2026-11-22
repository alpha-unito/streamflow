FROM python:3.15-slim AS python-amd64
FROM python:3.15-slim AS python-arm64
FROM riscv64/python:3.15-slim AS python-riscv64

ARG TARGETARCH

FROM python-${TARGETARCH} AS builder
ARG HELM_VERSION
ARG TARGETARCH
ARG TARGETOS

ENV VIRTUAL_ENV="/opt/streamflow"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

COPY ./pyproject.toml ./MANIFEST.in ./LICENSE ./README.md /build/
COPY ./streamflow /build/streamflow

WORKDIR /build

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
          build-essential \
          cargo \
          curl \
          libffi-dev \
          libssl-dev \
          pkg-config \
          rustc \
    && curl -fsSL \
          --retry 5 \
          --retry-max-time 60 \
          --connect-timeout 5 \
          --max-time 10 \
          https://get.helm.sh/helm-${HELM_VERSION}-${TARGETOS}-${TARGETARCH}.tar.gz \
          -o /tmp/helm.tar.gz \
    && tar -xzf /tmp/helm.tar.gz -C /tmp \
    && mv /tmp/${TARGETOS}-${TARGETARCH}/helm /usr/local/bin \
    && rm -rf /tmp/${TARGETOS}-${TARGETARCH} /tmp/helm.tar.gz \
    && python -m venv ${VIRTUAL_ENV} \
    && pip install --no-cache-dir .

FROM python-${TARGETARCH}
LABEL maintainer="iacopo.colonnelli@unito.it"

ENV VIRTUAL_ENV="/opt/streamflow"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY --from=builder /usr/local/bin/helm /usr/local/bin/helm

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /streamflow/results

WORKDIR /streamflow/results

CMD ["/bin/sh"]
