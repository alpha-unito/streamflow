ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS builder

ARG HELM_VERSION

RUN apk --no-cache add \
        bash \
        curl \
        openssl \
    && curl -L https://git.io/get_helm.sh -o /tmp/get_helm.sh \
    && chmod +x /tmp/get_helm.sh \
    && /tmp/get_helm.sh --version ${HELM_VERSION}

FROM ${BASE_IMAGE}
LABEL maintainer="iacopo.colonnelli@unito.it"

COPY --from=builder /usr/local/bin/helm /usr/local/bin/helm
COPY --from=builder /usr/local/bin/tiller /usr/local/bin/tiller
