FROM  gitlab.c3s.unito.it:5000/alpha/streamflow:base AS builder

RUN apt-get update -y -q \
    && apt-get install -y -q \
        bash \
        curl \
        openssl \
    && curl -L https://git.io/get_helm.sh | bash

FROM gitlab.c3s.unito.it:5000/alpha/streamflow:base
LABEL maintainer="iacopo.colonnelli@unito.it"

COPY --from=builder /usr/local/bin/helm /usr/local/bin/helm
COPY --from=builder /usr/local/bin/tiller /usr/local/bin/tiller