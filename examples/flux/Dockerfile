FROM fluxrm/flux-sched:focal-v0.28.0

# ubuntu base with Flux
# this allows for easy development of Flux connector

LABEL maintainer="Vanessasaurus <@vsoch>"

USER root
RUN apt-get update \
 && python3 -m pip install IPython
    
COPY . /code
WORKDIR /code

# Install in development mode in case container used for development
RUN pip install develop . \
 && pip install --upgrade --force-reinstall \
        attrs \
        lockfile \
 && chown -R fluxuser /code

# Ensure we enter the container as the fluxuser
USER fluxuser
