=======================
DockerComposeConnector
=======================

The `DockerCompose <https://docs.docker.com/compose/>`_ connector can spawn complex, multi-container environments described in a Docker Compose file locally on the StreamFlow node. The entire set of ``services`` in the Docker Compose file contitutes the unit of deployment, while a single service is the unit of binding. Finally, the single instance of a potentially replicated service is the unit of scheduling.

.. jsonschema:: ../../../streamflow/deployment/connector/schemas/docker-compose.json
    :lift_description: true
