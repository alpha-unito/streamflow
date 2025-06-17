=======================
DockerComposeConnector
=======================

The `DockerCompose <https://docs.docker.com/compose/>`_ connector can spawn complex, multi-container environments described in a Docker Compose file locally on the StreamFlow node. The entire set of ``services`` in the Docker Compose file contitutes the unit of deployment, while a single service is the unit of binding. Finally, the single instance of a potentially replicated service is the unit of scheduling. It extends the :ref:`ContainerConnector <ContainerConnector>`, which inherits from the :ref:`ConnectorWrapper <ConnectorWrapper>` interface, allowing users to spawn Docker containers on top of local or remote execution environments using the :ref:`stacked locations <Stacked locations>` mechanism.

.. jsonschema:: https://streamflow.di.unito.it/schemas/deployment/connector/docker-compose.json
    :lift_description: true
