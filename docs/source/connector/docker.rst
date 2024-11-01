===============
DockerConnector
===============

The `Docker <https://www.docker.com/>`_ connector can spawn one or more instances of a Docker container locally on the StreamFlow node. It extends the :ref:`ContainerConnector <ContainerConnector>`, which inherits from the :ref:`ConnectorWrapper <ConnectorWrapper>` interface, allowing users to spawn Docker containers on top of local or remote execution environments using the :ref:`stacked locations <Stacked locations>` mechanism. Normally, a single Docker container is reused for multiple workflow commands, reducing cold start overhead. However, when the ``ephemeral`` option is set to ``True``, a fresh container instance is spawned for each command to prevent internal state contamination. In addition, a ``ContainerConnector`` marked as ``ephemeral`` can successfully wrap :ref:`BatchConnector <BatchConnector>` instances.

.. jsonschema:: ../../../streamflow/deployment/connector/schemas/docker.json
    :lift_description: true