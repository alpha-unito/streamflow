===============
DockerConnector
===============

The `Docker <https://www.docker.com/>`_ connector can spawn one or more instances of a Docker container locally on the StreamFlow node. The units of deployment and binding for this connector correspond to the set of homogeneous container instances, while the unit of scheduling is the single instance.

.. jsonschema:: ../../../streamflow/deployment/connector/schemas/docker.json
    :lift_description: true