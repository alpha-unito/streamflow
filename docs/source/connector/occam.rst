=====================
OccamConnector
=====================

The `Occam <https://c3s.unito.it/index.php/super-computer>`_ SuperComputer is a High-Performance Computing (HPC) facility designed and managed in collaboration between the `University of Torino <https://www.unito.it/>`_ (UniTO) and the `National Institute for Nuclear Physics <https://home.infn.it/en/>`_ (INFN).

It is different from standard HPC facilities for two main reasons. First, users can reserve computing nodes for specific time slots instead of relying on a batched interaction orchestrated by a queue manager. Second, the execution model is entirely based on unprivileged `Docker <https://www.docker.com/>`_ containers.

This connector allows StreamFlow to offload computation to multi-container environments deployed on the Occam facility. The deployment unit is a multi-container environment deployed on one or more computing nodes. Multi-container environments are described in a YAML file with a syntax similar to the ``service`` section of `Docker Compose <https://docs.docker.com/compose/>`_. Users can pass this file to the connector through the ``file`` parameter. The unit of binding is the single top-level entry in the file, while the scheduling unit is the single container instance.

.. jsonschema:: ../../../streamflow/deployment/connector/schemas/occam.json
    :lift_description: true
