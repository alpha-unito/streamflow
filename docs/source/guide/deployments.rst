=======================
Import your environment
=======================

StreamFlow relies on external specifications and tools to describe and orchestrate a remote execution environment. For example, a Kubernetes-based deployment can be described in Helm, while a resource reservation request on an HPC facility can be specified with Slurm or PBS files.

This feature allows users to stick with the technologies they already know, or at least with production-grade tools that are solid, maintained and well-documented. Moreover, it adheres to the `infrastructure-as-code <https://en.wikipedia.org/wiki/Infrastructure_as_code>`_ principle, making execution environments easily portable and self-documented.

The lifecycle management of each StreamFlow deployment is demanded to a specific implementation of the ``Connector`` interface. Connectors provided by default in the StreamFlow codebase are reported :ref:`here <Connector>`, but users can add new connectors to the list by simply creating their implementation of the ``Connector`` interface.

The following snippet contains a simple example of Docker deployment named ``docker-openjdk``, which instantiates a container from the ``openjdk:9.0.1-11-slim`` image. At runtime, StreamFlow creates a :ref:`DockerConnector <DockerConnector>` instance to manage the container lifecycle.

.. code-block:: yaml

    docker-openjdk:
      type: docker
      config:
        image: openjdk:9.0.1-11-slim
