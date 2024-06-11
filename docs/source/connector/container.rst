==================
ContainerConnector
==================

The ``ContainerConnector`` is an abstract connector that serves as a base class to implement software container connectors (e.g., :ref:`Docker <DockerConnector>`, :ref:`Docker Compose <DockerComposeConnector>`, and :ref:`Singularity <SingularityConnector>`). It extends the abstract :ref:`ConnectorWrapper <ConnectorWrapper>` interface, allowing users to spawn software containers on top of local or remote execution environments using the :ref:`stacked locations <Stacked locations>` mechanism. Plus, it prevents :ref:`BatchConnector <BatchConnector>` instances to be wrapped as inner connectors.