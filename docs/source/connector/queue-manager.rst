=====================
QueueManagerConnector
=====================

The ``QueueManagerConnector`` is an abstract connector that serves as a base class to implement High Performance Computing connectors, based on queue managers (e.g., :ref:`Slurm <SlurmConnector>`, :ref:`PBS <PBSConnector>`, and :ref:`Flux <FluxConnector>`). It extends the :ref:`ConnectorWrapper <ConnectorWrapper>` interface, allowing users to offload jobs to local or remote queue managers. The HPC facility is supposed to be constantly active, reducing the deployment phase to deploy the inner connector (e.g., to create an :ref:`SSHConnection <SSHConnection>` pointing to an HPC login node).

.. warning::

   Note that in StreamFlow ``v0.1``, the ``QueueManagerConnector`` directly inherited from the :ref:`SSHConnector <SSHConnector>` at the implementation level. Consequently, all the properties needed to open an SSH connection to the HPC login node (e.g., ``hostname``, ``username``, and ``sshKey``) were defined directly in the ``QueueManagerConnector``. This path is still supported by StreamFlow ``v0.2``, but it is deprecated and will be removed in StreamFlow ``v0.3``.


.. jsonschema:: ../../../streamflow/deployment/connector/schemas/queue_manager.json