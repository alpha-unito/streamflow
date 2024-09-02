=====================
SingularityConnector
=====================

The `Singularity <https://sylabs.io/singularity>`_ connector can spawn one or more instances of a Singularity container locally on the StreamFlow node. The units of deployment and binding for this connector correspond to the set of homogeneous container instances, while the unit of scheduling is the single instance. It extends the :ref:`ContainerConnector <ContainerConnector>`, which inherits from the :ref:`ConnectorWrapper <ConnectorWrapper>` interface, allowing users to spawn Singularity containers on top of local or remote execution environments using the :ref:`stacked locations <Stacked locations>` mechanism.

.. jsonschema:: ../../../streamflow/deployment/connector/schemas/singularity.json
    :lift_description: true