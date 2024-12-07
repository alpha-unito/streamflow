==============================
SingularityCWLDockerTranslator
==============================

The Singularity :ref:`CWLDockerTranslator <CWLDockerTranslator>` instantiates a :ref:`SingularityConnector <SingularityConnector>` instance with the given configuration for every CWL :ref:`DockerRequirement <CWL Docker Requirement>` specification in the selected subworkflow. Note that the resulting ``SingularityConnector`` instance spawns ``ephemeral`` containers, making it able to wrap also :ref:`BatchConnector <BatchConnector>` instances for HPC deployments.

.. jsonschema:: ../../../../streamflow/cwl/requirement/docker/schemas/docker.json
    :lift_description: true
