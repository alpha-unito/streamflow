=========================
DockerCWLDockerTranslator
=========================

The NoContainer :ref:`CWLDockerTranslator <CWLDockerTranslator>` ignores the given configuration for every CWL :ref:`DockerRequirement <CWL Docker Requirement>` specification in the selected subworkflow. The :ref:`LocalConnector <LocalConnector>` is used by default, unless there are bindings of the step to other deployment.

.. jsonschema:: ../../../../streamflow/cwl/requirement/docker/schemas/no-container.json
    :lift_description: true
