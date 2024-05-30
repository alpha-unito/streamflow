==============================
NoContainerCWLDockerTranslator
==============================

The NoContainer :ref:`CWLDockerTranslator <CWLDockerTranslator>` ignores the given configuration for every CWL :ref:`DockerRequirement <CWL Docker Requirement>` specification in the selected subworkflow. The :ref:`LocalConnector <LocalConnector>` is used by default, unless the step is explicitly bound to a different deployment.

**WARNING:** Use this option with caution. The step execution may not work.  The user must manually ensure that the execution environment is properly configured with all the required software dependencies.