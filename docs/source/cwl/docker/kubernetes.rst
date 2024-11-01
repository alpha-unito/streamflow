=============================
KubernetesCWLDockerTranslator
=============================

The Kubernetes :ref:`CWLDockerTranslator <CWLDockerTranslator>` instantiates a :ref:`KubernetesConnector <KubernetesConnector>` instance with the given configuration for every CWL :ref:`DockerRequirement <CWL Docker Requirement>` specification in the selected subworkflow. Note that, unlike other ``CWLDockerTranslator`` classes, the ``KubernetesConnector`` does not support ``ephemeral`` containers.

.. jsonschema:: ../../../../streamflow/cwl/requirement/docker/schemas/kubernetes.json
    :lift_description: true
