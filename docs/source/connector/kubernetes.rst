===================
KubernetesConnector
===================

The `Kubernetes <https://kubernetes.io/>`_ connector can spawn complex, multi-container environments on a Kubernetes cluster. The deployment unit is a set of Kubernetes YAML files, which are deployed in the order they are written in the ``config`` section and undeployed in the reverse order. The binding unit is the single container in a ``Pod``. StreamFlow requires each container in a Kubernetes namespace to have a unique ``name`` attribute, allowing an unambiguous identification. Finally, the scheduling unit is the single instance of a potentially replicated container in a ``ReplicaSet``.

.. jsonschema:: ../../../streamflow/deployment/connector/schemas/kubernetes.json
    :lift_description: true