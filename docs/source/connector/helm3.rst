==============
Helm3Connector
==============

The `Helm v3 <https://helm.sh/>`_ connector can spawn complex, multi-container environments on a `Kubernetes <https://kubernetes.io/>`_ cluster. The deployment unit is the entire Helm release, while the binding unit is the single container in a ``Pod``. StreamFlow requires each container in a Helm release to have a unique ``name`` attribute, allowing an unambiguous identification. Finally, the scheduling unit is the single instance of a potentially replicated container in a ``ReplicaSet``.

.. jsonschema:: ../../../streamflow/deployment/connector/schemas/helm3.json
    :lift_description: true
