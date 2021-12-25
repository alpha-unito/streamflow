===========================
Helm2Connector (Deprecated)
===========================

The `Helm v2 <https://v2.helm.sh/>`_ connector can spawn complex, multi-container environments on a `Kubernetes <https://kubernetes.io/>`_ cluster. The deployment unit is the entire Helm release, while the binding unit is the single container in a ``Pod``. StreamFlow requires each container in a Helm release to have a unique ``name`` attribute, allowing an unambiguous identification. Finally, the scheduling unit is the single instance of a potentially replicated container in a ``ReplicaSet``.

.. warning::
    Starting from November 2020, Helm v2 is discontinued even for security patches. Consequently, the ``Helm2Connector`` class is deprecated in favour of the :ref:`Helm3Connector <Helm3Connector>`. The ``Helm2Connector`` class will no longer be available in StreamFlow 0.2.0 and later.

.. jsonschema:: ../../../streamflow/config/schemas/v1.0/helm2.json
    :lift_description: true
