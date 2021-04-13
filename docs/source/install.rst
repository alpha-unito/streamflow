=======
Install
=======

You can install StreamFlow as a Python package with ``pip``, run it in a `Docker <https://www.docker.com/>`_ container or deploy it on `Kubernetes <https://kubernetes.io/>`_ with `Helm <https://helm.sh/>`_.

Pip
---

The StreamFlow module is available on `PyPI <https://pypi.org/project/streamflow/>`_, so you can install it using the following command::

    pip install streamflow

Please note that StreamFlow requires ``python >= 3.8`` to be installed on the system. Then you can execute your workflows through the StreamFlow CLI::

    streamflow /path/to/streamflow.yml

Docker
------

StreamFlow Docker images are available on `Docker Hub <https://hub.docker.com/r/alphaunito/streamflow>`_. To download the latest StreamFlow image, you can use the following command::

    docker pull alphaunito/streamflow:latest

The command below gives an example of how to execute a StreamFlow workflow in a Docker container:

.. code-block:: bash

    docker run -d \
    --mount type=bind,source="$(pwd)"/my-project,target=/streamflow/project \
    --mount type=bind,source="$(pwd)"/results,target=/streamflow/results \
    --mount type=bind,source="$(pwd)"/tmp,target=/tmp/streamflow \
    alphaunito/streamflow \
    /streamflow/project/streamflow.yml

.. note::
  A StreamFlow project, containing a ``streamflow.yml`` file and all the other relevant dependencies (e.g. a CWL description of the workflow steps and a Helm description of the execution environment) need to be mounted as a volume inside the container, for example in the ``/streamflow/project`` folder.

  By default, workflow outputs will be stored in the ``/streamflow/results`` folder. Therefore, it is necessary to mount such location as a volume in order to persist the results.

  StreamFlow will save all its temporary files inside the ``/tmp/streamflow`` location. For debugging purposes, or in order to improve I/O performances in case of huge files, it could be useful to mount also such location as a volume.

.. warning::
  All the container-based connectors (i.e., ``DockerConnector``, ``DockerComposeConnector`` and ``SingularityConnector``) are not supported from inside a Docker container, as running nested containers is a non-trivial task.

Kubernetes
----------

It is also possible to execute the StreamFlow container as a `Job <https://kubernetes.io/docs/concepts/workloads/controllers/job/>`_ in Kubernetes, with the same characteristics and restrictions discussed for the :ref:`Docker <Docker>` case. A Helm template of a StreamfFlow Job can be found :repo:`here <helm/chart>`.

In this case, the StreamFlow ``HelmConnector`` is able to deploy Helm models directly on the parent cluster, relying on `ServiceAccount <https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/>`_ credentials.

.. warning::
  In case `RBAC <https://kubernetes.io/docs/reference/access-authn-authz/rbac/>`_ is active on the Kubernetes cluster, a proper RoleBinding must be attached to the ServiceAccount object, in order to give StreamFlow the permissions to manage deployments of pods and executions of tasks.