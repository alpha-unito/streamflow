=================
Run your workflow
=================

To run a workflow with the StreamFlow CLI, simply use the following command:

.. code-block:: bash

    streamflow run /path/to/streamflow.yml

.. note::
   For CWL workflows, StreamFlow also supports the ``cwl-runner`` interface (more details :ref:`here <CWL Runner>`).

The ``--outdir`` option specifies where StreamFlow must store the workflow results and the execution metadata. Metadata are collected and managed by the StreamFlow ``Database`` implementtion (see :ref:`here <Database>`). By default, StreamFlow uses the current directory as its output folder.

The ``--name`` option allows to specify a workflow name for the current execution. Note that multiple execution can have the same name, meaning that they are multiple instances of the same workflow. If a ``--name`` is not explicitly provided, StreamFlow will randomly generate a unique name for the current execution.

The ``--color`` option allows to print log preamble with colors related to the logging level, which can be useful for live demos and faster log inspections.

Run on Docker
=============

The command below gives an example of how to execute a StreamFlow workflow in a Docker container:

.. code-block:: bash

    docker run -d \
    --mount type=bind,source="$(pwd)"/my-project,target=/streamflow/project \
    --mount type=bind,source="$(pwd)"/results,target=/streamflow/results \
    --mount type=bind,source="$(pwd)"/tmp,target=/tmp/streamflow \
    alphaunito/streamflow \
    streamflow run /streamflow/project/streamflow.yml

.. note::
  A StreamFlow project, containing a ``streamflow.yml`` file and all the other relevant dependencies (e.g. a CWL description of the workflow steps and a Helm description of the execution environment) needs to be mounted as a volume inside the container, for example in the ``/streamflow/project`` folder.

  By default, workflow outputs will be stored in the ``/streamflow/results`` folder. Therefore, it is necessary to mount such location as a volume in order to persist the results.

  StreamFlow will save all its temporary files inside the ``/tmp/streamflow`` location. For debugging purposes, or in order to improve I/O performances in case of huge files, it could be useful to mount also such location as a volume.

  By default, the StreamFlow :ref:`Database <Database>` stores workflow metadata in the ``${HOME}/.streamflow`` folder. Mounting this floder as a volume preserve these metadata for further inspection (see :ref:`here <Inspect workflow runs>`).

.. warning::
  All the container-based connectors (i.e., ``DockerConnector``, ``DockerComposeConnector`` and ``SingularityConnector``) are not supported from inside a Docker container, as running nested containers is a non-trivial task.

Run on Kubernetes
=================

It is also possible to execute the StreamFlow container as a `Job <https://kubernetes.io/docs/concepts/workloads/controllers/job/>`_ in Kubernetes, with the same characteristics and restrictions discussed for the :ref:`Docker <Docker>` case. A Helm template of a StreamFlow Job can be found :repo:`here <helm/chart>`.

In this case, the StreamFlow ``HelmConnector`` is able to deploy Helm charts directly on the parent cluster, relying on `ServiceAccount <https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/>`_ credentials. In order to do that, the ``inCluster`` option must be set to ``true`` for each involved module on the ``streamflow.yml`` file

.. code-block:: yaml

    deployments:
      helm-deployment:
        type: helm
        config:
          inCluster: true
          ...

A Helm template of a StreamFlow Job can be found `here <https://github.com/alpha-unito/streamflow/tree/master/helm/chart>`_.

.. warning::
  In case `RBAC <https://kubernetes.io/docs/reference/access-authn-authz/rbac/>`_ is active on the Kubernetes cluster, a proper RoleBinding must be attached to the ServiceAccount object, in order to give StreamFlow the permissions to manage deployments of pods and executions of tasks.