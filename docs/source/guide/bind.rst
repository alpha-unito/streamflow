===================
Put it all together
===================

The entrypoint of each StreamFlow execution is a YAML file, conventionally called ``streamflow.yml``. The role of such file is to link each task in a workflow with the service that should execute it.

A valid StreamFlow file contains the ``version`` number (currently ``v1.0``) and two main sections: ``workflows`` and ``deployments``. The ``workflows`` section consists of a dictionary with uniquely named workflows to be executed in the current run, while the ``deployments`` section contains a dictionary of uniquely named deployment specifications.

Describing deployments
----------------------

Each deployment entry contains two main sections. The ``type`` field identifies which ``Connector`` implementation should be used for its creation, destruction and management. It should refer to one of the StreamFlow connectors described :ref:`here <Import your environment>`. The ``config`` field instead contains a dictionary of configuration parameters which are specific to each ``Connector`` class.

Describing workflows
--------------------

Each workflow entry contains three main sections. The ``type`` field identifies which language has been used to describe it (currently the only supported value is ``cwl``), the ``config`` field includes the paths to the files containing such description, and the ``bindings`` section is a list of step-deployment associations that specifies where the execution of a specific step should be offloaded.

In particular, CWL workflows ``config`` contain a mandatory ``file`` entry that points to the workflow description file (usually a ``*.cwl`` file similar to the example reported :ref:`here <Write your workflow>`) and an optional ``settings`` entry that points to a secondary file, containing the initial inputs of the workflow.

Binding steps and deployments
-----------------------------

Each entry in the ``bindings`` contains a ``step`` directive referring to a specific step in the workflow, and a ``target`` directive referring to a deployment entry in the ``deployments`` section of the StreamFlow file.

Each step can refer to either a single command or a nested sub-workflow. Steps are uniquely identified by means of a Posix-like path, where each simple task is mapped to a file and each sub-workflow is mapped to a folder. In partiuclar, the most external workflow description is always mapped to the root folder ``/``. Considering the example reported :ref:`here <Write your workflow>`, you should specify ``/compile`` in the ``step`` directive to identify the ``compile`` step, or ``/`` to identify the entire workflow.

The ``target`` directive binds the step with a specific service in a StreamFlow deployment. As discussed in the :doc:`architecture section <architecture>`, complex deployments can contain multiple services, which represent the unit of binding in StreamFlow. The best way to identify services in a deployment strictly depends on the deployment specification itself.

For example, in DockerCompose it is quite straightforward to uniquely identify each service by using its key in the ``services`` dictionary. Conversely, in Kubernetes we explicitly require users to label containers in a Pod with a unique identifier through the ``name`` attribute, in order to unambiguously identify them at deploy time.

Simpler deployments like single Docker or Singularity containers do not need a service layer, since the deployment contains a single service that is automatically uniquely identified.

Example
-------

The following snippet contains an example of a minimal ``streamflow.yml`` file, connecting the ``compile`` step of :ref:`this <Write your workflow>` workflow with an ``openjdk`` Docker container.

.. code-block:: yaml

   version: v1.0
   workflows:
     extract-and-compile:
       type: cwl
       config:
         file: main.cwl
         settings: config.yml
       bindings:
         - step: /compile
           target:
             deployment: docker-openjdk

   deployments:
     docker-openjdk:
       type: docker
       config:
         image: openjdk:9.0.1-11-slim