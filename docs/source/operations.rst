==========
Operations
==========

As shown in the :doc:`architecture <architecture>` section, you need three different components to run a hybrid workflow with StreamFlow:

* A :ref:`workflow description <Write your workflow>`, i.e. a representation of your application as a graph.
* One or more :ref:`deployment descriptions <Import your environment>`, i.e. infrastructure-as-code representations of your execution environments.
* A :ref:`StreamFlow file <Put it all together>` to bind each step of your workflow with the most suitable execution environment.

StreamFlow will automatically take care of all the secondary aspects, like checkpointing, fault-tolerance, data movements, etc.

Write your workflow
-------------------

StreamFlow relies on the `Common Workflow Language <https://www.commonwl.org/>`_ (CWL) standard to describe workflows. In particular, it supports version ``v1.2`` of the standard, which introduces conditional execution of workflow steps.

The reader is referred to the `official CWL documentation <https://www.commonwl.org/v1.2/>`_ to learn how the workflow description language works, as StreamFlow does not introduce any modification to the original specification.

.. warning::
  The process of integration with the CWL standard is still ongoing and some feature may still not work as expected. In particular, at the moment we lack support for format checking, nested cross-product of multiple scatter elements and the ``pickValue`` directive.

The following snippet contain a simple example of CWL workflow, which extracts a Java source file from a tar archive and compiles it.

.. code-block:: yaml

   cwlVersion: v1.2
   class: Workflow
   inputs:
     tarball: File
     name_of_file_to_extract: string

   outputs:
     compiled_class:
       type: File
       outputSource: compile/classfile

   steps:
     untar:
       run:
         class: CommandLineTool
         baseCommand: [tar, --extract]
         inputs:
           tarfile:
             type: File
             inputBinding:
              prefix: --file
         outputs:
           example_out:
             type: File
             outputBinding:
               glob: hello.txt
       in:
         tarfile: tarball
         extractfile: name_of_file_to_extract
       out: [extracted_file]

     compile:
       run:
         class: CommandLineTool
         baseCommand: javac
         arguments: ["-d", $(runtime.outdir)]
         inputs:
           src:
             type: File
             inputBinding:
               position: 1
         outputs:
           classfile:
             type: File
             outputBinding:
               glob: "*.class"
       in:
         src: untar/extracted_file
       out: [classfile]

Import your environment
-----------------------

StreamFlow relies on external specification and tools to describe and orchestrate remote execution environment. As an example, a Kubernetes-based deployment can be described in Helm, while a resource reservation request on a HPC facility can be specified with either a Slurm or PBS files.

This feature allows users to stick with the technologies they already know, or at least with production grade tools that are solid, maintained and well documented. Moreover, it adheres to the `infrastructure-as-code <https://en.wikipedia.org/wiki/Infrastructure_as_code>`_ principle, making execution environments easily portable and self-documented.

The lifecycle management of each StreamFlow model is demanded to a specific implementation of the ``Connector`` interface. Connectors provided by default in the StreamFlow codebase are reported in the table below, but users can add new connectors to the list by simply creating their own implementation of the ``Connector`` interface.

==============  ================================================================
Name            Class
==============  ================================================================
docker          streamflow.deployment.connector.docker.DockerConnector
docker-compose  streamflow.deployment.connector.docker.DockerComposeConnector
helm            streamflow.deployment.connector.helm.Helm3Connector
helm2           streamflow.deployment.connector.helm.Helm2Connector
helm3           streamflow.deployment.connector.helm.Helm3Connector
occam           streamflow.deployment.connector.occam.OccamConnector
pbs             streamflow.deployment.connector.queue_manager.PBSConnector
singularity     streamflow.deployment.connector.singularity.SingularityConnector
slurm           streamflow.deployment.connector.queue_manager.SlurmConnector
ssh             streamflow.deployment.connector.ssh.SSHConnector
==============  ================================================================

Put it all together
-------------------

The entrypoint of each StreamFlow execution is a YAML file, conventionally called ``streamflow.yml``. The role of such file is to link each task in a workflow with the service that should execute it.

A valid StreamFlow file contains the ``version`` number (currently ``v1.0``) and two main sections: ``workflows`` and ``models``. The ``workflows`` section consists of a dictionary with uniquely named workflows to be executed in the current run, while the ``models`` section contains a dictionary of uniquely named model specifications.

Describing models
^^^^^^^^^^^^^^^^^^

Each model entry contains two main sections. The ``type`` field identifies which ``Connector`` implementation should be used for its creation, destruction and management. It should refer to one of the StreamFlow connectors described :ref:`above <Import your environment>`. The ``config`` field instead contains a dictionary of configuration parameters which are specific to each ``Connector`` class.

Describing workflows
^^^^^^^^^^^^^^^^^^^^

Each workflow entry contains three main sections. The ``type`` field identifies which language has been used to describe it (currently the only supported value is ``cwl``), the ``config`` field includes the paths to the files containing such description, and the ``bindings`` section is a list of step-model associations that specifies where the execution of a specific step should be offloaded.

In particular, CWL workflows ``config`` contain a mandatory ``file`` entry that points to the workflow description file (usually a ``*.cwl`` file similar to the example reported :ref:`above <Write your workflow>`) and an optional ``settings`` entry that points to a secondary file, containing the initial inputs of the workflow.

Binding steps and models
^^^^^^^^^^^^^^^^^^^^^^^^

Each entry in the ``bindings`` contains a ``step`` directive referring to a specific step in the workflow, and a ``target`` directive refering to a model entry in the ``models`` section of the StreamFlow file.

Each step can refer to either a single command or a nested sub-workflow. Steps are uniquely identified by means of a Posix-like path, where each simple task is mapped to a file and each sub-workflow is mapped to a folder. In partiuclar, the most external workflow description is always mapped to the root folder ``/``. Considering the example reported :ref:`above <Write your workflow>`, you should specify ``/compile`` in the ``step`` directive to identify the ``compile`` step, or ``/`` to identify the entire workflow.

The ``target`` directive binds the step with a specific service in a StreamFlow model. As discussed in the :doc:`architecture section <architecture>`, complex models can contain multple services, which represent the unit of binding in StreamFlow. The best way to identify services in a model strictly depends on the model specification itself. For example, in DockerCompose it is quite straightforward to uniquely identify each service by using its key in the ``services`` dictionary. Conversely, in Kubernetes we explicitly require users to label containers in a Pod with a unique identifier through the ``name`` attribute, in order to unambiguously identify them at deploy time.

Simpler models like single Docker or Singularity containers do not need a service layer, since the model contains a single service that is automatically uniquely identified.

Example
^^^^^^^

The following snippet contains an example of a minimal ``streamflow.yml`` file, connecting the ``compile`` step of the previous workflow with an ``openjdk`` Docker container.

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
          model: docker-openjdk

   models:
     docker-openjdk:
       type: docker
       config:
         image: openjdk:9.0.1-11-slim
