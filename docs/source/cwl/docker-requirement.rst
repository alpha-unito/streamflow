======================
CWL Docker Requirement
======================

The CWL standard supports a ``DockerRequirement`` feature to execute one or more workflow steps inside a `Docker container <https://www.docker.com/>`_. A CWL runner must then ensure that all input files are available inside the container and choose a specific Docker runner to deploy the container. For example, the following script invokes a `Node.js <https://nodejs.org/en>`_ command inside a Docker image called `node:slim <https://hub.docker.com/_/node/>`_:

.. code-block:: yaml

    cwlVersion: v1.2
    class: CommandLineTool
    baseCommand: node
    requirements:
      DockerRequirement:
        dockerPull: node:slim
    inputs:
      src:
        type: File
        inputBinding:
          position: 1
    outputs:
      example_out:
        type: stdout
    stdout: output.txt

By default, StreamFlow autmoatically maps a step with the ``DockerRequirement`` option onto a :ref:`Docker <DockerConnector>` deployment with the specified image. This mapping is pretty much equivalent to the following ``streamflow.yml`` file:

.. code-block:: yaml

    version: v1.0
    workflows:
      example:
        type: cwl
        config:
          file: processfile
          settings: jobfile
        bindings:
          - step: /
            target:
              deployment: docker-example

    deployments:
      docker-example:
        type: docker
        config:
          image: node:slim

StreamFlow also supports the possibility to map a CWL ``DockerRequirement`` onto different types of connectors through the :ref:`CWLDockerTranslator <CWLDockerTranslator>` extension point. In particular, the ``docker`` section of a workflow configuration can bind each step or subworkflow to a specific translator type, making it possible to convert a pure CWL workflow with ``DockerRequirement`` features into a hybrid workflow.

As an example, the following ``streamflow.yml`` file runs the above ``CommandLineTool`` using a :ref:`SingularityConnector <SingularityConnector>` instead of a :ref:`DockerConnector <DockerConnector>` to spawn the container:

.. code-block:: yaml

    version: v1.0
    workflows:
      example:
        type: cwl
        config:
          file: processfile
          settings: jobfile
          docker:
            step: /
            deployment:
              type: singularity
              config: {}

In detail, StreamFlow instantiates a :ref:`SingularityCWLDockerTranslator <SingularityCWLDockerTranslator>` passing the content of the ``config`` field directly to the constructor. The translator is then in charge of generating a :ref:`SingularityConnector <SingularityConnector>` instance with the specified configuration for each CWL ``DockerRequirement`` configuration in the target subworkflow.