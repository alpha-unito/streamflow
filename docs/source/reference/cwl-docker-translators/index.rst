=====================
CWL Docker Translators
=====================

.. meta::
   :keywords: StreamFlow, CWL, Docker, DockerRequirement, translators, containers
   :description: CWL DockerRequirement translators for StreamFlow

Overview
========

StreamFlow uses **CWL Docker Translators** to convert CWL `DockerRequirement <https://www.commonwl.org/v1.2/CommandLineTool.html#DockerRequirement>`_ specifications into StreamFlow deployment bindings. This allows you to run CWL workflows with Docker requirements on different container runtimes without modifying the workflow.

**Default Behavior:**

By default, StreamFlow automatically maps steps with ``DockerRequirement`` to Docker deployments using the specified image.

**Supported Translators:**

StreamFlow provides translators for multiple container runtimes:

* :doc:`docker` - Docker containers (default)
* :doc:`kubernetes` - Kubernetes pods
* :doc:`singularity` - Singularity/Apptainer containers
* :doc:`no-container` - Skip containerization (use with caution)

Quick Reference
===============

================  ========================================  ======================================
Translator        Runtime                                   Use Case
================  ========================================  ======================================
``docker``        Docker Engine                             Local development, CI/CD
``kubernetes``    Kubernetes                                Cloud-native, scalable deployments
``singularity``   Singularity/Apptainer                     HPC clusters (rootless containers)
``none``          No container (local)                      Testing, pre-configured environments
================  ========================================  ======================================

How It Works
============

CWL Docker Translators convert CWL ``DockerRequirement`` specifications into StreamFlow bindings:

**CWL Workflow:**

.. code-block:: yaml
   :caption: workflow.cwl

   cwlVersion: v1.2
   class: CommandLineTool
   baseCommand: python
   requirements:
     DockerRequirement:
       dockerPull: python:3.10
   inputs:
     script:
       type: File
       inputBinding:
         position: 1
   outputs:
     result:
       type: stdout
   stdout: output.txt

**Default Translation (Docker):**

StreamFlow automatically creates this equivalent binding:

.. code-block:: yaml
   :caption: Automatic Docker binding

   deployments:
     auto-docker:
       type: docker
       config:
         image: python:3.10
   bindings:
     - step: /
       target:
         deployment: auto-docker

**Custom Translation (Singularity):**

Override the default translator in ``streamflow.yml``:

.. code-block:: yaml
   :caption: streamflow.yml - Use Singularity

   version: v1.0
   workflows:
     my-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /
             deployment:
               type: singularity
               config:
                 image: docker://python:3.10

This runs the workflow in Singularity instead of Docker.

Configuration Format
====================

Docker translators are configured in the workflow's ``docker`` section:

.. code-block:: yaml
   :caption: Basic structure

   workflows:
     workflow-name:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /step-name    # Step to translate
             deployment:
               type: translator-type
               config:
                 # Translator-specific configuration

**Fields:**

``step``
   CWL step name (with leading ``/``). Use ``/`` for entire workflow.
   
   **Type:** String  
   **Required:** Yes

``deployment.type``
   Translator type.
   
   **Type:** String  
   **Required:** Yes  
   **Values:** ``docker``, ``kubernetes``, ``singularity``, ``none``

``deployment.config``
   Translator-specific configuration.
   
   **Type:** Object  
   **Required:** Depends on translator

Common Use Cases
================

Run on HPC with Singularity
----------------------------

Convert Docker requirements to Singularity for HPC clusters:

.. code-block:: yaml
   :caption: HPC with Singularity

   version: v1.0
   workflows:
     hpc-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /
             deployment:
               type: singularity
               config:
                 image: docker://python:3.10

Run on Kubernetes
-----------------

Deploy to Kubernetes pods:

.. code-block:: yaml
   :caption: Kubernetes deployment

   version: v1.0
   workflows:
     k8s-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /
             deployment:
               type: kubernetes
               config:
                 namespace: default

Skip Containerization
---------------------

Run without containers (requires pre-configured environment):

.. code-block:: yaml
   :caption: No container (use with caution)

   version: v1.0
   workflows:
     local-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /
             deployment:
               type: none

.. warning::

   When using ``type: none``, you must manually ensure all required software is installed in the execution environment.

Per-Step Translation
--------------------

Apply different translators to different steps:

.. code-block:: yaml
   :caption: Mixed translators

   version: v1.0
   workflows:
     hybrid-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /preprocess
             deployment:
               type: docker
               config:
                 image: python:3.10
           - step: /analyze
             deployment:
               type: kubernetes
               config:
                 namespace: compute
           - step: /visualize
             deployment:
               type: singularity
               config:
                 image: docker://r-base:latest

Available Translators
=====================

.. toctree::
   :maxdepth: 1

   docker
   kubernetes
   singularity
   no-container

Implementation Details
======================

The ``CWLDockerTranslator`` interface is defined in ``streamflow.cwl.requirement.docker.translator`` and exposes a single method:

.. code-block:: python

   def get_target(
       self,
       image: str,
       output_directory: str | None,
       network_access: bool,
       target: Target,
   ) -> Target:
       ...

**Parameters:**

``image``
   Docker image name from CWL ``DockerRequirement``.

``output_directory``
   Value of CWL ``dockerOutputDirectory`` option.

``network_access``
   Value from CWL `NetworkAccess <https://www.commonwl.org/v1.2/CommandLineTool.html#NetworkAccess>`_ requirement.

``target``
   Original target object from step binding.

**Returns:**

``Target`` object with auto-generated deployment configuration.

Custom Translators
==================

You can implement custom translators via the plugin system. See :doc:`/developer-guide/extension-points/index` for details.

**Registration:**

.. code-block:: python

   from streamflow.core.context import StreamFlowContext
   
   def register_cwl_docker_translator(context: StreamFlowContext):
       context.register_translator('my-translator', MyCustomTranslator)

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/writing-workflows` - Writing CWL workflows
   - :doc:`/user-guide/binding-workflows` - Binding workflows to deployments

**Reference:**
   - :doc:`/reference/connectors/docker` - Docker connector
   - :doc:`/reference/connectors/singularity` - Singularity connector
   - :doc:`/reference/connectors/kubernetes` - Kubernetes connector

**Developer Guide:**
   - :doc:`/developer-guide/extension-points/index` - Creating custom translators

**External Resources:**
   - `CWL DockerRequirement <https://www.commonwl.org/v1.2/CommandLineTool.html#DockerRequirement>`_
   - `CWL NetworkAccess <https://www.commonwl.org/v1.2/CommandLineTool.html#NetworkAccess>`_
