=====================
Singularity Translator
=====================

.. meta::
   :keywords: StreamFlow, Singularity, Apptainer, CWL, DockerRequirement, HPC
   :description: Singularity translator for CWL DockerRequirement in StreamFlow

Overview
========

The **Singularity Translator** converts CWL ``DockerRequirement`` specifications into :doc:`/reference/connectors/singularity` deployments. It enables running Docker-based CWL workflows on HPC systems using Singularity/Apptainer.

**Use Cases:**

* HPC cluster workflows
* Rootless container execution
* Systems without Docker
* Security-conscious environments

**Key Benefits:**

* No root/sudo required
* OCI/Docker image compatibility
* HPC-optimized performance
* Enhanced security model

Configuration
=============

.. jsonschema:: https://streamflow.di.unito.it/schemas/cwl/requirement/docker/singularity.json
    :lift_description: true

Image Formats
=============

Singularity supports multiple image sources:

**Docker Hub (Automatic):**

CWL ``dockerPull: python:3.10`` automatically converts to ``docker://python:3.10``

**Explicit Docker:**

.. code-block:: yaml

   config:
     image: docker://nvidia/cuda:11.8.0-base

**Singularity Library:**

.. code-block:: yaml

   config:
     image: library://lolcow

**Singularity Hub:**

.. code-block:: yaml

   config:
     image: shub://vsoch/hello-world

**Local SIF File:**

.. code-block:: yaml

   config:
     image: /path/to/image.sif

Examples
========

Basic Usage (Automatic)
-----------------------

StreamFlow automatically converts Docker images:

.. code-block:: yaml
   :caption: CWL with DockerRequirement

   cwlVersion: v1.2
   class: CommandLineTool
   baseCommand: python
   requirements:
     DockerRequirement:
       dockerPull: python:3.10

.. code-block:: yaml
   :caption: streamflow.yml - Use Singularity

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

The Docker image ``python:3.10`` is automatically converted to ``docker://python:3.10`` for Singularity.

HPC Cluster Deployment
----------------------

Run on HPC with Singularity:

.. code-block:: yaml
   :caption: HPC configuration

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

Pre-Built SIF Image
-------------------

Use pre-built Singularity image:

.. code-block:: yaml
   :caption: Local SIF file

   workflows:
     cached-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /
             deployment:
               type: singularity
               config:
                 image: /shared/containers/python-3.10.sif

GPU-Enabled Containers
----------------------

Run GPU workloads with Singularity:

.. code-block:: yaml
   :caption: GPU container

   workflows:
     gpu-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /compute
             deployment:
               type: singularity
               config:
                 image: docker://nvidia/cuda:11.8.0-runtime

Mixed Container Runtimes
------------------------

Use different runtimes for different steps:

.. code-block:: yaml
   :caption: Docker for local, Singularity for HPC

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
           - step: /compute
             deployment:
               type: singularity
               config:
                 image: docker://python:3.10

Best Practices
==============

1. **Pre-Build Images**
   
   Build Singularity images ahead of time for faster execution:
   
   .. code-block:: bash
   
      singularity build python-3.10.sif docker://python:3.10

2. **Use Shared Storage**
   
   Store SIF files on shared HPC storage for all nodes to access.

3. **Cache Docker Pulls**
   
   Set ``SINGULARITY_CACHEDIR`` to cache Docker image conversions:
   
   .. code-block:: bash
   
      export SINGULARITY_CACHEDIR=/shared/singularity-cache

4. **Test Locally First**
   
   Test with Docker locally, then switch to Singularity for HPC deployment.

Troubleshooting
===============

**Image Conversion Issues:**

If automatic Docker→Singularity conversion fails, pre-build the image:

.. code-block:: bash

   singularity build myimage.sif docker://python:3.10

Then use the local SIF file in configuration.

**Permission Errors:**

Singularity runs as your user. Ensure proper permissions on:

* Input/output directories
* Cache directories
* Image files

Related Documentation
=====================

**Connectors:**
   - :doc:`/reference/connectors/singularity` - Singularity connector reference

**CWL Docker Translators:**
   - :doc:`index` - CWL Docker Translators overview
   - :doc:`docker` - Docker translator
   - :doc:`kubernetes` - Kubernetes translator

**User Guide:**
   - :doc:`/user-guide/writing-workflows` - Writing CWL workflows

**External Resources:**
   - `Singularity Documentation <https://sylabs.io/docs/>`_
   - `Apptainer Documentation <https://apptainer.org/docs/>`_
