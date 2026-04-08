================
Docker Translator
================

.. meta::
   :keywords: StreamFlow, Docker, CWL, DockerRequirement, containers
   :description: Docker translator for CWL DockerRequirement in StreamFlow

Overview
========

The **Docker Translator** is the default CWL Docker Translator in StreamFlow. It instantiates a :doc:`/reference/connectors/docker` deployment for every CWL ``DockerRequirement`` specification.

**Use Cases:**

* Local workflow development
* CI/CD pipelines
* Systems with Docker Engine installed

Configuration
=============

.. jsonschema:: https://streamflow.di.unito.it/schemas/cwl/requirement/docker/docker.json
    :lift_description: true

Examples
========

Basic Usage (Automatic)
-----------------------

StreamFlow automatically uses Docker for ``DockerRequirement``:

.. code-block:: yaml
   :caption: CWL workflow with DockerRequirement

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

No ``streamflow.yml`` configuration needed - Docker is used automatically.

Explicit Configuration
----------------------

Override Docker settings in ``streamflow.yml``:

.. code-block:: yaml
   :caption: streamflow.yml - Custom Docker config

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
               type: docker
               config:
                 addHost:
                   - "database.local:192.168.1.10"
                 transferBufferSize: 65536

Custom Network Settings
-----------------------

Add custom DNS and hosts:

.. code-block:: yaml
   :caption: Custom networking

   workflows:
     network-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /process
             deployment:
               type: docker
               config:
                 addHost:
                   - "api.internal:10.0.0.5"
                   - "cache.internal:10.0.0.6"

Related Documentation
=====================

**Connectors:**
   - :doc:`/reference/connectors/docker` - Docker connector reference

**CWL Docker Translators:**
   - :doc:`index` - CWL Docker Translators overview
   - :doc:`kubernetes` - Kubernetes translator
   - :doc:`singularity` - Singularity translator

**User Guide:**
   - :doc:`/user-guide/writing-workflows` - Writing CWL workflows
