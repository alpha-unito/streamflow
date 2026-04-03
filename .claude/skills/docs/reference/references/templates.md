# Reference Documentation Templates

This reference contains complete templates for Reference documentation files.

## CLI Command Reference Template

```rst
==============
streamflow run
==============

.. meta::
   :keywords: StreamFlow, CLI, run, workflow, execution
   :description: Run StreamFlow workflows - complete command reference

Overview
========

Execute a StreamFlow workflow defined in a configuration file.

Synopsis
========

.. code-block:: text

   streamflow run [OPTIONS] FILE

Arguments
=========

``FILE``
   Path to StreamFlow configuration file (``streamflow.yml``)

Options
=======

``--debug``
   Enable debug logging output

``--quiet``
   Suppress non-error output

``--outdir DIR``
   Output directory for workflow results
   
   **Type:** string  
   **Default:** Current directory

[Continue with ALL options from --help]

Examples
========

Basic Execution
---------------

.. code-block:: bash

   $ streamflow run streamflow.yml

   [ACTUAL TESTED OUTPUT]

With Debug Logging
------------------

.. code-block:: bash

   $ streamflow run --debug streamflow.yml

   [ACTUAL TESTED OUTPUT]

Exit Status
===========

**0** - Success  
**1** - Error during execution  
**2** - Invalid arguments

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/running-workflows` - Workflow execution guide
   - :doc:`/user-guide/troubleshooting` - Troubleshooting execution

**Reference:**
   - :doc:`/reference/configuration/streamflow-yml` - Configuration file reference
   - :doc:`list` - List workflows
```

## Connector Reference Template

```rst
================
Docker Connector
================

.. meta::
   :keywords: StreamFlow, docker, container, deployment
   :description: Docker connector reference for StreamFlow

Overview
========

The Docker connector executes workflow tasks in Docker containers, providing isolation, reproducibility, and portability for local and single-node deployments.

Quick Reference
===============

============  ====================================
Type          ``docker``
Category      Container
Scalability   Single host
Best For      Local development, CI/CD pipelines
============  ====================================

Examples
========

Basic Docker Deployment
-----------------------

.. code-block:: yaml
   :caption: streamflow.yml - Minimal Docker

   deployments:
     docker-python:
       type: docker
       config:
         image: python:3.10

With Volume Mounts
------------------

.. code-block:: yaml
   :caption: streamflow.yml - With volumes

   deployments:
     docker-volumes:
       type: docker
       config:
         image: ubuntu:22.04
         volume:
           - /host/data:/container/data:ro
           - /host/output:/container/output:rw

With Resource Limits
--------------------

.. code-block:: yaml
   :caption: streamflow.yml - Resource limits

   deployments:
     docker-limited:
       type: docker
       config:
         image: python:3.10
         memory: 8g
         cpus: 4

Prerequisites
=============

* Docker installed and running
* User has Docker permissions (member of ``docker`` group on Linux)
* Required images available or pullable from registry

Platform Support
================

**Linux:** Full support  
**macOS:** Full support  
**Windows:** Not supported

.. note::
   StreamFlow only supports Linux and macOS. Windows is not supported.

Configuration
=============

.. jsonschema:: https://streamflow.di.unito.it/schemas/deployment/connector/docker.json
    :lift_description: true

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/configuring-deployments` - Deployment configuration guide
   - :doc:`/user-guide/binding-workflows` - Binding workflows to deployments
   - :doc:`/user-guide/troubleshooting` - Docker troubleshooting

**Connectors:**
   - :doc:`index` - All connectors
   - :doc:`singularity` - For HPC container deployments
   - :doc:`kubernetes` - For container orchestration

**External Resources:**
   - :doc:`/reference/cwl-docker-translators/docker` - CWL Docker translator configuration
```

## Configuration File Reference Template

```rst
===================
The StreamFlow File
===================

.. meta::
   :keywords: StreamFlow, configuration, YAML, streamflow.yml
   :description: Main StreamFlow configuration file reference

Overview
========

The ``streamflow.yml`` file is the main configuration file for StreamFlow. It defines workflows, deployments, and bindings in a single YAML document.

File Structure
==============

.. code-block:: yaml

   version: v1.0
   
   workflows:
     # Workflow definitions
   
   deployments:
     # Deployment configurations
   
   bindings:
     # Step-to-deployment bindings

Schema
======

.. jsonschema:: https://streamflow.di.unito.it/schemas/config/v1.0/config_schema.json
    :lift_description: true

Examples
========

Complete Configuration
----------------------

.. code-block:: yaml
   :caption: streamflow.yml - Complete example

   version: v1.0
   
   workflows:
     my-workflow:
       type: cwl
       config:
         file: workflow.cwl
   
   deployments:
     docker-python:
       type: docker
       config:
         image: python:3.10
   
   bindings:
     - step: /process
       target:
         deployment: docker-python

Validation
==========

Validate configuration:

.. code-block:: bash

   $ streamflow schema

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/quickstart` - First configuration
   - :doc:`/user-guide/configuring-deployments` - Deployment configuration

**Reference:**
   - :doc:`workflow-config` - Workflow configuration
   - :doc:`deployment-config` - Deployment configuration
   - :doc:`binding-config` - Binding configuration
```

## CWL Docker Translator Template

```rst
=================
Docker Translator
=================

.. meta::
   :keywords: StreamFlow, CWL, Docker, translator
   :description: Docker translator for CWL DockerRequirement

Overview
========

The **Docker Translator** is the default CWL Docker Translator in StreamFlow. It instantiates a Docker deployment for every CWL ``DockerRequirement`` specification.

Examples
========

Default Behavior
----------------

No ``streamflow.yml`` configuration needed - Docker is used automatically.

.. code-block:: yaml
   :caption: workflow.cwl - With DockerRequirement

   class: CommandLineTool
   requirements:
     DockerRequirement:
       dockerPull: python:3.10

Custom Docker Configuration
---------------------------

Override Docker settings:

.. code-block:: yaml
   :caption: streamflow.yml - Custom Docker config

   cwl:
     docker:
       type: docker
       config:
         memory: 8g
         cpus: 4

Configuration
=============

.. jsonschema:: https://streamflow.di.unito.it/schemas/cwl/requirement/docker/docker.json
    :lift_description: true

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/writing-workflows` - CWL workflow guide

**Connectors:**
   - :doc:`/reference/connectors/docker` - Docker connector reference

**CWL Translators:**
   - :doc:`index` - All CWL translators
   - :doc:`kubernetes` - Kubernetes translator
   - :doc:`singularity` - Singularity translator
```

## Glossary Entry Template

```rst
Term
----

**Definition:** Brief one-sentence definition

**Usage:** How the term is used in StreamFlow context

**Example:** Code example or usage example

**Related Terms:** :term:`Related Term 1`, :term:`Related Term 2`

**See Also:** :doc:`/reference/related-doc`
```
