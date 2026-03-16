===============
Docker Connector
===============

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

With Volume Mounts
------------------

.. code-block:: yaml

   deployments:
     docker-volumes:
       type: docker
       config:
         image: ubuntu:22.04
         volumes:
           - /host/data:/container/data:ro
           - /host/output:/container/output:rw

With GPU Support
----------------

.. code-block:: yaml

   deployments:
     docker-gpu:
       type: docker
       config:
         image: nvidia/cuda:11.8.0-runtime-ubuntu22.04
         gpus: all

With Resource Limits
--------------------

.. code-block:: yaml

   deployments:
     docker-limited:
       type: docker
       config:
         image: python:3.10
         cpus: 4.0
         memory: 8g

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
   - :doc:`index` - All container connectors
   - :doc:`docker-compose` - Multi-container orchestration
   - :doc:`singularity` - For HPC container execution
   - :doc:`/reference/connectors/kubernetes` - For cloud-native container orchestration

**External Resources:**
   - :doc:`/reference/cwl-docker-translators/docker` - CWL Docker translator configuration
