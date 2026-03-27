====================
Singularity Connector
====================

.. meta::
   :keywords: StreamFlow, singularity, apptainer, container, HPC
   :description: Singularity/Apptainer connector reference for StreamFlow

Overview
========

The Singularity connector executes workflow tasks in Singularity (now Apptainer) containers, designed for HPC environments with rootless execution and enhanced security.

Quick Reference
===============

============  ====================================
Type          ``singularity``
Category      Container
Scalability   HPC environments
Best For      HPC clusters, rootless containers
============  ====================================

Examples
========

From Docker Hub
---------------

.. code-block:: yaml

   deployments:
     singularity-docker:
       type: singularity
       config:
         image: docker://tensorflow/tensorflow:latest

With Bind Mounts
----------------

.. code-block:: yaml

   deployments:
     singularity-binds:
       type: singularity
       config:
         image: docker://python:3.10
         bindPaths:
           - /data:/data
           - /scratch:/scratch

Stacked on Slurm
----------------

.. code-block:: yaml

   deployments:
     slurm-batch:
       type: slurm
       config:
         hostname: hpc.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa
     
     singularity-on-slurm:
       type: singularity
       config:
         image: docker://python:3.10
       wraps: slurm-batch

Prerequisites
=============

* Singularity/Apptainer installed on execution host
* Appropriate image access permissions
* Sufficient disk space for image cache

HPC Integration
===============

Singularity is designed for HPC systems and provides:

* **Rootless execution** - No privileged access required
* **MPI support** - Native HPC application support
* **GPU access** - Direct GPU passthrough
* **Shared filesystems** - Automatic bind mounting

Platform Support
================

**Linux:** Full support  
**macOS:** Limited (via VM)  
**Windows:** Not supported

Configuration
=============

.. jsonschema:: https://streamflow.di.unito.it/schemas/deployment/connector/singularity.json
    :lift_description: true

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/configuring-deployments` - Deployment configuration guide
   - :doc:`/user-guide/advanced-patterns/stacked-locations` - Stacking with batch schedulers
   - :doc:`/user-guide/troubleshooting` - Container troubleshooting

**Connectors:**
   - :doc:`index` - All container connectors
   - :doc:`docker` - For local Docker containers
   - :doc:`/reference/connectors/slurm` - Slurm integration
   - :doc:`/reference/connectors/index` - HPC connectors

**External Resources:**
   - :doc:`/reference/cwl-docker-translators/singularity` - CWL Singularity translator
