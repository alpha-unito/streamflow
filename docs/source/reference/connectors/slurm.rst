==============
Slurm Connector
==============

.. meta::
   :keywords: StreamFlow, slurm, HPC, batch scheduler
   :description: Slurm connector reference for StreamFlow

Overview
========

The Slurm connector executes workflow tasks via the Slurm workload manager, the most widely used HPC batch scheduler, providing resource allocation, queue management, and fair-share scheduling.

Quick Reference
===============

============  ====================================
Type          ``slurm``
Category      HPC
Scalability   HPC clusters (1000s of nodes)
Best For      Large-scale HPC computations
============  ====================================

Examples
========

Basic CPU Job
-------------

.. code-block:: yaml

   deployments:
     slurm-cpu:
       type: slurm
       config:
         hostname: hpc.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa
         workdir: /scratch/user/jobs
       services:
         compute:
           partition: standard
           nodes: 2
           ntasks: 64
           mem: 128G
           time: "04:00:00"

With GPU Allocation
-------------------

.. code-block:: yaml

   deployments:
     slurm-gpu:
       type: slurm
       config:
         hostname: gpu-login.hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa
         workdir: /gpfs/scratch/user/jobs
       services:
         gpu-jobs:
           partition: gpu
           nodes: 1
           ntasks: 8
           gres: gpu:v100:2  # 2 V100 GPUs
           mem: 128G
           time: "08:00:00"

With Account and QoS
--------------------

.. code-block:: yaml

   deployments:
     slurm-priority:
       type: slurm
       config:
         hostname: hpc.example.com
         username: user
         sshKey: ~/.ssh/id_rsa
         workdir: /scratch/user
       services:
         high-priority:
           partition: priority
           account: research-grant-123
           qos: high
           nodes: 4
           ntasksPerNode: 32
           time: "24:00:00"

With Singularity Container
---------------------------

.. code-block:: yaml

   deployments:
     slurm-batch:
       type: slurm
       config:
         hostname: hpc.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa
         workdir: /scratch/user
       services:
         compute:
           partition: standard
           time: "04:00:00"
     
     singularity-on-slurm:
       type: singularity
       config:
         image: docker://python:3.10
       wraps: slurm-batch

Multiple Services
-----------------

.. code-block:: yaml

   deployments:
     slurm-mixed:
       type: slurm
       config:
         hostname: hpc.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa
         workdir: /scratch/user
       services:
         cpu-large:
           partition: compute
           nodes: 10
           ntasksPerNode: 64
           time: "12:00:00"
         
         gpu-small:
           partition: gpu
           nodes: 1
           gres: gpu:4
           time: "02:00:00"

Prerequisites
=============

* HPC system access
* SSH access to Slurm login nodes
* Job submission permissions
* Partition/queue access
* Resource quota available
* Working directory exists on HPC filesystem

Slurm Directives
================

StreamFlow service configuration maps directly to Slurm sbatch directives. For complete Slurm options, see the `Slurm documentation <https://slurm.schedmd.com/sbatch.html>`_.

Common Slurm Commands
=====================

**Check queue status:**

.. code-block:: bash

   squeue -u $USER

**Job details:**

.. code-block:: bash

   scontrol show job <job_id>

**Cancel job:**

.. code-block:: bash

   scancel <job_id>

Platform Support
================

**Linux:** Full support  
**macOS:** Full support (via SSH to Linux HPC)  
**Windows:** Not supported

Configuration
=============

.. jsonschema:: https://streamflow.di.unito.it/schemas/deployment/connector/slurm.json
    :lift_description: true

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/configuring-deployments` - Deployment configuration guide
   - :doc:`/user-guide/advanced-patterns/stacked-locations` - Container stacking
   - :doc:`/user-guide/troubleshooting` - HPC troubleshooting

**Connectors:**
   - :doc:`index` - All HPC connectors
   - :doc:`ssh` - For simple remote execution
   - :doc:`pbs` - For PBS-managed systems
   - :doc:`/reference/connectors/singularity` - Container integration

**External Resources:**
   - `Slurm Documentation <https://slurm.schedmd.com/sbatch.html>`_
