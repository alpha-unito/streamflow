============
PBS Connector
============

.. meta::
   :keywords: StreamFlow, pbs, torque, HPC, batch scheduler
   :description: PBS/Torque connector reference for StreamFlow

Overview
========

The PBS connector executes workflow tasks via PBS Pro or OpenPBS batch schedulers, commonly found in traditional HPC environments.

Quick Reference
===============

============  ====================================
Type          ``pbs``
Category      HPC
Scalability   HPC clusters
Best For      PBS-managed HPC systems
============  ====================================

Examples
========

Basic CPU Job
-------------

.. code-block:: yaml

   deployments:
     pbs-cpu:
       type: pbs
       config:
         hostname: hpc.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa
         workdir: /home/user/jobs
       services:
         compute:
           queue: batch
           nodes: 2
           cpus: 32
           mem: 128gb
           walltime: "04:00:00"

With Resource Selection
-----------------------

.. code-block:: yaml

   deployments:
     pbs-select:
       type: pbs
       config:
         hostname: hpc.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa
         workdir: /scratch/user
       services:
         custom:
           queue: longrun
           select: "2:ncpus=16:mem=64gb:ngpus=1"
           walltime: "48:00:00"

With GPU Allocation
-------------------

.. code-block:: yaml

   deployments:
     pbs-gpu:
       type: pbs
       config:
         hostname: gpu-login.hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa
         workdir: /gpfs/scratch/user
       services:
         gpu-jobs:
           queue: gpu-queue
           nodes: 1
           cpus: 16
           ngpus: 2
           mem: 128gb
           walltime: "08:00:00"

Prerequisites
=============

* HPC system access
* SSH access to PBS login nodes
* Job submission permissions
* Queue access
* Resource quota available
* Working directory exists on HPC filesystem

PBS Directives
==============

StreamFlow service configuration maps to PBS qsub directives. For complete PBS options, see PBS documentation.

Common PBS Commands
===================

**Check queue status:**

.. code-block:: bash

   qstat -u $USER

**Job details:**

.. code-block:: bash

   qstat -f <job_id>

**Cancel job:**

.. code-block:: bash

   qdel <job_id>

Platform Support
================

**Linux:** Full support  
**macOS:** Full support (via SSH to Linux HPC)  
**Windows:** Not supported

Configuration
=============

.. jsonschema:: https://streamflow.di.unito.it/schemas/deployment/connector/pbs.json
    :lift_description: true

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/configuring-deployments` - Deployment configuration guide
   - :doc:`/user-guide/troubleshooting` - HPC troubleshooting
   - :doc:`/user-guide/advanced-patterns/stacked-locations` - Container stacking

**Connectors:**
   - :doc:`index` - All HPC connectors
   - :doc:`slurm` - For Slurm-managed systems
   - :doc:`ssh` - For simple remote execution
   - :doc:`/reference/connectors/singularity` - Container integration

**External Resources:**
   - `PBS Professional Documentation <https://www.pbspro.org/documentation.html>`_
