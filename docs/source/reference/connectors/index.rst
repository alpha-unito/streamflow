==========
Connectors
==========

.. meta::
   :keywords: StreamFlow, connectors, docker, kubernetes, slurm, ssh, deployment
   :description: Complete reference for all StreamFlow execution environment connectors

Overview
========

Connectors are StreamFlow's interface to execution environments. This section provides comprehensive reference documentation for all built-in connectors.

Quick Reference
===============

============  ====================================
Purpose       Connector reference
Audience      Users configuring deployments
Total Count   12+ built-in connectors
============  ====================================

Available Connectors
====================

.. toctree::
   :maxdepth: 1

   docker
   singularity
   kubernetes
   ssh
   slurm
   pbs

Connector Comparison
====================

====================  =============  ==============  ==============  ================
Connector             Category       Scalability     Complexity      Best For
====================  =============  ==============  ==============  ================
**local**             Native         Single node     Very Simple     Development
**docker**            Container      Single node     Simple          Local testing
**docker-compose**    Container      Multi-service   Simple          Service apps
**singularity**       Container      HPC-friendly    Medium          HPC containers
**kubernetes**        Cloud          Multi-node      Medium          Cloud native
**helm3**             Cloud          Multi-node      Medium          K8s packages
**ssh**               Remote         Remote nodes    Simple          Remote exec
**slurm**             HPC            HPC clusters    Medium          HPC batch
**pbs**               HPC            HPC clusters    Medium          HPC batch
**flux**              HPC            HPC clusters    Medium          Modern HPC
**occam**             HPC            Specific HPC    Complex         Torino HPC
====================  =============  ==============  ==============  ================

Choosing a Connector
====================

**For Development:**
   Use ``local`` connector for simplicity

**For Testing:**
   Use ``docker`` for reproducibility

**For Production:**
   * **Cloud workloads:** Use ``kubernetes`` or ``helm3``
   * **HPC workloads:** Use ``slurm``, ``pbs``, or ``flux``
   * **Hybrid:** Combine connectors with bindings

**For Containers on HPC:**
   Use ``singularity`` connector or stack ``singularity`` on ``slurm``

**For Remote Execution:**
   Use ``ssh`` connector for general remote hosts

Common Configuration Patterns
==============================

**Local Deployment:**

.. code-block:: yaml

   deployments:
     local-env:
       type: local

**Docker with Volume:**

.. code-block:: yaml

   deployments:
     docker-env:
       type: docker
       config:
         image: alpine:latest
         volume:
           - /host/path:/container/path

**Kubernetes with Pod Spec:**

.. code-block:: yaml

   deployments:
     k8s-env:
       type: kubernetes
       config:
         files:
           - pod-spec.yaml

**Slurm with Queue:**

.. code-block:: yaml

   deployments:
     slurm-env:
       type: slurm
       config:
         file: ssh-config.yaml
         maxConcurrentJobs: 10
         slurmConfig:
           partition: gpu-queue

Connector Features Matrix
==========================

=================  ======  ============  ===========  =============
Feature            Local   Container     Cloud        HPC
=================  ======  ============  ===========  =============
Multi-node         No      Depends       Yes          Yes
Resource limits    No      Yes           Yes          Yes
Queue management   No      No            Yes          Yes
Container support  No      Native        Native       Via Singularity
GPU support        Yes     Yes           Yes          Yes
Network isolation  No      Yes           Yes          Depends
=================  ======  ============  ===========  =============

Related Documentation
=====================

**User Guide:**
   For deployment tutorials:
   
   - :doc:`/user-guide/configuring-deployments` - Deployment setup guide

**Configuration:**
   For schema reference:
   
   - :doc:`/reference/configuration/deployment-config` - Deployment schema

**Developer Guide:**
   For creating custom connectors:
   
   - :doc:`/developer-guide/extension-points/connector` - Connector plugin

Advanced Topics
===============

**Stacked Deployments:**
   Combine connectors for complex scenarios:
   
   - :doc:`/user-guide/advanced-patterns/stacked-locations`

**Custom Connectors:**
   Create your own connector plugin:
   
   - :doc:`/developer-guide/extension-points/creating-plugins`
