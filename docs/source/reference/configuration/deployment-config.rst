=======================
Deployment Configuration
=======================

.. meta::
   :keywords: StreamFlow, deployment, configuration, connectors
   :description: Deployment configuration reference for StreamFlow

Overview
========

Deployment configuration defines execution environments where workflow tasks run.

Configuration Structure
=======================

.. code-block:: yaml

   deployments:
     deployment-name:
       type: connector-type
       config:
         # Connector-specific configuration
       wraps: other-deployment  # Optional

Fields Reference
================

``type``
   Connector type identifier.
   
   **Type:** String  
   **Required:** Yes  
   **Values:** ``local``, ``docker``, ``kubernetes``, ``ssh``, ``slurm``, ``pbs``, ``singularity``, etc.

``config``
   Connector-specific configuration.
   
   **Type:** Object  
   **Required:** Depends on connector  
   **Format:** See connector documentation

``wraps``
   Name of another deployment to wrap (for stacked deployments).
   
   **Type:** String  
   **Required:** No

Connector Types
===============

**Container Connectors:**

* ``docker`` - Docker containers
* ``singularity`` - Singularity/Apptainer containers

**Cloud Connectors:**

* ``kubernetes`` - Kubernetes pods

**HPC Connectors:**

* ``ssh`` - Remote SSH hosts
* ``slurm`` - Slurm batch scheduler
* ``pbs`` - PBS/Torque scheduler

**Other:**

* ``local`` - Local execution

Examples
========

**Local Deployment:**

.. code-block:: yaml

   deployments:
     local-env:
       type: local

**Docker Deployment:**

.. code-block:: yaml

   deployments:
     docker-env:
       type: docker
       config:
         image: python:3.10
         volume:
           - /data:/data

**Kubernetes Deployment:**

.. code-block:: yaml

   deployments:
     k8s-env:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config
         namespace: streamflow

**Slurm Deployment:**

.. code-block:: yaml

   deployments:
     slurm-env:
       type: slurm
       config:
         hostname: hpc.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa
         maxConcurrentJobs: 10

**Stacked Deployment:**

.. code-block:: yaml

   deployments:
     base-slurm:
       type: slurm
       config:
         hostname: hpc.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa
     
     container-on-slurm:
       type: singularity
       config:
         image: docker://python:3.10
       wraps: base-slurm

Connector Configuration
=======================

For detailed connector-specific configuration, see:

* :doc:`/reference/connectors/docker` - Docker configuration
* :doc:`/reference/connectors/singularity` - Singularity configuration
* :doc:`/reference/connectors/kubernetes` - Kubernetes configuration
* :doc:`/reference/connectors/ssh` - SSH configuration
* :doc:`/reference/connectors/slurm` - Slurm configuration
* :doc:`/reference/connectors/pbs` - PBS configuration
* :doc:`/reference/connectors/index` - All connectors

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/configuring-deployments` - Deployment guide
   - :doc:`/user-guide/advanced-patterns/stacked-locations` - Stacked deployments

**Configuration:**
   - :doc:`streamflow-yml` - Main configuration file

See Also
========

* :doc:`/reference/connectors/index` - Complete connector documentation
