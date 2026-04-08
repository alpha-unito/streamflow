==================
Binding Workflows
==================

.. meta::
   :keywords: StreamFlow, binding, deployment, workflow, target, filter
   :description: Learn how to bind workflow steps to execution environments in StreamFlow

Overview
========

Bindings connect workflow steps to execution environments. This guide explains how to configure bindings in the StreamFlow configuration file to control where each step executes.

Binding Concepts
================

========================  ========================================
Concept                   Description
========================  ========================================
**Step**                  A single computational task in a workflow
**Binding**               Association between step and deployment
**Target**                Deployment/service where step executes
**Filter**                Strategy for selecting among multiple targets
========================  ========================================

StreamFlow Configuration File
==============================

The ``streamflow.yml`` file is the entrypoint for StreamFlow execution. It connects workflows with deployments through bindings.

Basic Structure
---------------

.. code-block:: yaml
   :caption: streamflow.yml - Basic structure

   version: v1.0
   
   workflows:
     my-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
       bindings:
         - step: /step-name
           target:
             deployment: deployment-name
   
   deployments:
     deployment-name:
       type: connector-type
       config:
         # Connector configuration

**Required Fields:**

* ``version`` - StreamFlow configuration version (currently ``v1.0``)
* ``workflows`` - Dictionary of workflow definitions
* ``deployments`` - Dictionary of deployment definitions

Workflow Configuration
======================

Each workflow entry contains:

==============  ====================================
Field           Description
==============  ====================================
``type``        Workflow language (``cwl``)
``config``      Workflow-specific configuration
``bindings``    List of step-to-deployment mappings
==============  ====================================

CWL Workflow Config
-------------------

.. code-block:: yaml
   :caption: CWL workflow configuration

   workflows:
     my-cwl-workflow:
       type: cwl
       config:
         file: workflow.cwl         # Required: CWL workflow file
         settings: inputs.yml       # Optional: Input values file

Step Identification
===================

Steps are identified using POSIX-like paths:

==================  ========================================
Path                Meaning
==================  ========================================
``/``               Entire workflow (root)
``/step-name``      Top-level step
``/sub/nested``     Nested sub-workflow step
==================  ========================================

**Example Workflow:**

.. code-block:: yaml
   :caption: workflow.cwl - Multi-step workflow

   cwlVersion: v1.2
   class: Workflow
   steps:
     preprocess:
       run: preprocess.cwl
       in: { ... }
       out: [...]
     
     analyze:
       run: analyze.cwl
       in: { ... }
       out: [...]
     
     visualize:
       run: visualize.cwl
       in: { ... }
       out: [...]

**Step Paths:**

* ``/preprocess`` - The preprocess step
* ``/analyze`` - The analyze step
* ``/visualize`` - The visualize step
* ``/`` - The entire workflow

Basic Bindings
==============

Single Step Binding
-------------------

Bind a specific step to a deployment:

.. code-block:: yaml
   :caption: Single step binding

   workflows:
     example:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
       bindings:
         - step: /compile
           target:
             deployment: docker-java
   
   deployments:
     docker-java:
       type: docker
       config:
         image: openjdk:11

**Result:** The ``/compile`` step executes in the ``docker-java`` deployment.

Whole Workflow Binding
----------------------

Bind all steps to one deployment:

.. code-block:: yaml
   :caption: Bind entire workflow

   workflows:
     example:
       type: cwl
       config:
         file: workflow.cwl
       bindings:
         - step: /
           target:
             deployment: my-cluster
   
   deployments:
     my-cluster:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config

**Result:** All steps execute on the Kubernetes cluster.

Multiple Step Bindings
----------------------

Bind different steps to different deployments:

.. code-block:: yaml
   :caption: Multiple bindings

   workflows:
     pipeline:
       type: cwl
       config:
         file: pipeline.cwl
       bindings:
         - step: /preprocess
           target:
             deployment: fast-cloud
         
         - step: /heavy_compute
           target:
             deployment: hpc-cluster
         
         - step: /visualize
           target:
             deployment: local
   
   deployments:
     fast-cloud:
       type: kubernetes
       config: { ... }
     
     hpc-cluster:
       type: slurm
       config: { ... }
     
     local:
       type: local

**Result:** Hybrid execution across cloud, HPC, and local environments.

Service-Level Bindings
======================

Target specific services within deployments:

.. code-block:: yaml
   :caption: Service-level binding

   workflows:
     example:
       type: cwl
       config:
         file: workflow.cwl
       bindings:
         - step: /cpu_task
           target:
             deployment: k8s-cluster
             service: cpu-workers
         
         - step: /gpu_task
           target:
             deployment: k8s-cluster
             service: gpu-workers
   
   deployments:
     k8s-cluster:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config
       services:
         cpu-workers:
           replicas: 10
           template:
             spec:
               containers:
                 - name: worker
                   image: python:3.10
         
         gpu-workers:
           replicas: 2
           template:
             spec:
               containers:
                 - name: gpu-worker
                   image: tensorflow/tensorflow:latest-gpu
                   resources:
                     limits:
                       nvidia.com/gpu: 1

**Result:** CPU tasks run on CPU workers, GPU tasks run on GPU workers.

Multiple Targets
================

Bind a step to multiple targets for load balancing:

.. code-block:: yaml
   :caption: Multiple targets

   workflows:
     example:
       type: cwl
       config:
         file: workflow.cwl
       bindings:
         - step: /process
           target:
             - deployment: cluster-1
             - deployment: cluster-2
             - deployment: cluster-3
   
   deployments:
     cluster-1:
       type: slurm
       config: { ... }
     
     cluster-2:
       type: slurm
       config: { ... }
     
     cluster-3:
       type: slurm
       config: { ... }

**Result:** 

* Step instances can execute on any of the three clusters
* Useful for scatter operations that generate multiple tasks
* StreamFlow scheduler selects target based on availability

Binding Filters
===============

Filters control target selection among multiple options.

Shuffle Filter
--------------

Evaluate targets in random order:

.. code-block:: yaml
   :caption: Shuffle filter

   workflows:
     example:
       type: cwl
       config:
         file: workflow.cwl
       bindings:
         - step: /process
           target:
             - deployment: cluster-1
             - deployment: cluster-2
             - deployment: cluster-3
           filters:
             - shuffle

**Use Case:** Distribute load randomly across clusters.

Custom Filters
--------------

StreamFlow supports custom binding filters through plugins. See :doc:`/developer-guide/extension-points/binding-filter` for creating custom filters.

Port Bindings
=============

Bind input/output ports to specific locations for data staging.

Basic Port Binding
------------------

.. code-block:: yaml
   :caption: Port target configuration

   workflows:
     example:
       type: cwl
       config:
         file: workflow.cwl
       bindings:
         - port: /compile/src
           target:
             deployment: hpc-storage
             workdir: /scratch/user/data
   
   deployments:
     hpc-storage:
       type: ssh
       config:
         hostname: storage.hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa

**Result:** StreamFlow looks for the ``src`` input file on the remote HPC storage instead of locally.

Port Path Syntax
----------------

Ports use POSIX-like paths: ``/step-name/port-name``

==================  ========================================
Path                Meaning
==================  ========================================
``/tarball``        Workflow input port
``/compile/src``    Input port ``src`` of step ``compile``
``/compile/class``  Output port ``class`` of step ``compile``
==================  ========================================

Use Cases for Port Bindings
----------------------------

1. **Data Already on Remote System:**
   
   .. code-block:: yaml
   
      bindings:
        - port: /input_data
          target:
            deployment: hpc-cluster
            workdir: /data/project

2. **Avoid Large Data Transfers:**
   
   .. code-block:: yaml
   
      bindings:
        - port: /large_dataset
          target:
            deployment: storage-server
            workdir: /mnt/datasets

3. **Stage Outputs to Specific Location:**
   
   .. code-block:: yaml
   
      bindings:
        - port: /final_results
          target:
            deployment: archive-storage
            workdir: /archive/project-123

Advanced Binding Patterns
==========================

Stacked Locations
-----------------

Wrap deployments for complex execution environments:

.. code-block:: yaml
   :caption: Singularity on Slurm via SSH

   deployments:
     ssh-hpc:
       type: ssh
       config:
         hostname: login.hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa
     
     slurm-hpc:
       type: slurm
       config:
         partition: standard
       wraps: ssh-hpc
     
     singularity-container:
       type: singularity
       config:
         image: docker://python:3.10
       wraps: slurm-hpc
   
   workflows:
     example:
       type: cwl
       config:
         file: workflow.cwl
       bindings:
         - step: /analyze
           target:
             deployment: singularity-container

**Result:** 

1. Connect via SSH to HPC login node
2. Submit Slurm job
3. Run Singularity container in the job
4. Execute workflow step inside container

For details, see :doc:`advanced-patterns/stacked-locations`.

Wrap Specific Service
---------------------

.. code-block:: yaml
   :caption: Wrap specific service

   deployments:
     microservices:
       type: docker-compose
       config:
         file: docker-compose.yml
     
     compute-in-container:
       type: slurm
       config:
         partition: batch
       wraps:
         deployment: microservices
         service: compute-node

Conditional Bindings
--------------------

Use different deployments based on conditions (requires custom logic in workflow):

.. code-block:: yaml
   :caption: Environment-specific bindings

   workflows:
     adaptive-workflow:
       type: cwl
       config:
         file: workflow.cwl
       bindings:
         - step: /small_task
           target:
             deployment: local
         
         - step: /medium_task
           target:
             deployment: docker-cluster
         
         - step: /large_task
           target:
             deployment: hpc-cluster

Binding Best Practices
======================

* **Start simple:** Begin with ``step: /`` binding to single deployment, then optimize for specific steps
* **Group similar steps:** Bind related steps to the same deployment to minimize data transfers
* **Consider data locality:** Execute steps close to where data resides using port bindings
* **Use services for resource differentiation:** Define separate services for CPU vs GPU tasks
* **Test locally first:** Validate workflow logic with local execution before remote deployment

Complete Example
================

Here's a complete ``streamflow.yml`` example demonstrating hybrid execution:

.. code-block:: yaml
   :caption: Complete StreamFlow configuration

   version: v1.0
   
   workflows:
     data-pipeline:
       type: cwl
       config:
         file: pipeline.cwl
         settings: inputs.yml
       bindings:
         - step: /ingest
           target:
             deployment: cloud-workers
         - step: /process
           target:
             deployment: hpc-slurm
             service: compute-nodes
         - step: /visualize
           target:
             deployment: local
         - port: /raw_data
           target:
             deployment: hpc-storage
             workdir: /scratch/project/data
   
   deployments:
     cloud-workers:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config
     
     hpc-storage:
       type: ssh
       config:
         hostname: storage.hpc.edu
         username: researcher
         sshKey: ~/.ssh/hpc_key
     
     hpc-slurm:
       type: slurm
       config:
         partition: standard
       wraps: hpc-storage
       services:
         compute-nodes:
           nodes: 4
           time: "08:00:00"
     
     local:
       type: local

Validation
==========

Generate JSON Schema for IDE validation:

.. code-block:: bash

   streamflow schema > streamflow-schema.json

Configure your IDE to use the schema for auto-completion and validation of ``streamflow.yml`` files.

For binding troubleshooting, see :doc:`troubleshooting`.

Next: :doc:`running-workflows` to execute your configured workflows, or :doc:`advanced-patterns/index` for complex binding patterns.
