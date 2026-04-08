=================
Stacked Locations
=================

.. meta::
   :keywords: StreamFlow, stacked locations, wraps, deployment hierarchy, HPC, containers
   :description: Learn how to create complex execution environments by stacking deployments using the wraps directive

Overview
========

Stacked locations enable you to describe complex, layered execution environments by composing multiple deployments. For example, you can run Singularity containers inside Slurm jobs accessed through SSH—a common pattern in HPC environments.

Understanding Stacking
======================

**Stacking Concept:**

Deployments can "wrap" other deployments, creating execution hierarchies that match real-world infrastructure.

**Common Pattern:**

``Container → Queue Manager → SSH → Local``

**Benefits:**

* Separation of concerns (networking vs. scheduling vs. environment)
* Reusable deployment definitions
* Match actual infrastructure topology
* Simplify complex configurations

When to Use Stacked Locations
==============================

============================  ========================================
Scenario                      Example
============================  ========================================
**HPC Access**                SSH to login node, submit to queue manager
**Containerized HPC**         Containers launched by queue managers
**Complex Microservices**     Target specific services in Docker Compose
**Multi-Hop Access**          Jump hosts to reach compute resources
**Environment Layering**      Python virtualenv in Singularity on Slurm
============================  ========================================

Basic Stacking with wraps
==========================

Simple Two-Layer Stack
----------------------

Connect to HPC via SSH, then submit to Slurm:

.. code-block:: yaml
   :caption: SSH wrapping example

   version: v1.0
   
   workflows:
     compute-job:
       type: cwl
       config:
         file: workflow.cwl
       bindings:
         - step: /compute
           target:
             deployment: slurm-hpc
   
   deployments:
     ssh-hpc:
       type: ssh
       config:
         hostname: login.hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa
         maxConnections: 5
     
     slurm-hpc:
       type: slurm
       config:
         partition: compute
         nodes: 1
         ntasks: 16
       wraps: ssh-hpc

**Execution Flow:**

1. StreamFlow opens SSH connection to ``login.hpc.edu``
2. Through SSH, submits Slurm job to ``compute`` partition
3. Job executes on compute node
4. Results return through SSH connection

**Key Point:** The ``wraps`` directive tells StreamFlow that ``slurm-hpc`` wraps ``ssh-hpc``.

Three-Layer Stack
-----------------

Singularity container in Slurm job via SSH:

.. code-block:: yaml
   :caption: Three-layer stack

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
         partition: gpu
         nodes: 1
         ntasks: 8
         gres: gpu:1
       wraps: ssh-hpc
     
     singularity-env:
       type: singularity
       config:
         image: docker://tensorflow/tensorflow:latest-gpu
       wraps: slurm-hpc
   
   workflows:
     ml-training:
       type: cwl
       config:
         file: train.cwl
       bindings:
         - step: /train
           target:
             deployment: singularity-env

**Execution Flow:**

1. SSH to login node (``ssh-hpc``)
2. Submit Slurm job requesting GPU (``slurm-hpc``)
3. Within job, launch Singularity container (``singularity-env``)
4. Execute training inside container

**Result:** TensorFlow job runs in GPU-enabled container on Slurm-managed node.

Practical Examples
==================

Classic HPC Pattern
-------------------

Most HPC facilities use this structure:

.. code-block:: yaml
   :caption: Standard HPC configuration

   deployments:
     # Network layer: Access to HPC
     hpc-login:
       type: ssh
       config:
         hostname: login.supercomputer.edu
         username: researcher
         sshKey: ~/.ssh/hpc_key
         maxConnections: 10
         # Connection through firewall/VPN
         proxyJump: gateway.university.edu
     
     # Scheduling layer: Resource management
     hpc-scheduler:
       type: slurm
       config:
         partition: high-memory
         nodes: 2
         ntasksPerNode: 32
         mem: 256GB
         time: 24:00:00
         account: project-12345
       wraps: hpc-login
     
     # Environment layer: Software stack
     hpc-container:
       type: singularity
       config:
         image: /apps/containers/bioinformatics.sif
         bind:
           - /scratch:/scratch
           - /projects:/projects
       wraps: hpc-scheduler
   
   workflows:
     genome-assembly:
       type: cwl
       config:
         file: assembly.cwl
       bindings:
         - step: /assemble
           target:
             deployment: hpc-container

**Why Three Layers:**

* **SSH Layer:** Handles network access, authentication
* **Slurm Layer:** Manages compute resources, scheduling
* **Singularity Layer:** Provides consistent software environment

**Best Practice:** Keep each layer focused on single responsibility.

Docker Compose with Slurm
--------------------------

Run Slurm jobs that target services in Docker Compose:

.. code-block:: yaml
   :caption: Slurm wrapping Docker Compose service

   deployments:
     # Complex deployment: microservices architecture
     microservices:
       type: docker-compose
       config:
         file: docker-compose.yml
     
     # Target specific service for compute jobs
     batch-processor:
       type: slurm
       config:
         partition: batch
         nodes: 1
         ntasks: 4
       wraps:
         deployment: microservices
         service: controller

**docker-compose.yml:**

.. code-block:: yaml
   :caption: Docker Compose with multiple services

   version: '3'
   services:
     controller:
       image: batch-controller:latest
       ports:
         - "5000:5000"
     
     database:
       image: postgres:13
       volumes:
         - db-data:/var/lib/postgresql/data
     
     worker:
       image: batch-worker:latest
       depends_on:
         - database

**Behavior:**

* Slurm jobs target only the ``controller`` service
* Other services (database, worker) run independently
* Enables complex architectures with targeted execution

Kubernetes with SSH
-------------------

Access remote Kubernetes cluster:

.. code-block:: yaml
   :caption: Kubernetes through SSH tunnel

   deployments:
     k8s-gateway:
       type: ssh
       config:
         hostname: k8s-master.cloud.example.com
         username: admin
         sshKey: ~/.ssh/k8s_key
     
     k8s-cluster:
       type: kubernetes
       config:
         kubeconfig: /home/admin/.kube/config
         namespace: streamflow-jobs
       wraps: k8s-gateway

**Use Case:** Kubernetes API not directly accessible, must tunnel through gateway.

ConnectorWrapper Interface
==========================

Which Connectors Support Wrapping?
-----------------------------------

Only connectors implementing ``ConnectorWrapper`` interface support the ``wraps`` directive:

===================  =====================  ===========================
Connector            Supports wraps         Default Wraps
===================  =====================  ===========================
``local``            No                     N/A
``ssh``              No                     N/A
``docker``           Yes                    LocalConnector
``docker-compose``   Yes                    LocalConnector
``kubernetes``       Yes                    LocalConnector
``singularity``      Yes                    LocalConnector
``slurm``            Yes                    LocalConnector
``pbs``              Yes                    LocalConnector
===================  =====================  ===========================

**Important Rules:**

1. **Only wrappers can use wraps directive**
   
   .. code-block:: yaml
   
      # ERROR: ssh does not implement ConnectorWrapper
      deployments:
        invalid:
          type: ssh
          wraps: local  # Will fail during initialization

2. **Default wrapping**
   
   .. code-block:: yaml
   
      # These are equivalent
      deployments:
        explicit:
          type: docker
          wraps: local
        
        implicit:
          type: docker
          # Automatically wraps LocalConnector

3. **Single inner location**
   
   .. code-block:: yaml
   
      # ERROR: Cannot wrap multiple deployments
      deployments:
        invalid:
          type: slurm
          wraps: [ssh-1, ssh-2]  # Not supported

Multiple Wrappers, Single Inner
--------------------------------

One deployment can be wrapped by multiple outer deployments:

.. code-block:: yaml
   :caption: Shared SSH connection

   deployments:
     # Shared inner layer
     hpc-access:
       type: ssh
       config:
         hostname: hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa
     
     # Multiple queue managers wrap same SSH
     slurm-short:
       type: slurm
       config:
         partition: short
         time: 01:00:00
       wraps: hpc-access
     
     slurm-long:
       type: slurm
       config:
         partition: long
         time: 48:00:00
       wraps: hpc-access
     
     pbs-gpu:
       type: pbs
       config:
         queue: gpu
         walltime: 24:00:00
       wraps: hpc-access

**Benefit:** Single SSH connection shared by multiple schedulers.

Service-Level Wrapping
======================

Target Specific Service
-----------------------

Wrap individual services in complex deployments:

.. code-block:: yaml
   :caption: Service-level wrapping

   deployments:
     app-stack:
       type: docker-compose
       config:
         file: stack.yml
       services:
         frontend:
           ports: ["80:80"]
         backend:
           ports: ["3000:3000"]
         worker:
           replicas: 3
     
     # Wrap only the worker service
     batch-jobs:
       type: slurm
       config:
         partition: batch
       wraps:
         deployment: app-stack
         service: worker

**Use Case:** In a microservices architecture, only the worker service needs batch processing, while frontend/backend run continuously.

Full Syntax
-----------

.. code-block:: yaml
   :caption: Service wrapping syntax

   deployments:
     outer-deployment:
       type: connector-type
       config:
         # Outer configuration
       wraps:
         deployment: inner-deployment-name
         service: service-name

**When to Use:**

* Docker Compose with multiple services
* Kubernetes deployments with multiple containers
* Complex architectures where only specific services need wrapping

Deployment Order
================

StreamFlow's DeploymentManager guarantees correct deployment order:

.. code-block:: yaml
   :caption: Complex stack

   deployments:
     ssh-access:
       type: ssh
       config: { ... }
     
     slurm-scheduler:
       type: slurm
       config: { ... }
       wraps: ssh-access
     
     container-env:
       type: singularity
       config: { ... }
       wraps: slurm-scheduler

**Deployment Order:** (innermost to outermost)

1. ``ssh-access`` - Establish SSH connection
2. ``slurm-scheduler`` - Set up Slurm submission
3. ``container-env`` - Prepare container environment

**Undeployment Order:** (outermost to innermost)

1. ``container-env`` - Clean up container
2. ``slurm-scheduler`` - Cancel/cleanup Slurm jobs
3. ``ssh-access`` - Close SSH connection

**Automatic:** StreamFlow handles ordering automatically based on ``wraps`` relationships.

Advanced Patterns
=================

Conditional Stacking
--------------------

Use different stacks for different steps:

.. code-block:: yaml
   :caption: Different stacks per step

   deployments:
     ssh-hpc:
       type: ssh
       config: { ... }
     
     slurm-cpu:
       type: slurm
       config:
         partition: cpu
       wraps: ssh-hpc
     
     slurm-gpu:
       type: slurm
       config:
         partition: gpu
         gres: gpu:1
       wraps: ssh-hpc
     
     singularity-cpu:
       type: singularity
       config:
         image: /apps/cpu-tools.sif
       wraps: slurm-cpu
     
     singularity-gpu:
       type: singularity
       config:
         image: /apps/gpu-tools.sif
         nv: true  # NVIDIA GPU support
       wraps: slurm-gpu
   
   workflows:
     pipeline:
       type: cwl
       config:
         file: pipeline.cwl
       bindings:
         - step: /preprocess
           target:
             deployment: singularity-cpu
         
         - step: /train_model
           target:
             deployment: singularity-gpu
         
         - step: /evaluate
           target:
             deployment: singularity-cpu

**Pattern:** CPU and GPU workloads use different stacks with appropriate resources.

Reusable Base Layers
--------------------

Define common base layers, specialize on top:

.. code-block:: yaml
   :caption: Reusable base

   deployments:
     # Base: SSH access (reusable)
     hpc-ssh:
       type: ssh
       config:
         hostname: hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa
     
     # Specializations: Different queues
     quick-jobs:
       type: slurm
       config:
         partition: quick
         time: 00:15:00
       wraps: hpc-ssh
     
     standard-jobs:
       type: slurm
       config:
         partition: standard
         time: 24:00:00
       wraps: hpc-ssh
     
     bigmem-jobs:
       type: slurm
       config:
         partition: bigmem
         mem: 500GB
       wraps: hpc-ssh

**Benefit:** Don't repeat SSH configuration for each queue.

Migration from StreamFlow v0.1
===============================

Deprecated Pattern (Still Supported)
-------------------------------------

StreamFlow v0.1 embedded SSH config in queue manager:

.. code-block:: yaml
   :caption: Old pattern (deprecated, will be removed in v0.3)

   deployments:
     slurm-hpc:
       type: slurm
       config:
         # SSH properties directly in Slurm config
         hostname: hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa
         # Slurm properties
         partition: compute
         nodes: 1

**Status:** Works in v0.2, but deprecated. Will be removed in v0.3.

New Pattern (Recommended)
--------------------------

Use explicit stacking:

.. code-block:: yaml
   :caption: New pattern (recommended)

   deployments:
     ssh-hpc:
       type: ssh
       config:
         hostname: hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa
     
     slurm-hpc:
       type: slurm
       config:
         partition: compute
         nodes: 1
       wraps: ssh-hpc

**Migration:** Separate SSH config into dedicated deployment, use ``wraps``.

Best Practices
==============

1. **Separate Concerns**
   
   Each layer should have single responsibility:
   
   .. code-block:: yaml
   
      # Good: Clear separation
      ssh-layer:       # Networking
      slurm-layer:     # Scheduling
      container-layer: # Environment

2. **Name Layers Descriptively**
   
   .. code-block:: yaml
   
      # Good
      deployments:
        hpc-ssh-access:
          type: ssh
        hpc-gpu-queue:
          type: slurm
        cuda-environment:
          type: singularity
      
      # Avoid
      deployments:
        deployment1:
          type: ssh
        deployment2:
          type: slurm

3. **Document Stacking Relationships**
   
   .. code-block:: yaml
   
      deployments:
        # Layer 1: Network access to HPC facility
        hpc-login:
          type: ssh
          config: { ... }
        
        # Layer 2: Slurm scheduler (wraps hpc-login)
        hpc-slurm:
          type: slurm
          config: { ... }
          wraps: hpc-login
        
        # Layer 3: Singularity container (wraps hpc-slurm)
        analysis-env:
          type: singularity
          config: { ... }
          wraps: hpc-slurm

4. **Test Each Layer Independently**
   
   Verify each deployment works before stacking:
   
   .. code-block:: bash
   
      # Test SSH connection
      ssh -i ~/.ssh/id_rsa user@hpc.edu "hostname"
      
      # Test Slurm submission (through SSH)
      ssh -i ~/.ssh/id_rsa user@hpc.edu "sbatch --wrap 'hostname'"
      
      # Test Singularity container
      singularity exec container.sif command

5. **Use Defaults Wisely**
   
   If wrapping local execution, omit ``wraps`` directive:
   
   .. code-block:: yaml
   
      # These are equivalent
      deployments:
        explicit:
          type: docker
          wraps: local
        
        implicit:
          type: docker
          # Implicitly wraps local

6. **Reuse Base Deployments**
   
   Create shared base layers for consistency:
   
   .. code-block:: yaml
   
      deployments:
        shared-ssh:
          type: ssh
          config: { ... }
        
        # Multiple deployments reuse base
        slurm-cpu:
          type: slurm
          config: { ... }
          wraps: shared-ssh
        
        slurm-gpu:
          type: slurm
          config: { ... }
          wraps: shared-ssh

Troubleshooting
===============

Invalid wraps Directive
-----------------------

**Problem:** ``Error: Connector 'X' does not support wraps directive``

**Cause:** Attempting to use ``wraps`` on connector that doesn't implement ConnectorWrapper

**Solution:**

Check if connector supports wrapping:

.. code-block:: yaml

   # ERROR: ssh cannot wrap
   deployments:
     invalid:
       type: ssh
       wraps: local

**Fix:** Remove ``wraps`` directive or use connector that supports it.

Cannot Connect Through Stack
-----------------------------

**Problem:** Deployment succeeds but cannot execute commands

**Cause:** Connectivity issue in one layer

**Solution:**

Test each layer:

.. code-block:: bash

   # Test inner layer (SSH)
   ssh user@host "echo success"
   
   # Test middle layer (Slurm through SSH)
   ssh user@host "sinfo"  # Check Slurm is accessible
   
   # Test outer layer (container)
   ssh user@host "singularity exec image.sif echo success"

**Check logs:**

.. code-block:: bash

   streamflow run streamflow.yml --debug

Look for connection failures at specific layers.

Wrong Deployment Order
----------------------

**Problem:** Deployment fails with initialization errors

**Cause:** StreamFlow may have incorrect dependency order

**Solution:**

Verify ``wraps`` relationships form valid tree:

.. code-block:: yaml

   # Valid tree structure
   A wraps: local
   B wraps: A
   C wraps: B

   # Invalid: circular
   A wraps: B
   B wraps: A  # ERROR: circular dependency

**Fix:** Ensure no circular dependencies in wrapping.

Service Not Found
-----------------

**Problem:** ``Service 'X' not found in deployment 'Y'``

**Cause:** Service name doesn't exist in wrapped deployment

**Solution:**

1. **Check service name in deployment config:**
   
   .. code-block:: yaml
   
      deployments:
        compose-app:
          type: docker-compose
          config:
            file: app.yml
          services:
            web:       # Service name
            worker:    # Service name
            database:  # Service name

2. **Match exactly in wraps:**
   
   .. code-block:: yaml
   
      batch:
        type: slurm
        wraps:
          deployment: compose-app
          service: worker  # Must match exactly

Container Not Starting
----------------------

**Problem:** Container fails to start in stacked environment

**Cause:** Resource constraints or image access issues

**Solution:**

1. **Check Slurm job has sufficient resources:**
   
   .. code-block:: yaml
   
      slurm-layer:
        type: slurm
        config:
          nodes: 1
          ntasks: 8
          mem: 32GB  # Ensure sufficient memory

2. **Verify container image accessible from compute node:**
   
   .. code-block:: bash
   
      # On compute node (in Slurm job)
      ssh user@host 'srun singularity exec /path/to/image.sif ls'

3. **Check bind mounts exist:**
   
   .. code-block:: yaml
   
      singularity-layer:
        type: singularity
        config:
          image: /apps/image.sif
          bind:
            - /data:/data      # Verify /data exists on compute node
            - /scratch:/scratch

Real-World Examples
===================

Bioinformatics HPC Workflow
----------------------------

.. code-block:: yaml
   :caption: Complete bioinformatics stack

   version: v1.0
   
   deployments:
     # Layer 1: SSH to HPC login node
     bioinfo-hpc:
       type: ssh
       config:
         hostname: login.biocompute.edu
         username: researcher
         sshKey: ~/.ssh/bioinfo_key
         maxConnections: 5
     
     # Layer 2: Slurm scheduler
     slurm-highmem:
       type: slurm
       config:
         partition: highmem
         nodes: 1
         ntasksPerNode: 16
         mem: 128GB
         time: 72:00:00
         account: bio-project-001
       wraps: bioinfo-hpc
     
     # Layer 3: Singularity with bioinformatics tools
     biotools-container:
       type: singularity
       config:
         image: /apps/containers/bioinfo-suite-2024.sif
         bind:
           - /gpfs/genomics:/data
           - /gpfs/scratch:/scratch
       wraps: slurm-highmem
   
   workflows:
     genome-analysis:
       type: cwl
       config:
         file: analysis.cwl
       bindings:
         - step: /quality-control
           target:
             deployment: biotools-container
         
         - step: /alignment
           target:
             deployment: biotools-container
         
         - step: /variant-calling
           target:
             deployment: biotools-container
         
         # Reference genome on shared storage
         - port: /reference
           target:
             deployment: bioinfo-hpc
             workdir: /gpfs/genomics/references/hg38

**Architecture:**

* All tools execute in consistent Singularity environment
* Slurm manages compute resources
* SSH provides access through firewall
* Shared storage accessible at all layers

Cloud-HPC Hybrid
----------------

.. code-block:: yaml
   :caption: Hybrid cloud-HPC deployment

   deployments:
     # Cloud: Direct Kubernetes access
     cloud-k8s:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config-cloud
         namespace: preprocessing
     
     # HPC: SSH access
     hpc-ssh:
       type: ssh
       config:
         hostname: hpc.university.edu
         username: user
         sshKey: ~/.ssh/hpc_key
     
     # HPC: Slurm for computation
     hpc-slurm:
       type: slurm
       config:
         partition: compute
         nodes: 4
         ntasksPerNode: 32
       wraps: hpc-ssh
     
     # HPC: Container environment
     hpc-container:
       type: singularity
       config:
         image: /shared/apps/compute-env.sif
       wraps: hpc-slurm
   
   workflows:
     hybrid-pipeline:
       type: cwl
       config:
         file: pipeline.cwl
       bindings:
         # Light preprocessing on cloud
         - step: /preprocess
           target:
             deployment: cloud-k8s
         
         # Heavy computation on HPC
         - step: /simulate
           target:
             deployment: hpc-container
         
         # Visualization back on cloud
         - step: /visualize
           target:
             deployment: cloud-k8s

**Pattern:**

* Cloud (single layer): Kubernetes for lightweight tasks
* HPC (three layers): SSH → Slurm → Singularity for heavy computation
* Data automatically transferred between environments

Next Steps
==========

After mastering stacked locations:

* :doc:`multiple-targets` - Distribute work across resources
* :doc:`port-targets` - Control data flow between layers
* :doc:`/developer-guide/extension-points/connector` - Create custom connectors
* :doc:`/reference/configuration/deployment-config` - Complete deployment reference

Related Topics
==============

* :doc:`/user-guide/configuring-deployments` - Deployment fundamentals
* :doc:`/user-guide/binding-workflows` - Binding concepts
* :doc:`/developer-guide/core-interfaces/deployment` - Deployment internals
* :ref:`ConnectorWrapper <ConnectorWrapper>` - Wrapper interface details
