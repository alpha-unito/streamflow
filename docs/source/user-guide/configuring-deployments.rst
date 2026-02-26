=======================
Configuring Deployments
=======================

.. meta::
   :keywords: StreamFlow, deployment, connector, docker, kubernetes, slurm, ssh, configuration
   :description: Learn how to configure execution environments and deployments in StreamFlow

Overview
========

Deployments define where and how workflow steps execute. StreamFlow supports diverse execution environments including containers, cloud platforms, and HPC systems through a unified connector interface.

Deployment Concepts
===================

Understanding the deployment hierarchy:

========================  ========================================
Concept                   Description
========================  ========================================
**Deployment**            An entire infrastructure (unit of deployment)
**Service**               A type of compute resource within a deployment
**Location**              A single instance of a service (unit of scheduling)
**Connector**             Implementation that manages deployment lifecycle
========================  ========================================

**Example:**

* Deployment: ``my-k8s-cluster``
* Service: ``gpu-workers`` (Kubernetes deployment with GPU nodes)
* Locations: Individual pods created by Kubernetes

Deployment Configuration Structure
===================================

Deployments are defined in the StreamFlow configuration file (``streamflow.yml``):

.. code-block:: yaml
   :caption: streamflow.yml structure

   version: v1.0
   
   workflows:
     my-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
   
   deployments:
     deployment-name:
       type: connector-type
       config:
         # Connector-specific configuration
       services:
         service-name:
           # Service-specific configuration

Available Connectors
====================

StreamFlow provides connectors for various environments:

========================  ========================================
Connector Type            Use Case
========================  ========================================
``local``                 Local machine execution
``docker``                Docker containers
``docker-compose``        Docker Compose multi-container
``singularity``           Singularity/Apptainer containers
``kubernetes``            Kubernetes clusters
``helm``                  Helm charts on Kubernetes
``ssh``                   SSH to remote machines
``slurm``                 Slurm HPC scheduler
``pbs``                   PBS/Torque HPC scheduler
``flux``                  Flux Framework scheduler
``occam``                 OCCAM connector
========================  ========================================

For complete connector reference, see :doc:`/reference/index`.

Local Connector
===============

Execute on the local machine without containers:

.. code-block:: yaml
   :caption: Local deployment

   deployments:
     local:
       type: local

**Use Cases:**

* Testing workflows locally
* Running lightweight tools
* Development and debugging

**Notes:**

* No isolation between tasks
* Shares filesystem with StreamFlow process
* Fastest option for small-scale testing

Docker Connector
================

Execute in Docker containers:

.. code-block:: yaml
   :caption: Docker deployment with common options

   deployments:
     docker-python:
       type: docker
       config:
         image: python:3.10
         volumes:
           - /host/data:/container/data:ro
         environment:
           DATABASE_URL: postgresql://localhost/mydb
         gpus: all            # GPU access (optional)
         cpus: 4.0            # CPU limit (optional)
         memory: 8g           # Memory limit (optional)

See :doc:`/reference/connectors/docker` for all configuration options.

Docker Compose Connector
========================

Manage multi-container deployments:

.. code-block:: yaml
   :caption: Docker Compose deployment

   deployments:
     app-stack:
       type: docker-compose
       config:
         file: docker-compose.yml
       services:
         web:
           # Target the 'web' service from docker-compose.yml
         worker:
           # Target the 'worker' service

.. code-block:: yaml
   :caption: docker-compose.yml

   version: '3.8'
   services:
     web:
       image: nginx:latest
       ports:
         - "8080:80"
     worker:
       image: python:3.10
       volumes:
         - ./app:/app

**Use Cases:**

* Multi-container applications
* Service dependencies (database + application)
* Complex network configurations

Kubernetes Connector
====================

Deploy on Kubernetes clusters:

.. code-block:: yaml
   :caption: Kubernetes deployment

   deployments:
     k8s-cluster:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config
         namespace: streamflow
       services:
         compute:
           replicas: 5
           template:
             spec:
               containers:
                 - name: worker
                   image: python:3.10
                   resources:
                     requests:
                       memory: "4Gi"
                       cpu: "2"
                     limits:
                       memory: "8Gi"
                       cpu: "4"
                       nvidia.com/gpu: 1  # For GPU support

See :doc:`/reference/connectors/kubernetes` for node affinity, tolerations, and advanced configuration.

Helm Connector
==============

Deploy using Helm charts:

.. code-block:: yaml
   :caption: Helm deployment

   deployments:
     spark-cluster:
       type: helm
       config:
         chart: bitnami/spark
         release: streamflow-spark
         namespace: analytics
         values:
           worker:
             replicaCount: 3
             resources:
               limits:
                 cpu: 2
                 memory: 4Gi

**Use Cases:**

* Deploying complex applications with Helm charts
* Managing application lifecycle
* Using community charts (Spark, Airflow, etc.)

SSH Connector
=============

Execute on remote machines via SSH:

.. code-block:: yaml
   :caption: SSH deployment

   deployments:
     remote-server:
       type: ssh
       config:
         hostname: compute.example.com
         username: myuser
         sshKey: ~/.ssh/id_rsa
         sshKeyPassphrase: passphrase  # Optional
         port: 22  # Optional (default: 22)
         maxConnections: 10  # Optional (default: 1)
         nodes:  # For multiple hosts
           - hostname: node1.example.com
             username: user
             sshKey: ~/.ssh/id_rsa
           - hostname: node2.example.com
             username: user
             sshKey: ~/.ssh/id_rsa

See :doc:`/reference/connectors/ssh` for password authentication and additional options.

Slurm Connector
===============

Submit jobs to Slurm HPC schedulers:

.. code-block:: yaml
   :caption: Slurm deployment

   deployments:
     hpc-slurm:
       type: slurm
       config:
         hostname: login.hpc.example.edu
         username: researcher
         sshKey: ~/.ssh/hpc_key
         workdir: /scratch/researcher/streamflow
       services:
         compute:
           partition: standard
           nodes: 1
           ntasks: 16
           mem: 64G
           time: 02:00:00
           gres: gpu:v100:2  # For GPU allocation (optional)
           account: research-grant-123  # For account tracking (optional)
           qos: high  # For QoS (optional)

See :doc:`/reference/connectors/slurm` for all Slurm directives and batch options.

PBS Connector
=============

Submit jobs to PBS/Torque schedulers:

.. code-block:: yaml
   :caption: PBS deployment

   deployments:
     hpc-pbs:
       type: pbs
       config:
         hostname: pbs-login.hpc.edu
         username: researcher
         sshKey: ~/.ssh/id_rsa
         workdir: /home/researcher/jobs
       services:
         compute:
           queue: batch
           nodes: 2
           cpus: 32
           mem: 128gb
           walltime: "04:00:00"
           select: "2:ncpus=16:mem=64gb:ngpus=1"  # Alternative resource spec

See :doc:`/reference/connectors/pbs` for all PBS directives.

Singularity Connector
=====================

Execute in Singularity/Apptainer containers:

.. code-block:: yaml
   :caption: Singularity deployment

   deployments:
     singularity-hpc:
       type: singularity
       config:
         image: library://library/default/ubuntu:20.04
         # or image: docker://python:3.10
         # or image: /path/to/image.sif

Singularity is commonly used on HPC systems. See :doc:`/reference/connectors/singularity` for integration with Slurm/PBS.

Service Configuration
=====================

Services define resource subsets within deployments:

.. code-block:: yaml
   :caption: Multiple services in one deployment

   deployments:
     mixed-resources:
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
                   resources:
                     requests:
                       cpu: "2"
                       memory: "4Gi"
         
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
         
         memory-intensive:
           replicas: 3
           template:
             spec:
               containers:
                 - name: bigmem
                   image: r-base:latest
                   resources:
                     requests:
                       memory: "64Gi"

Then bind different workflow steps to different services based on resource needs.

Multi-Deployment Workflows
===========================

Use multiple deployments in one workflow:

.. code-block:: yaml
   :caption: Hybrid cloud-HPC workflow

   deployments:
     cloud-preprocessing:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config
       services:
         workers:
           replicas: 20
     
     hpc-computation:
       type: slurm
       config:
         hostname: supercomputer.edu
         username: researcher
         sshKey: ~/.ssh/id_rsa
       services:
         compute-nodes:
           partition: standard
           nodes: 10
     
     cloud-postprocessing:
       type: docker
       config:
         image: python:3.10
   
   bindings:
     - step: preprocess
       target:
         deployment: cloud-preprocessing
     - step: heavy_computation
       target:
         deployment: hpc-computation
     - step: visualize
       target:
         deployment: cloud-postprocessing

This enables hybrid workflows across cloud and HPC, cost optimization, and data locality.

Validation
==========

After configuring deployments, verify they work by running a simple test workflow:

.. code-block:: yaml
   :caption: test-deployment.yml

   version: v1.0
   
   workflows:
     test:
       type: cwl
       config:
         file: test.cwl
   
   deployments:
     docker-test:
       type: docker
       config:
         image: python:3.10
   
   bindings:
     - step: /
       target:
         deployment: docker-test

.. code-block:: bash

   $ streamflow run test-deployment.yml --debug

If the workflow completes successfully, your deployment is configured correctly. For troubleshooting deployment issues, see :doc:`troubleshooting`.

Next: :doc:`binding-workflows` to bind workflow steps to deployments, or see :doc:`/reference/index` for complete connector documentation.
