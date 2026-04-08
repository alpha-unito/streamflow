====================
Kubernetes Translator
====================

.. meta::
   :keywords: StreamFlow, Kubernetes, CWL, DockerRequirement, cloud, pods
   :description: Kubernetes translator for CWL DockerRequirement in StreamFlow

Overview
========

The **Kubernetes Translator** converts CWL ``DockerRequirement`` specifications into :doc:`/reference/connectors/kubernetes` deployments. It runs each containerized step as a Kubernetes pod.

**Use Cases:**

* Cloud-native workflows
* Scalable compute clusters
* Multi-tenant environments
* Enterprise Kubernetes platforms

Configuration
=============

.. jsonschema:: https://streamflow.di.unito.it/schemas/cwl/requirement/docker/kubernetes.json
    :lift_description: true

Examples
========

Basic Usage
-----------

Run workflow on Kubernetes:

.. code-block:: yaml
   :caption: streamflow.yml - Basic Kubernetes

   version: v1.0
   workflows:
     k8s-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /
             deployment:
               type: kubernetes
               config:
                 namespace: workflows

Custom Namespace
----------------

Deploy to specific namespace:

.. code-block:: yaml
   :caption: Custom namespace

   workflows:
     compute-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /compute
             deployment:
               type: kubernetes
               config:
                 namespace: compute-intensive
                 maxCores: 8
                 maxMemory: 16Gi

In-Cluster Execution
--------------------

Run StreamFlow inside Kubernetes cluster:

.. code-block:: yaml
   :caption: In-cluster configuration

   workflows:
     in-cluster-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /
             deployment:
               type: kubernetes
               config:
                 inCluster: true
                 namespace: default

**Requirements:**

* StreamFlow must run inside a Kubernetes pod
* ServiceAccount with appropriate RBAC permissions
* See :doc:`/reference/connectors/kubernetes` for RBAC configuration

Multi-Namespace Workflow
-------------------------

Different steps in different namespaces:

.. code-block:: yaml
   :caption: Multi-namespace deployment

   workflows:
     multi-ns-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /preprocess
             deployment:
               type: kubernetes
               config:
                 namespace: preprocessing
           - step: /analyze
             deployment:
               type: kubernetes
               config:
                 namespace: compute
           - step: /visualize
             deployment:
               type: kubernetes
               config:
                 namespace: visualization

Related Documentation
=====================

**Connectors:**
   - :doc:`/reference/connectors/kubernetes` - Kubernetes connector reference

**CWL Docker Translators:**
   - :doc:`index` - CWL Docker Translators overview
   - :doc:`docker` - Docker translator
   - :doc:`singularity` - Singularity translator

**User Guide:**
   - :doc:`/user-guide/writing-workflows` - Writing CWL workflows
   - :doc:`/user-guide/running-workflows` - Running on Kubernetes
