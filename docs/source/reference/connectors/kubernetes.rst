====================
Kubernetes Connector
====================

.. meta::
   :keywords: StreamFlow, kubernetes, k8s, cloud, orchestration
   :description: Kubernetes connector reference for StreamFlow

Overview
========

The Kubernetes connector executes workflow tasks as Kubernetes pods, providing multi-node scalability, resource management, and cloud-native integration.

Quick Reference
===============

============  ====================================
Type          ``kubernetes``
Category      Cloud
Scalability   Multi-node, auto-scaling
Best For      Cloud environments, production
============  ====================================

Examples
========

With Resource Requests
-----------------------

.. code-block:: yaml

   deployments:
     k8s-workers:
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

With GPU Support
----------------

.. code-block:: yaml

   deployments:
     k8s-gpu:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config
       services:
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

With Node Affinity
------------------

.. code-block:: yaml

   deployments:
     k8s-affinity:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config
       services:
         specific-nodes:
           replicas: 3
           template:
             spec:
               containers:
                 - name: worker
                   image: python:3.10
               affinity:
                 nodeAffinity:
                   requiredDuringSchedulingIgnoredDuringExecution:
                     nodeSelectorTerms:
                       - matchExpressions:
                           - key: node-type
                             operator: In
                             values:
                               - compute

Multiple Services
-----------------

.. code-block:: yaml

   deployments:
     k8s-mixed:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config
       services:
         cpu-workers:
           replicas: 10
           template:
             spec:
               containers:
                 - name: cpu-worker
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

Prerequisites
=============

* Kubernetes cluster access
* ``kubectl`` installed and configured
* Valid kubeconfig file
* Appropriate RBAC permissions
* Namespace exists (or permission to create)

Pod Specifications
==================

The ``template`` field accepts standard Kubernetes pod template specifications. See `Kubernetes Pod documentation <https://kubernetes.io/docs/concepts/workloads/pods/>`_ for complete options.

Platform Support
================

**Linux:** Full support  
**macOS:** Full support  
**Windows:** Not supported

Configuration
=============

.. jsonschema:: https://streamflow.di.unito.it/schemas/deployment/connector/kubernetes.json
    :lift_description: true

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/configuring-deployments` - Deployment configuration guide
   - :doc:`/user-guide/running-workflows` - Workflow execution
   - :doc:`/user-guide/troubleshooting` - Kubernetes troubleshooting

**Connectors:**
   - :doc:`index` - All cloud connectors
   - :doc:`helm3` - For Helm chart deployments
   - :doc:`/reference/connectors/docker` - For local Docker execution

**External Resources:**
   - :doc:`/reference/cwl-docker-translators/kubernetes` - CWL Kubernetes translator
   - `Kubernetes Documentation <https://kubernetes.io/docs/>`_
