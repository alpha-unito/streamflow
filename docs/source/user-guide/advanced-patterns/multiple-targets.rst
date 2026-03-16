================
Multiple Targets
================

.. meta::
   :keywords: StreamFlow, multiple targets, binding filters, load balancing, scheduling
   :description: Learn how to bind workflow steps to multiple execution targets for load balancing and flexibility

Overview
========

StreamFlow allows binding workflow steps to multiple execution targets, enabling load balancing, failover, and flexible resource allocation. This pattern is especially useful for scatter operations and workflows with variable workloads.

Use Cases
=========

========================  ========================================
Scenario                  Benefit
========================  ========================================
**Scatter Operations**    Distribute parallel tasks across clusters
**Load Balancing**        Spread work across multiple resources
**Failover**              Use backup resources if primary unavailable
**Hybrid Execution**      Use mix of cloud and HPC resources
**Cost Optimization**     Use cheaper resources when available
========================  ========================================

Basic Multiple Target Binding
==============================

Simple Configuration
--------------------

Bind a step to multiple deployments:

.. code-block:: yaml
   :caption: Multiple targets configuration

   version: v1.0
   
   workflows:
     my-workflow:
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
       config:
         hostname: hpc1.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa
     
     cluster-2:
       type: slurm
       config:
         hostname: hpc2.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa
     
     cluster-3:
       type: slurm
       config:
         hostname: hpc3.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa

**Behavior:**

* StreamFlow scheduler evaluates targets in order
* Tasks scheduled to first available target
* Subsequent tasks may use different targets

Scatter with Multiple Targets
------------------------------

Particularly useful for scatter operations:

.. code-block:: yaml
   :caption: workflow.cwl - Scatter workflow

   cwlVersion: v1.2
   class: Workflow
   
   requirements:
     ScatterFeatureRequirement: {}
   
   inputs:
     input_files: File[]
   
   outputs:
     results:
       type: File[]
       outputSource: process/output
   
   steps:
     process:
       run: process-tool.cwl
       scatter: input_file
       in:
         input_file: input_files
       out: [output]

.. code-block:: yaml
   :caption: streamflow.yml - Bind to multiple targets

   bindings:
     - step: /process
       target:
         - deployment: cluster-1
         - deployment: cluster-2
         - deployment: cluster-3

**Result:**

* If ``input_files`` has 30 items, creates 30 scattered tasks
* Tasks distributed across all three clusters
* Maximizes resource utilization

Service-Level Multiple Targets
===============================

Target multiple services within or across deployments:

.. code-block:: yaml
   :caption: Multiple service targets

   bindings:
     - step: /compute
       target:
         - deployment: k8s-cluster
           service: cpu-workers
         - deployment: k8s-cluster
           service: gpu-workers
         - deployment: hpc-cluster
           service: compute-nodes
   
   deployments:
     k8s-cluster:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config
       services:
         cpu-workers:
           replicas: 10
         gpu-workers:
           replicas: 2
     
     hpc-cluster:
       type: slurm
       config:
         hostname: hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa
       services:
         compute-nodes:
           partition: standard
           nodes: 4

Binding Filters
===============

Filters control how StreamFlow selects among multiple targets.

Default Behavior
----------------

Without filters, targets are evaluated in order of appearance:

.. code-block:: yaml

   bindings:
     - step: /process
       target:
         - deployment: cluster-1  # Tried first
         - deployment: cluster-2  # Tried second
         - deployment: cluster-3  # Tried third

**Selection Logic:**

1. Try cluster-1
2. If unavailable/busy, try cluster-2
3. If unavailable/busy, try cluster-3
4. If all unavailable, wait and retry

Shuffle Filter
--------------

Randomize target evaluation order:

.. code-block:: yaml
   :caption: Shuffle filter

   bindings:
     - step: /process
       target:
         - deployment: cluster-1
         - deployment: cluster-2
         - deployment: cluster-3
       filters:
         - shuffle

**Benefits:**

* Distributes load randomly
* Prevents overloading first target
* Better load balancing for bursty workloads

**Use Cases:**

* Multiple equivalent resources
* Load balancing across identical clusters
* Avoiding hotspots

Multiple Filters
----------------

Apply multiple filters in sequence:

.. code-block:: yaml
   :caption: Multiple filters

   bindings:
     - step: /process
       target:
         - deployment: cluster-1
         - deployment: cluster-2
         - deployment: cluster-3
         - deployment: cluster-4
       filters:
         - filter-type-1
         - filter-type-2
         - shuffle

Filters are applied in order, each transforming the target list.

Custom Filters
--------------

Create custom binding filters for advanced selection logic. See :doc:`/developer-guide/extension-points/binding-filter`.

**Example Custom Logic:**

* Select based on current queue wait times
* Prefer targets with local data
* Use cost-based selection
* Time-of-day routing

Practical Examples
==================

Hybrid Cloud-HPC
----------------

.. code-block:: yaml
   :caption: Hybrid execution

   workflows:
     data-pipeline:
       type: cwl
       config:
         file: pipeline.cwl
       bindings:
         # Lightweight preprocessing on cloud
         - step: /preprocess
           target:
             - deployment: aws-cluster
             - deployment: gcp-cluster
           filters:
             - shuffle
         
         # Heavy computation on HPC
         - step: /compute
           target:
             - deployment: hpc-primary
             - deployment: hpc-backup
         
         # Visualization back on cloud
         - step: /visualize
           target:
             - deployment: aws-cluster
   
   deployments:
     aws-cluster:
       type: kubernetes
       config: { ... }
     
     gcp-cluster:
       type: kubernetes
       config: { ... }
     
     hpc-primary:
       type: slurm
       config: { ... }
     
     hpc-backup:
       type: slurm
       config: { ... }

**Strategy:**

* Preprocessing uses either cloud provider
* Computation uses HPC with backup
* Visualization returns to cloud

Cost-Optimized Execution
-------------------------

.. code-block:: yaml
   :caption: Cost optimization

   bindings:
     - step: /analysis
       target:
         - deployment: on-premise    # Free
         - deployment: spot-instances  # Cheap
         - deployment: on-demand      # Expensive backup
   
   deployments:
     on-premise:
       type: local
     
     spot-instances:
       type: kubernetes
       config:
         # Kubernetes with spot/preemptible instances
         ...
     
     on-demand:
       type: kubernetes
       config:
         # Kubernetes with on-demand instances
         ...

**Cost Strategy:**

1. Use free on-premise if available
2. Use cheap spot instances
3. Fall back to expensive on-demand only if needed

Geographic Distribution
-----------------------

.. code-block:: yaml
   :caption: Geographic targets

   bindings:
     - step: /process
       target:
         - deployment: us-east
         - deployment: us-west
         - deployment: eu-central
         - deployment: asia-pacific
       filters:
         - shuffle
   
   deployments:
     us-east:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config-us-east
     
     us-west:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config-us-west
     
     eu-central:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config-eu
     
     asia-pacific:
       type: kubernetes
       config:
         kubeconfig: ~/.kube/config-apac

**Benefits:**

* Global load distribution
* Reduced latency for distributed data
* Regulatory compliance (data locality)

Advanced Patterns
=================

Tiered Resource Strategy
-------------------------

.. code-block:: yaml
   :caption: Resource tiers

   bindings:
     - step: /light_task
       target:
         - deployment: small-vms
         - deployment: medium-vms
         - deployment: large-vms
     
     - step: /medium_task
       target:
         - deployment: medium-vms
         - deployment: large-vms
     
     - step: /heavy_task
       target:
         - deployment: large-vms
         - deployment: gpu-nodes

**Strategy:** Match task requirements to appropriate resources.

Per-Step Multiple Targets
--------------------------

Different steps use different target sets:

.. code-block:: yaml

   bindings:
     # I/O intensive: use fast storage nodes
     - step: /read_data
       target:
         - deployment: storage-node-1
         - deployment: storage-node-2
     
     # CPU intensive: use compute nodes
     - step: /compute
       target:
         - deployment: compute-node-1
         - deployment: compute-node-2
         - deployment: compute-node-3
     
     # Memory intensive: use bigmem nodes
     - step: /analyze
       target:
         - deployment: bigmem-node-1
         - deployment: bigmem-node-2

Conditional Targeting
---------------------

While StreamFlow doesn't support conditional bindings directly, use CWL conditional execution with multiple target bindings:

.. code-block:: yaml
   :caption: workflow.cwl with conditions

   cwlVersion: v1.2
   class: Workflow
   requirements:
     InlineJavascriptRequirement: {}
   
   inputs:
     use_gpu: boolean
     data: File
   
   steps:
     cpu_process:
       when: $(inputs.use_gpu == false)
       run: cpu-tool.cwl
       in:
         use_gpu: use_gpu
         input: data
       out: [output]
     
     gpu_process:
       when: $(inputs.use_gpu == true)
       run: gpu-tool.cwl
       in:
         use_gpu: use_gpu
         input: data
       out: [output]

.. code-block:: yaml
   :caption: streamflow.yml with separate bindings

   bindings:
     - step: /cpu_process
       target:
         - deployment: cpu-cluster-1
         - deployment: cpu-cluster-2
     
     - step: /gpu_process
       target:
         - deployment: gpu-cluster-1
         - deployment: gpu-cluster-2

Monitoring and Debugging
=========================

Track Target Usage
------------------

Generate reports to see which targets were used:

.. code-block:: bash

   streamflow report workflow-name --format json | \
     jq '.steps[] | {step: .name, location: .location}'

Debug Target Selection
----------------------

Enable debug logging to see selection decisions:

.. code-block:: bash

   streamflow run streamflow.yml --debug

Logs show:

* Target evaluation order
* Why targets were selected/rejected
* Load balancing decisions

Best Practices
==============

1. **Use Shuffle for Equivalent Targets**
   
   .. code-block:: yaml
   
      # Good for load balancing
      target:
        - deployment: node-1
        - deployment: node-2
        - deployment: node-3
      filters:
        - shuffle

2. **Order by Preference Without Shuffle**
   
   .. code-block:: yaml
   
      # Prefer fast-cluster, fall back to others
      target:
        - deployment: fast-cluster
        - deployment: medium-cluster
        - deployment: slow-cluster

3. **Match Resources to Tasks**
   
   Don't bind lightweight tasks to expensive GPU clusters.

4. **Consider Data Locality**
   
   Prefer targets where data already resides:
   
   .. code-block:: yaml
   
      bindings:
        - step: /process
          target:
            - deployment: hpc-with-data
            - deployment: cloud-cluster
        - port: /input_data
          target:
            deployment: hpc-with-data
            workdir: /data

5. **Test Failover Behavior**
   
   Verify workflow continues if a target becomes unavailable.

6. **Monitor Resource Utilization**
   
   Use reports to verify load is distributed as expected.

Limitations
===========

**No User-Defined Selection Logic:**

StreamFlow doesn't support custom selection logic in configuration. Use binding filter plugins for advanced selection.

**All Targets Must Support Step:**

All targets must have required tools and environment for the step.

**No Dynamic Target Addition:**

Target list is fixed at workflow start. Cannot add targets during execution.

Troubleshooting
===============

All Targets Unavailable
-----------------------

**Problem:** ``No available targets for step``

**Solution:**

* Check deployment connectivity
* Verify resource availability
* Review logs for specific failures
* Add more targets

Uneven Load Distribution
------------------------

**Problem:** Some targets overloaded, others idle

**Solution:**

* Add shuffle filter
* Check target capabilities
* Review scheduling logs
* Verify targets have equal capacity

Tasks Only Use First Target
----------------------------

**Problem:** All tasks scheduled to first target

**Cause:** First target has excess capacity

**Solution:**

* Add shuffle filter for random distribution
* Reduce target capacity to force spillover
* Use custom binding filter

Next Steps
==========

After mastering multiple targets:

* :doc:`port-targets` - Advanced port binding patterns
* :doc:`stacked-locations` - Complex deployment stacking
* :doc:`/developer-guide/extension-points/binding-filter` - Create custom filters
* :doc:`/reference/configuration/binding-config` - Complete binding reference

Related Topics
==============

* :doc:`/user-guide/binding-workflows` - Basic binding concepts
* :doc:`/user-guide/configuring-deployments` - Deployment configuration
* :doc:`/developer-guide/core-interfaces/scheduling` - Scheduling internals
