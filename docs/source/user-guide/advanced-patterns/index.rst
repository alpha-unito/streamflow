================
Advanced Patterns
================

.. meta::
   :keywords: StreamFlow, advanced, patterns, multiple targets, port targets, stacked locations
   :description: Advanced StreamFlow configuration patterns for complex workflow scenarios

Overview
========

This section covers advanced StreamFlow binding patterns that enable sophisticated workflow configurations. These patterns are useful for complex scenarios requiring fine-grained control over task placement and data locality.

Patterns Covered
================

.. toctree::
   :maxdepth: 2
   :titlesonly:

   multiple-targets
   port-targets
   stacked-locations

When to Use Advanced Patterns
==============================

**Multiple Targets:**
   Use when a workflow step should execute on different deployment targets based on runtime conditions or data characteristics.
   
   *Example:* Process different data subsets on different clouds, or route tasks based on size/type.

**Port Targets:**
   Use when input/output data resides on specific deployments separate from where computation occurs.
   
   *Example:* Data stored in cloud storage while computation happens on HPC, or distributed data sources.

**Stacked Locations:**
   Use when you need to wrap one deployment inside another, creating layered execution environments.
   
   *Example:* Run Singularity containers inside Slurm jobs accessed via SSH, or Docker-in-Docker scenarios.

Pattern Selection Guide
=======================

============================================  ====================================
Scenario                                      Pattern to Use
============================================  ====================================
Different clouds for different tasks          :doc:`multiple-targets`
Load balancing across resources               :doc:`multiple-targets`
Data on storage, compute elsewhere            :doc:`port-targets`
Optimize data transfer costs                  :doc:`port-targets`
Container in batch scheduler in remote host   :doc:`stacked-locations`
Complex multi-layer environments              :doc:`stacked-locations`
============================================  ====================================

Prerequisites
=============

Before exploring advanced patterns, ensure you understand:

* :doc:`../binding-workflows` - Basic binding concepts
* :doc:`../configuring-deployments` - Deployment configuration
* :doc:`/reference/configuration/binding-config` - Binding schema reference

Related Topics
==============

**User Guide:**
   - :doc:`../binding-workflows` - Basic binding patterns
   - :doc:`../configuring-deployments` - Deployment setup

**Reference:**
   - :doc:`/reference/configuration/binding-config` - Complete binding schema
   - :doc:`/developer-guide/extension-points/binding-filter` - Custom filters

**Examples:**
   Working examples for each pattern are available in the ``docs/examples/advanced/`` directory.

Next Steps
==========

Choose the pattern that matches your use case:

* :doc:`multiple-targets` - Multiple deployment targets per step
* :doc:`port-targets` - Data placement separate from computation
* :doc:`stacked-locations` - Nested deployment environments
