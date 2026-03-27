===============
Core Interfaces
===============

.. meta::
   :keywords: StreamFlow, core, interfaces, API, abstractions
   :description: StreamFlow core interface documentation for plugin developers

Overview
========

This section documents StreamFlow's core interfaces defined in the ``streamflow.core`` module. These abstractions form the foundation for all StreamFlow functionality and are essential for plugin development.

Quick Reference
===============

==============  ====================================
Audience        Plugin developers
Purpose         Understand core abstractions
Difficulty      Intermediate to Advanced
Prerequisites   Python async programming
==============  ====================================

Core Interfaces
===============

.. toctree::
   :maxdepth: 2
   :titlesonly:

   streamflow-context
   workflow
   deployment
   data
   persistence
   scheduling
   recovery

Interface Descriptions
======================

**StreamFlowContext**
   The central coordinator managing all StreamFlow components and their lifecycle. Entry point for accessing managers and services.
   
   :doc:`streamflow-context`

**Workflow Interfaces**
   Workflow, Step, Port, and Token abstractions for representing computational workflows.
   
   :doc:`workflow`

**Deployment Interfaces**
   Connector, Target, Service, and Location for interacting with execution environments.
   
   :doc:`deployment`

**Data Interfaces**
   DataManager and DataLocation for tracking and transferring data across environments.
   
   :doc:`data`

**Persistence Interfaces**
   Database abstraction for storing workflow metadata and provenance.
   
   :doc:`persistence`

**Scheduling Interfaces**
   Scheduler and Policy for resource allocation and task assignment.
   
   :doc:`scheduling`

**Recovery Interfaces**
   CheckpointManager and FailureManager for fault tolerance.
   
   :doc:`recovery`

Using Core Interfaces
======================

**For Plugin Development:**

When creating plugins, you'll implement one or more of these interfaces:

* **Connector plugin:** Implement ``Connector`` interface from :doc:`deployment`
* **Scheduler plugin:** Implement ``Scheduler`` and ``Policy`` from :doc:`scheduling`
* **Database plugin:** Implement ``Database`` from :doc:`persistence`
* **DataManager plugin:** Implement ``DataManager`` from :doc:`data`

**For Core Development:**

When contributing to StreamFlow core, understand these interfaces to maintain consistency and avoid breaking changes.

Interface Contracts
===================

All core interfaces define contracts that implementations must honor:

* **Async Methods:** Most methods are async and must be awaited
* **Type Hints:** All methods have complete type annotations
* **Error Handling:** Raise appropriate exceptions from ``streamflow.core.exception``
* **Context Management:** Use StreamFlowContext for accessing other components

Related Documentation
=====================

**Extension Points:**
   For step-by-step plugin development:
   
   - :doc:`/developer-guide/extension-points/creating-plugins` - Plugin tutorial
   - :doc:`/developer-guide/extension-points/index` - All extension points

**Architecture:**
   For understanding how interfaces fit together:
   
   - :doc:`/developer-guide/architecture/overview` - System architecture
   - :doc:`/developer-guide/architecture/module-structure` - Code organization

**API Reference:**
   For complete API documentation:
   
   - :doc:`/reference/api/index` - Auto-generated API docs

Next Steps
==========

Start with the context, then explore interfaces relevant to your needs:

1. :doc:`streamflow-context` - Central coordinator (start here)
2. :doc:`deployment` - If creating connectors
3. :doc:`scheduling` - If creating schedulers
4. :doc:`data` - If working with data management
5. :doc:`workflow` - If extending workflow capabilities
