===============
Developer Guide
===============

.. meta::
   :keywords: StreamFlow, developer, architecture, plugin, extension, API
   :description: Comprehensive guide for StreamFlow developers: architecture, core interfaces, and plugin development

Overview
========

The Developer Guide provides in-depth information about StreamFlow's architecture, core abstractions, and extension mechanisms. This guide is essential for developers who want to understand StreamFlow's internals, contribute to the codebase, or create custom extensions.

Quick Reference
===============

==============  ====================================
Audience        Developers and contributors
Purpose         Understand and extend StreamFlow
Time            4-6 hours for complete guide
Difficulty      Intermediate to Advanced
Prerequisites   Python 3.10+, async programming
==============  ====================================

Who Should Read This
====================

This guide is for:

* **Core Contributors:** Developers contributing to StreamFlow codebase
* **Plugin Developers:** Creating custom connectors, schedulers, or other extensions
* **Architecture Enthusiasts:** Understanding StreamFlow's design and implementation
* **System Developers:** Integrating StreamFlow into larger systems

Table of Contents
=================

Architecture
------------

Understand StreamFlow's design, execution model, and module structure:

.. toctree::
   :maxdepth: 2
   :titlesonly:

   architecture/index

Development Setup
-----------------

Set up your development environment and learn the contribution workflow:

.. toctree::
   :maxdepth: 2
   :titlesonly:

   development-setup
   testing-guide
   code-style
   contributing

Core Interfaces
---------------

Deep dive into StreamFlow's core abstractions for plugin development:

.. toctree::
   :maxdepth: 2
   :titlesonly:

   core-interfaces/index

Extension Points
----------------

Learn how to create custom plugins and extend StreamFlow:

.. toctree::
   :maxdepth: 2
   :titlesonly:

   extension-points/index

Quick Navigation
================

**New to StreamFlow Development?**
   Start with :doc:`architecture/overview` to understand the system design.

**Want to Create a Plugin?**
   Go to :doc:`extension-points/creating-plugins` for a step-by-step tutorial.

**Contributing Code?**
   Read :doc:`development-setup` and :doc:`contributing` for guidelines.

**Need API Reference?**
   See :doc:`/reference/api/index` for complete API documentation.

Key Architecture Concepts
=========================

**StreamFlowContext**
   The central coordinator managing all StreamFlow components and their lifecycle.

**Connector**
   Interface for interacting with execution environments (Docker, Kubernetes, HPC, etc.).

**Scheduler**
   Component responsible for assigning workflow tasks to available resources.

**DataManager**
   Handles data transfer and location tracking across different execution environments.

**Database**
   Persistence layer for workflow metadata, provenance, and checkpointing.

Related Documentation
=====================

**User Guide:**
   For using StreamFlow (not developing it):
   
   - :doc:`/user-guide/quickstart` - Get started in 10 minutes
   - :doc:`/user-guide/installation` - Installation instructions
   - :doc:`/user-guide/writing-workflows` - Writing CWL workflows

**Reference:**
   For detailed API and configuration reference:
   
   - :doc:`/reference/api/index` - Auto-generated API documentation
   - :doc:`/reference/configuration/index` - Configuration schemas

Contributing to StreamFlow
===========================

We welcome contributions! Please see:

* :doc:`contributing` - Contribution guidelines
* :doc:`code-style` - Coding standards and best practices
* :doc:`testing-guide` - How to write and run tests
* `GitHub Repository <https://github.com/alpha-unito/streamflow>`_ - Source code

Next Steps
==========

Choose your path:

* :doc:`architecture/overview` - Understand the architecture
* :doc:`development-setup` - Set up your environment
* :doc:`extension-points/creating-plugins` - Build your first plugin
* :doc:`core-interfaces/index` - Learn the core abstractions
