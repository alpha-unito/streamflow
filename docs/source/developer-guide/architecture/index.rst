============
Architecture
============

.. meta::
   :keywords: StreamFlow, architecture, design, execution model, modules
   :description: StreamFlow architectural design, execution model, and module structure

Overview
========

This section provides a comprehensive overview of StreamFlow's architecture, from high-level design principles to detailed execution flows and module organization.

Quick Reference
===============

============  ====================================
Audience      Developers and architects
Purpose       Understand StreamFlow design
Time          2-3 hours
Difficulty    Intermediate
============  ====================================

Table of Contents
=================

.. toctree::
   :maxdepth: 2
   :titlesonly:

   overview
   execution-model
   data-management
   module-structure
   plugin-architecture

Architecture Topics
===================

**Overview**
   High-level architectural design and the three-layer environment stack (deployment → service → location).
   
   :doc:`overview`

**Execution Model**
   Detailed workflow execution flow from CWL parsing through task scheduling and remote execution.
   
   :doc:`execution-model`

**Data Management**
   How StreamFlow tracks data locations, optimizes transfers, and manages remote paths.
   
   :doc:`data-management`

**Module Structure**
   Python package organization and the role of each module in the codebase.
   
   :doc:`module-structure`

**Plugin Architecture**
   How the plugin system works, including discovery, loading, and extension point registration.
   
   :doc:`plugin-architecture`

Key Architectural Principles
=============================

**Container-Native Design**
   StreamFlow is built from the ground up to support multi-container environments and service-oriented architectures.

**Hybrid Infrastructure Support**
   Relaxes the shared data space requirement to enable execution across heterogeneous cloud and HPC resources.

**CWL Standard Compliance**
   Implements the Common Workflow Language standard (v1.0-v1.2) for workflow portability.

**Extensibility via Plugins**
   Core functionality can be extended through a well-defined plugin system with multiple extension points.

Related Documentation
=====================

**User Guide:**
   For using StreamFlow:
   
   - :doc:`/user-guide/quickstart` - Get started quickly
   - :doc:`/user-guide/writing-workflows` - Writing CWL workflows

**Core Interfaces:**
   For detailed interface documentation:
   
   - :doc:`/developer-guide/core-interfaces/index` - Core abstractions

**Extension Points:**
   For creating plugins:
   
   - :doc:`/developer-guide/extension-points/index` - Plugin development

Next Steps
==========

Start with the overview and progress through the execution model:

1. :doc:`overview` - Understand the high-level design
2. :doc:`execution-model` - Learn how workflows execute
3. :doc:`data-management` - Understand data handling
4. :doc:`module-structure` - Explore the codebase organization
5. :doc:`plugin-architecture` - Learn the extension system
