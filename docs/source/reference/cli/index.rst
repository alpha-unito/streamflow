======================
Command-Line Interface
======================

.. meta::
   :keywords: StreamFlow, CLI, command line, streamflow run, commands
   :description: Complete StreamFlow command-line interface reference

Overview
========

StreamFlow provides a comprehensive command-line interface (CLI) for executing workflows, inspecting results, and managing plugins. This section documents all available commands and their options.

Quick Reference
===============

============  ====================================
Purpose       CLI command reference
Audience      All users
Organization  By command
Usage         Lookup command details
============  ====================================

Available Commands
==================

.. toctree::
   :maxdepth: 2
   :titlesonly:

   run
   list
   report
   prov
   plugin
   ext
   schema
   cwl-runner

Command Summary
===============

Core Commands
-------------

=====================  ===========================================
Command                Purpose
=====================  ===========================================
``streamflow run``     Execute a workflow
``streamflow list``    List executed workflows
``streamflow report``  Generate execution report
``streamflow prov``    Export provenance archive
=====================  ===========================================

Utility Commands
----------------

========================  ===========================================
Command                   Purpose
========================  ===========================================
``streamflow plugin``     Manage installed plugins
``streamflow ext``        List available extensions
``streamflow schema``     Dump configuration schema
``streamflow version``    Show StreamFlow version
========================  ===========================================

Alternative Interfaces
----------------------

=====================  ===========================================
Command                Purpose
=====================  ===========================================
``cwl-runner``         CWL standard runner interface
=====================  ===========================================

Getting Help
============

For any command, use ``--help`` to see detailed usage:

.. code-block:: bash

   streamflow --help
   streamflow run --help
   streamflow list --help

Common Options
==============

Many commands share these common options:

``--log-level``
   Set logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL)

``--config``
   Path to StreamFlow configuration file (default: ``streamflow.yml``)

Exit Codes
==========

StreamFlow uses standard exit codes:

====  ===========================================
Code  Meaning
====  ===========================================
0     Success
1     General error
2     Configuration error
3     Workflow execution error
====  ===========================================

Related Documentation
=====================

**User Guide:**
   For tutorials on using commands:
   
   - :doc:`/user-guide/running-workflows` - Running workflows
   - :doc:`/user-guide/inspecting-results` - Inspection commands

**Configuration:**
   For configuration file reference:
   
   - :doc:`/reference/configuration/streamflow-yml` - Main config file

Examples
========

**Run a workflow:**

.. code-block:: bash

   streamflow run streamflow.yml

**List all workflows:**

.. code-block:: bash

   streamflow list

**Generate report:**

.. code-block:: bash

   streamflow report <workflow-name>

**Debug mode:**

.. code-block:: bash

   streamflow run --log-level DEBUG streamflow.yml

Next Steps
==========

Explore command documentation:

* :doc:`run` - Most commonly used command
* :doc:`list` - Inspect workflow history
* :doc:`report` - Generate execution reports
