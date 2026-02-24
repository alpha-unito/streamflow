=========
Reference
=========

.. meta::
   :keywords: StreamFlow, reference, API, CLI, configuration, schema
   :description: Complete reference documentation for StreamFlow: CLI commands, configuration schemas, connector reference, and API documentation

Overview
========

The Reference section provides comprehensive technical documentation for StreamFlow. This includes command-line interface details, configuration schemas, connector specifications, and complete API documentation.

Quick Reference
===============

============  ====================================
Audience      All users (lookup reference)
Purpose       Detailed technical specifications
Organization  By component type
Updates       Generated from source code/schemas
============  ====================================

Using This Reference
====================

This reference is organized by component type:

**Need to run a command?**
   See :doc:`cli/index` for complete command-line reference

**Configuring StreamFlow?**
   See :doc:`configuration/index` for all configuration options

**Choosing a connector?**
   Browse the connector pages below for available execution environments

**Using CWL features?**
   See :doc:`cwl-support/index` for CWL-specific documentation

**Programming with StreamFlow?**
   See :doc:`api/index` for complete API documentation

Table of Contents
=================

.. toctree::
   :maxdepth: 2
   :titlesonly:

   cli/index
   configuration/index
   connectors/index
   cwl-docker-translators/index
   glossary

Quick Links
===========

**Most Common References:**

* :doc:`cli/run` - Run workflows
* :doc:`configuration/streamflow-yml` - The StreamFlow file
* :doc:`connectors/docker` - Docker connector
* :doc:`connectors/slurm` - Slurm connector
* :doc:`glossary` - Term definitions

**Connector Reference:**

* **Container:** :doc:`connectors/docker`, :doc:`connectors/singularity`
* **Cloud:** :doc:`connectors/kubernetes`
* **HPC:** :doc:`connectors/ssh`, :doc:`connectors/slurm`, :doc:`connectors/pbs`

**CWL-Specific:**

* :doc:`cwl-docker-translators/index` - Docker requirement handling
* :doc:`cwl-docker-translators/docker` - Docker translator
* :doc:`cwl-docker-translators/kubernetes` - Kubernetes translator
* :doc:`cwl-docker-translators/singularity` - Singularity translator

Related Documentation
=====================

**User Guide:**
   For tutorials and usage examples:
   
   - :doc:`/user-guide/quickstart` - Get started in 10 minutes
   - :doc:`/user-guide/installation` - Installation instructions
   - :doc:`/user-guide/writing-workflows` - Writing CWL workflows

**Developer Guide:**
   For extending StreamFlow:
   
   - :doc:`/developer-guide/extension-points/index` - Creating plugins

Finding Information
===================

**By Task:**

* **Installing:** :doc:`/user-guide/installation`
* **Running workflows:** :doc:`cli/index`
* **Configuring deployments:** :doc:`configuration/index`
* **Inspecting results:** :doc:`cli/index`

**By Connector Type:**

* **Container:** :doc:`connectors/docker`, :doc:`connectors/singularity`
* **Cloud:** :doc:`connectors/kubernetes`
* **HPC:** :doc:`connectors/ssh`, :doc:`connectors/slurm`, :doc:`connectors/pbs`
* **Schedulers:** :doc:`/developer-guide/extension-points/scheduler`
* **Data Managers:** :doc:`/developer-guide/extension-points/data-manager`
* **Databases:** :doc:`/developer-guide/extension-points/database`

**By Format:**

* **Configuration Schemas:** :doc:`configuration/index`
* **Python API:** :doc:`api/index`
* **CLI Commands:** :doc:`cli/index`

Conventions Used
================

**Configuration Examples:**

All configuration examples use YAML format for streamflow.yml files and follow the schema defined in :doc:`configuration/streamflow-yml`.

**Command Examples:**

Command-line examples show the prompt with ``$`` and use ``streamflow`` as the command name. Commands are assumed to run from the project directory.

**Code Examples:**

Python code examples use type hints and follow StreamFlow's coding standards documented in :doc:`/developer-guide/code-style`.

Need Help?
==========

* **Can't find something?** Check :doc:`glossary` for terminology
* **Configuration not working?** Validate with ``streamflow schema``
* **CLI questions?** Run ``streamflow --help`` or ``streamflow <command> --help``
* **API questions?** See :doc:`api/index` for complete documentation
