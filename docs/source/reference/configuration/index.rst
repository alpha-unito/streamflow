=============
Configuration
=============

.. meta::
   :keywords: StreamFlow, configuration, schema, YAML, streamflow.yml
   :description: Complete StreamFlow configuration reference with schemas and examples

Overview
========

StreamFlow uses YAML configuration files to define workflows, deployments, and bindings. This section provides complete reference documentation for all configuration options, generated from JSON schemas.

Quick Reference
===============

============  ====================================
Purpose       Configuration file reference
Audience      All users
Format        YAML (validated against JSON Schema)
Main File     ``streamflow.yml``
============  ====================================

Configuration Documentation
===========================

.. toctree::
   :maxdepth: 2
   :titlesonly:

   streamflow-yml
   workflow-config
   deployment-config
   binding-config
   environment-variables

Configuration Structure
=======================

The main ``streamflow.yml`` file has this structure:

.. code-block:: yaml

   version: v1.0
   
   workflows:
     # Workflow configurations
   
   deployments:
     # Deployment configurations
   
   bindings:
     # Step-to-deployment bindings
   
   filters:
     # Binding filters (optional)

See :doc:`streamflow-yml` for complete documentation.

Configuration by Topic
======================

**Workflows**
   Define CWL workflows and their execution parameters.
   
   :doc:`workflow-config`

**Deployments**
   Configure execution environments (Docker, Kubernetes, HPC, etc.).
   
   :doc:`deployment-config`

**Bindings**
   Associate workflow steps with deployments.
   
   :doc:`binding-config`

**Environment Variables**
   Configure StreamFlow behavior via environment variables.
   
   :doc:`environment-variables`

Schema Validation
=================

Validate your configuration:

.. code-block:: bash

   # Dump the JSON schema
   streamflow schema
   
   # Validate during run
   streamflow run streamflow.yml

Invalid configurations will produce detailed error messages indicating the problem location.

Configuration Examples
======================

**Minimal Configuration:**

.. code-block:: yaml

   version: v1.0
   
   workflows:
     my-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: {}

**With Deployment:**

.. code-block:: yaml

   version: v1.0
   
   workflows:
     my-workflow:
       type: cwl
       config:
         file: workflow.cwl
   
   deployments:
     docker-env:
       type: docker
       config:
         image: alpine:latest
   
   bindings:
     - step: /my-step
       target:
         deployment: docker-env

Connector-Specific Configuration
=================================

Each connector type has its own configuration schema:

* :doc:`/reference/connectors/docker` - Docker configuration
* :doc:`/reference/connectors/kubernetes` - Kubernetes configuration
* :doc:`/reference/connectors/slurm` - Slurm configuration
* :doc:`/reference/index` - All connectors

Related Documentation
=====================

**User Guide:**
   For configuration tutorials:
   
   - :doc:`/user-guide/configuring-deployments` - Deployment setup
   - :doc:`/user-guide/binding-workflows` - Binding configuration

**Reference:**
   For connector details:
   
   - :doc:`/reference/index` - Available connectors

Best Practices
==============

**Use Version Control:**
   Store ``streamflow.yml`` in version control with your workflows.

**Validate Early:**
   Run ``streamflow run`` with ``--dry-run`` to validate before execution.

**Use Comments:**
   YAML supports comments - document complex configurations.

**External Files:**
   Reference external CWL and deployment files for better organization.

Next Steps
==========

* :doc:`streamflow-yml` - Complete configuration file reference
* :doc:`deployment-config` - Deployment configuration details
* :doc:`binding-config` - Binding configuration details
