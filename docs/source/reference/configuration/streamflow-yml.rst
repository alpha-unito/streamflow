===================
The StreamFlow File
===================

.. meta::
   :keywords: StreamFlow, configuration, YAML, streamflow.yml
   :description: Main StreamFlow configuration file reference

Overview
========

The ``streamflow.yml`` file is the main configuration file for StreamFlow. It defines workflows, deployments, and bindings in a single YAML document.

File Structure
==============

.. code-block:: yaml

   version: v1.0
   
   workflows:
     # Workflow definitions
   
   deployments:
     # Deployment configurations
   
   bindings:
     # Step-to-deployment bindings
   
   filters:
     # Binding filters (optional)

Top-Level Fields
================

``version``
   StreamFlow configuration schema version.
   
   **Type:** String  
   **Required:** Yes  
   **Values:** ``v1.0``

``workflows``
   Map of workflow definitions. Keys are workflow names, values are workflow configurations.
   
   **Type:** Object  
   **Required:** Yes

``deployments``
   Map of deployment configurations. Keys are deployment names, values are deployment configurations.
   
   **Type:** Object  
   **Required:** No

``bindings``
   List of bindings associating workflow steps with deployments.
   
   **Type:** Array  
   **Required:** No

``filters``
   Map of binding filter configurations.
   
   **Type:** Object  
   **Required:** No

Workflow Configuration
======================

Each workflow entry has the following structure:

.. code-block:: yaml

   workflows:
     my-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: input.yml

**Fields:**

``type``
   Workflow type. Currently only ``cwl`` is supported.
   
   **Type:** String  
   **Required:** Yes  
   **Values:** ``cwl``

``config``
   Workflow-specific configuration.
   
   **Type:** Object  
   **Required:** Yes

``config.file``
   Path to CWL workflow file.
   
   **Type:** String  
   **Required:** Yes

``config.settings``
   Path to workflow inputs file (YAML or JSON).
   
   **Type:** String  
   **Required:** Yes

Deployment Configuration
========================

Each deployment entry has the following structure:

.. code-block:: yaml

   deployments:
     my-deployment:
       type: docker
       config:
         image: python:3.10

**Fields:**

``type``
   Deployment connector type.
   
   **Type:** String  
   **Required:** Yes  
   **Values:** ``local``, ``docker``, ``kubernetes``, ``ssh``, ``slurm``, ``pbs``, ``singularity``, etc.

``config``
   Connector-specific configuration.
   
   **Type:** Object  
   **Required:** Depends on connector

For connector-specific configuration options, see:

* :doc:`/reference/connectors/docker` - Docker configuration
* :doc:`/reference/connectors/kubernetes` - Kubernetes configuration
* :doc:`/reference/connectors/slurm` - Slurm configuration
* :doc:`/reference/connectors/ssh` - SSH configuration
* :doc:`/reference/connectors/index` - All connectors

Binding Configuration
=====================

Each binding associates a workflow step with a deployment:

.. code-block:: yaml

   bindings:
     - step: /my-step
       target:
         deployment: my-deployment

**Fields:**

``step``
   CWL step name (with leading ``/``).
   
   **Type:** String  
   **Required:** Yes

``target``
   Target deployment specification.
   
   **Type:** Object  
   **Required:** Yes

``target.deployment``
   Name of the deployment to use.
   
   **Type:** String  
   **Required:** Yes

For advanced binding patterns, see:

* :doc:`/user-guide/binding-workflows` - Binding workflows guide
* :doc:`/user-guide/advanced-patterns/index` - Advanced patterns

Filter Configuration
====================

Filters customize binding behavior:

.. code-block:: yaml

   filters:
     my-filter:
       type: shuffle

**Built-in filter types:**

* ``shuffle`` - Randomize target selection
* ``match`` - Pattern-based target matching

Complete Example
================

.. code-block:: yaml

   version: v1.0
   
   workflows:
     example-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
   
   deployments:
     local-env:
       type: local
     
     docker-env:
       type: docker
       config:
         image: python:3.10
     
     hpc-cluster:
       type: slurm
       config:
         hostname: hpc.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa
   
   bindings:
     - step: /preprocess
       target:
         deployment: docker-env
     
     - step: /compute
       target:
         deployment: hpc-cluster
     
     - step: /postprocess
       target:
         deployment: docker-env

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/configuring-deployments` - Deployment configuration
   - :doc:`/user-guide/binding-workflows` - Binding configuration

**Configuration:**
   - :doc:`workflow-config` - Workflow configuration details
   - :doc:`deployment-config` - Deployment configuration details
   - :doc:`binding-config` - Binding configuration details

**CLI:**
   - :doc:`/reference/cli/schema` - Dump JSON schema

See Also
========

* Use ``streamflow schema`` to view the complete JSON schema
* See :doc:`/reference/connectors/index` for connector-specific configuration
