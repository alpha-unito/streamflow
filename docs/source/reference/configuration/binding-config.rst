====================
Binding Configuration
====================

.. meta::
   :keywords: StreamFlow, binding, configuration, step mapping
   :description: Binding configuration reference for StreamFlow

Overview
========

Binding configuration associates workflow steps with deployment targets, specifying where each step executes.

Configuration Structure
=======================

.. code-block:: yaml

   bindings:
     - step: /step-name
       target:
         deployment: deployment-name
         # Additional target options

Fields Reference
================

``step``
   CWL step name to bind.
   
   **Type:** String  
   **Required:** Yes  
   **Format:** Must start with ``/``, use ``/*`` for all steps

``target``
   Target deployment specification.
   
   **Type:** Object or Array  
   **Required:** Yes

``target.deployment``
   Name of the deployment defined in the ``deployments`` section.
   
   **Type:** String  
   **Required:** Yes

``target.service``
   Specific service within the deployment (for multi-service deployments).
   
   **Type:** String  
   **Required:** No

``target.locations``
   Number of parallel locations to create.
   
   **Type:** Integer  
   **Required:** No  
   **Default:** 1

Examples
========

**Basic Binding:**

.. code-block:: yaml

   bindings:
     - step: /my-step
       target:
         deployment: docker-env

**Bind All Steps:**

.. code-block:: yaml

   bindings:
     - step: /*
       target:
         deployment: docker-env

**Multiple Bindings:**

.. code-block:: yaml

   bindings:
     - step: /preprocess
       target:
         deployment: local-env
     
     - step: /compute
       target:
         deployment: hpc-cluster
     
     - step: /postprocess
       target:
         deployment: docker-env

**Parallel Locations:**

.. code-block:: yaml

   bindings:
     - step: /parallel-task
       target:
         deployment: docker-env
         locations: 10

**Multiple Targets:**

.. code-block:: yaml

   bindings:
     - step: /distributed-task
       target:
         - deployment: cloud-env-1
         - deployment: cloud-env-2
         - deployment: hpc-cluster

**Port-Specific Targets:**

.. code-block:: yaml

   bindings:
     - step: /data-processing
       target:
         deployment: compute-env
     - step: /data-processing/input-data
       target:
         deployment: storage-env

Advanced Patterns
=================

For advanced binding patterns, see:

* :doc:`/user-guide/advanced-patterns/multiple-targets` - Multiple deployment targets
* :doc:`/user-guide/advanced-patterns/port-targets` - Port-specific bindings
* :doc:`/user-guide/advanced-patterns/stacked-locations` - Nested deployments

Binding Filters
===============

Filters can customize binding behavior:

.. code-block:: yaml

   bindings:
     - step: /my-step
       target:
         - deployment: env-1
         - deployment: env-2
       filters:
         - my-filter

   filters:
     my-filter:
       type: shuffle

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/binding-workflows` - Binding guide
   - :doc:`/user-guide/advanced-patterns/index` - Advanced patterns

**Configuration:**
   - :doc:`streamflow-yml` - Main configuration file
   - :doc:`deployment-config` - Deployment configuration

See Also
========

* :doc:`/user-guide/binding-workflows` - Complete binding guide
* :doc:`/user-guide/advanced-patterns/index` - Advanced binding patterns
