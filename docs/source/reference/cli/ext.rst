===============
streamflow ext
===============

.. meta::
   :keywords: StreamFlow, CLI, extensions, connectors
   :description: List StreamFlow extensions

Synopsis
========

.. code-block:: bash

   streamflow ext {list,show} [OPTIONS]

Description
===========

List and inspect available StreamFlow extensions including built-in connectors, schedulers, and other extension points.

Subcommands
===========

``list``
   List all available StreamFlow extensions.
   
   .. code-block:: bash
   
      streamflow ext list

``show``
   Show details of a specific StreamFlow extension.
   
   .. code-block:: bash
   
      streamflow ext show EXTENSION_NAME

Examples
========

**List All Extensions:**

.. code-block:: bash

   streamflow ext list

**Show Extension Details:**

.. code-block:: bash

   streamflow ext show docker

Output
======

Lists available extension types:

* **Connectors:** Docker, Kubernetes, Slurm, SSH, etc.
* **Schedulers:** Data locality scheduler, etc.
* **Binding Filters:** Shuffle, match filters
* **Other Extensions:** Custom plugins

Related Commands
================

* :doc:`plugin` - Manage installed plugins

Related Documentation
=====================

**Reference:**
   - :doc:`/reference/connectors/index` - Connector documentation

**Developer Guide:**
   - :doc:`/developer-guide/extension-points/index` - Extension points

See Also
========

* :doc:`plugin` - For plugin management
