==================
streamflow plugin
==================

.. meta::
   :keywords: StreamFlow, CLI, plugin, extensions
   :description: Manage StreamFlow plugins

Synopsis
========

.. code-block:: bash

   streamflow plugin {list,show} [OPTIONS]

Description
===========

Manage installed StreamFlow plugins. View available plugins and their details.

Subcommands
===========

``list``
   List all installed StreamFlow plugins.
   
   .. code-block:: bash
   
      streamflow plugin list

``show``
   Show details of a specific StreamFlow plugin.
   
   .. code-block:: bash
   
      streamflow plugin show PLUGIN_NAME

Examples
========

**List All Plugins:**

.. code-block:: bash

   streamflow plugin list

**Show Plugin Details:**

.. code-block:: bash

   streamflow plugin show my-connector-plugin

Related Commands
================

* :doc:`ext` - List available extensions

Related Documentation
=====================

**Developer Guide:**
   - :doc:`/developer-guide/extension-points/index` - Creating plugins

See Also
========

* :doc:`ext` - For extension management
