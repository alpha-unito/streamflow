================
streamflow list
================

.. meta::
   :keywords: StreamFlow, CLI, list, workflows
   :description: List executed StreamFlow workflows

Synopsis
========

.. code-block:: bash

   streamflow list [OPTIONS] [NAME]

Description
===========

List all executed workflows stored in the StreamFlow database. Can list all workflows or filter by workflow name.

Arguments
=========

``NAME``
   List all executions for the given workflow name.
   
   **Optional:** If omitted, lists all workflows

Options
=======

``-h, --help``
   Show help message and exit.

``--file FILE, -f FILE``
   Path to the StreamFlow configuration file. Uses the database configured in that file.
   
   **Default:** Uses default database location

Examples
========

**List All Workflows:**

.. code-block:: bash

   streamflow list

**List Executions for Specific Workflow:**

.. code-block:: bash

   streamflow list my-workflow

**Use Custom Configuration File:**

.. code-block:: bash

   streamflow list --file streamflow.yml my-workflow

Output Format
=============

The command outputs a table with the following columns:

* **Name:** Workflow name
* **Status:** Execution status (completed, failed, running)
* **Start Time:** When execution started
* **End Time:** When execution finished
* **Duration:** Total execution time

Related Commands
================

* :doc:`run` - Execute a workflow
* :doc:`report` - Generate detailed execution report

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/inspecting-results` - Inspecting workflow results
   - :doc:`/user-guide/running-workflows` - Running workflows

See Also
========

* :doc:`report` - For detailed reports
* :doc:`prov` - For provenance data
