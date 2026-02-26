==============
streamflow run
==============

.. meta::
   :keywords: StreamFlow, CLI, run, execute workflow
   :description: Execute a StreamFlow workflow

Synopsis
========

.. code-block:: bash

   streamflow run [OPTIONS] STREAMFLOW_FILE

Description
===========

Execute a workflow defined in a StreamFlow configuration file. This is the primary command for running workflows.

Arguments
=========

``STREAMFLOW_FILE``
   Path to the StreamFlow configuration file (typically ``streamflow.yml``) describing the workflow execution.
   
   **Required:** Yes

Options
=======

``-h, --help``
   Show help message and exit.

``--color``
   Print log output with colors related to the logging level.
   
   **Default:** Disabled

``--debug``
   Print debug-level diagnostic output. Useful for troubleshooting workflow execution issues.
   
   **Default:** Disabled

``--name [NAME]``
   Name of the current workflow. Used for search and indexing in the database.
   
   **Default:** Derived from workflow file name

``--outdir OUTDIR``
   Output directory to store final results of the workflow.
   
   **Default:** Current directory

``--quiet``
   Only print results, warnings, and errors. Suppresses informational messages.
   
   **Default:** Disabled

Examples
========

**Basic Execution:**

.. code-block:: bash

   streamflow run streamflow.yml

**With Debug Output:**

.. code-block:: bash

   streamflow run --debug streamflow.yml

**Custom Output Directory:**

.. code-block:: bash

   streamflow run --outdir /path/to/results streamflow.yml

**Named Workflow:**

.. code-block:: bash

   streamflow run --name my-workflow streamflow.yml

**Quiet Mode:**

.. code-block:: bash

   streamflow run --quiet streamflow.yml

Exit Codes
==========

====  ================================
Code  Meaning
====  ================================
0     Workflow completed successfully
1     General error
2     Configuration error
3     Workflow execution error
====  ================================

Related Commands
================

* :doc:`list` - List executed workflows
* :doc:`report` - Generate execution report
* :doc:`prov` - Export provenance data

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/running-workflows` - Running workflows tutorial
   - :doc:`/user-guide/troubleshooting` - Troubleshooting guide

**Configuration:**
   - :doc:`/reference/configuration/streamflow-yml` - Configuration file reference

See Also
========

* ``streamflow --help`` - Show general help
* ``cwl-runner`` - CWL standard runner interface
