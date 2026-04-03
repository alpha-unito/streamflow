==================
streamflow report
==================

.. meta::
   :keywords: StreamFlow, CLI, report, execution report
   :description: Generate execution reports for StreamFlow workflows

Synopsis
========

.. code-block:: bash

   streamflow report [OPTIONS] WORKFLOWS

Description
===========

Generate execution reports for one or more workflows. Reports can be generated in multiple formats including HTML, PDF, and JSON.

Arguments
=========

``WORKFLOWS``
   Comma-separated list of workflow names to process.
   
   **Required:** Yes

Options
=======

``-h, --help``
   Show help message and exit.

``--all, -a``
   Include all executions of the selected workflow(s). If false, include only the last execution.
   
   **Default:** false

``--file FILE, -f FILE``
   Path to the StreamFlow configuration file.
   
   **Default:** Uses default database location

``--format [{html,pdf,eps,png,jpg,webp,svg,csv,json} ...]``
   Report output format(s). Can specify multiple formats.
   
   **Available formats:**
   
   * ``html`` - Interactive HTML report (default)
   * ``pdf`` - PDF document
   * ``eps`` - Encapsulated PostScript
   * ``png`` - PNG image
   * ``jpg`` - JPEG image
   * ``webp`` - WebP image
   * ``svg`` - SVG vector graphic
   * ``csv`` - CSV data export
   * ``json`` - JSON data export
   
   **Default:** html

``--group-by-step``
   Group execution of multiple instances of the same step on a single line.
   
   **Default:** Disabled

``--name NAME``
   Name of the report folder.
   
   **Default:** ``${WORKFLOW}-report``

``--outdir OUTDIR``
   Output directory to store the report file.
   
   **Default:** Current directory

Examples
========

**Generate HTML Report:**

.. code-block:: bash

   streamflow report my-workflow

**Generate Multiple Format Reports:**

.. code-block:: bash

   streamflow report --format html pdf json my-workflow

**Include All Executions:**

.. code-block:: bash

   streamflow report --all my-workflow

**Custom Output Directory:**

.. code-block:: bash

   streamflow report --outdir /path/to/reports my-workflow

**Multiple Workflows:**

.. code-block:: bash

   streamflow report workflow1,workflow2,workflow3

**Group by Step:**

.. code-block:: bash

   streamflow report --group-by-step my-workflow

Report Contents
===============

The generated report includes:

* **Workflow Overview:** Name, status, duration
* **Execution Timeline:** Visual timeline of step executions
* **Step Details:** Individual step execution times and statuses
* **Resource Usage:** CPU, memory, and other resource metrics
* **Data Transfers:** Information about data movements between locations

Related Commands
================

* :doc:`list` - List executed workflows
* :doc:`prov` - Export provenance data
* :doc:`run` - Execute a workflow

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/inspecting-results` - Inspecting workflow results
   - :doc:`/user-guide/troubleshooting` - Troubleshooting guide

See Also
========

* :doc:`list` - For listing workflows
* :doc:`prov` - For provenance archives
