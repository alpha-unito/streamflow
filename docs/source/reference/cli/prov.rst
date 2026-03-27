================
streamflow prov
================

.. meta::
   :keywords: StreamFlow, CLI, provenance, RO-Crate
   :description: Export provenance data for StreamFlow workflows

Synopsis
========

.. code-block:: bash

   streamflow prov [OPTIONS] WORKFLOW

Description
===========

Export workflow provenance data as a structured archive. StreamFlow supports RO-Crate format for capturing comprehensive workflow execution provenance.

Arguments
=========

``WORKFLOW``
   Name of the workflow to process.
   
   **Required:** Yes

Options
=======

``-h, --help``
   Show help message and exit.

``--add-file ADD_FILE``
   Add an external file to the provenance archive. File properties are specified as comma-separated key-value pairs.
   
   **Required properties:**
   
   * ``src`` - Source file path (mandatory)
   * ``dst`` - Destination path in archive (default: ``/``)
   
   Additional properties can be specified as strings or JSON objects.

``--add-property ADD_PROPERTY``
   Add a property to the archive manifest. Properties are specified as comma-separated key-value pairs and can be strings or JSON objects.

``--all, -a``
   Include all executions of the selected workflow. If false, include only the last execution.
   
   **Default:** false

``--file FILE, -f FILE``
   Path to the StreamFlow configuration file.
   
   **Default:** Uses default database location

``--name NAME``
   Name of the generated archive.
   
   **Default:** ``${WORKFLOW_NAME}.crate.zip``

``--outdir OUTDIR``
   Directory where the archive should be created.
   
   **Default:** Current directory

``--type {run_crate}, -t {run_crate}``
   Type of provenance archive to generate.
   
   **Available types:**
   
   * ``run_crate`` - RO-Crate workflow run provenance
   
   **Default:** run_crate

Examples
========

**Export Provenance Archive:**

.. code-block:: bash

   streamflow prov my-workflow

**Include All Executions:**

.. code-block:: bash

   streamflow prov --all my-workflow

**Custom Archive Name:**

.. code-block:: bash

   streamflow prov --name my-archive.zip my-workflow

**Custom Output Directory:**

.. code-block:: bash

   streamflow prov --outdir /path/to/archives my-workflow

**Add External File:**

.. code-block:: bash

   streamflow prov --add-file src=/path/to/file.txt,dst=/docs/file.txt my-workflow

**Add Metadata Property:**

.. code-block:: bash

   streamflow prov --add-property author="John Doe",license=MIT my-workflow

Archive Contents
================

The generated RO-Crate archive includes:

* **Workflow Definition:** CWL workflow files
* **Execution Metadata:** Timestamps, status, parameters
* **Input/Output Data:** Links or copies of data files
* **RO-Crate Metadata:** JSON-LD manifest describing archive contents
* **Provenance Information:** Complete execution trace

RO-Crate Format
===============

StreamFlow generates provenance archives following the `RO-Crate <https://www.researchobject.org/ro-crate/>`_ specification, which provides:

* **Interoperability:** Standard format for workflow provenance
* **Reproducibility:** Captures complete execution context
* **FAIR Principles:** Findable, Accessible, Interoperable, Reusable data

Related Commands
================

* :doc:`report` - Generate execution report
* :doc:`list` - List executed workflows
* :doc:`run` - Execute a workflow

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/inspecting-results` - Inspecting workflow results

**External Resources:**
   - `RO-Crate Specification <https://www.researchobject.org/ro-crate/>`_
   - `Workflow Run Crate <https://www.researchobject.org/workflow-run-crate/>`_

See Also
========

* :doc:`report` - For execution reports
* :doc:`list` - For listing workflows
