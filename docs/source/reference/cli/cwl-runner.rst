===========
cwl-runner
===========

.. meta::
   :keywords: StreamFlow, CWL, cwl-runner, Common Workflow Language
   :description: CWL standard runner interface for StreamFlow

Synopsis
========

.. code-block:: bash

   cwl-runner [OPTIONS] WORKFLOW INPUTS

Description
===========

Execute CWL workflows using the CWL standard ``cwl-runner`` interface. StreamFlow implements this interface for compatibility with CWL-based tools and pipelines.

Arguments
=========

``WORKFLOW``
   Path to the CWL workflow file (``.cwl``).
   
   **Required:** Yes

``INPUTS``
   Path to the inputs file (typically YAML or JSON).
   
   **Required:** Yes

Options
=======

``--streamflow-file FILE``
   Path to StreamFlow configuration file for deployment and binding configuration.
   
   **Optional:** If not specified, runs locally

``--debug``
   Enable debug output.

``--quiet``
   Suppress informational messages.

``--outdir DIRECTORY``
   Output directory for results.
   
   **Default:** Current directory

Examples
========

**Basic CWL Execution:**

.. code-block:: bash

   cwl-runner workflow.cwl inputs.yml

**With StreamFlow Configuration:**

.. code-block:: bash

   cwl-runner --streamflow-file streamflow.yml workflow.cwl inputs.yml

**Custom Output Directory:**

.. code-block:: bash

   cwl-runner --outdir /path/to/results workflow.cwl inputs.yml

**Debug Mode:**

.. code-block:: bash

   cwl-runner --debug workflow.cwl inputs.yml

CWL Compatibility
=================

StreamFlow implements CWL standard versions:

* **CWL v1.0** - Fully supported
* **CWL v1.1** - Fully supported  
* **CWL v1.2** - Fully supported

For conformance details, see the CWL conformance documentation.

Differences from streamflow run
================================

The ``cwl-runner`` interface:

* **CWL Standard:** Implements the CWL reference interface
* **Compatibility:** Works with CWL tools expecting ``cwl-runner``
* **Simpler:** Takes workflow + inputs directly
* **Limited Config:** Uses ``--streamflow-file`` for StreamFlow-specific configuration

The ``streamflow run`` command:

* **StreamFlow Native:** Uses StreamFlow configuration format
* **Full Features:** Access to all StreamFlow features
* **Configuration:** Single YAML file with workflow, deployments, bindings

Related Commands
================

* :doc:`run` - Native StreamFlow execution interface

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/writing-workflows` - Writing CWL workflows
   - :doc:`/user-guide/running-workflows` - Running workflows

**External Resources:**
   - `Common Workflow Language <https://www.commonwl.org/>`_
   - `CWL User Guide <https://www.commonwl.org/user_guide/>`_

See Also
========

* :doc:`run` - For StreamFlow native interface
* `cwltool <https://github.com/common-workflow-language/cwltool>`_ - CWL reference implementation
