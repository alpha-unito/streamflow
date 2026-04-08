======================
Environment Variables
======================

.. meta::
   :keywords: StreamFlow, environment variables, configuration
   :description: Environment variable reference for StreamFlow

Overview
========

StreamFlow can be configured using environment variables for certain runtime behaviors.

Available Variables
===================

``STREAMFLOW_CONFIG``
   Default path to StreamFlow configuration file.
   
   **Type:** String  
   **Default:** ``streamflow.yml``  
   **Usage:**
   
   .. code-block:: bash
   
      export STREAMFLOW_CONFIG=/path/to/config.yml
      streamflow run

``STREAMFLOW_DATABASE``
   Path to StreamFlow database file.
   
   **Type:** String  
   **Default:** ``~/.streamflow/streamflow.db``  
   **Usage:**
   
   .. code-block:: bash
   
      export STREAMFLOW_DATABASE=/path/to/database.db

``STREAMFLOW_LOG_LEVEL``
   Logging level for StreamFlow.
   
   **Type:** String  
   **Values:** ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL``  
   **Default:** ``INFO``  
   **Usage:**
   
   .. code-block:: bash
   
      export STREAMFLOW_LOG_LEVEL=DEBUG
      streamflow run streamflow.yml

``STREAMFLOW_WORKING_DIR``
   Working directory for StreamFlow temporary files.
   
   **Type:** String  
   **Default:** ``~/.streamflow``  
   **Usage:**
   
   .. code-block:: bash
   
      export STREAMFLOW_WORKING_DIR=/tmp/streamflow

Examples
========

**Set Multiple Variables:**

.. code-block:: bash

   export STREAMFLOW_LOG_LEVEL=DEBUG
   export STREAMFLOW_DATABASE=/data/streamflow.db
   streamflow run workflow.yml

**Temporary Configuration:**

.. code-block:: bash

   STREAMFLOW_LOG_LEVEL=DEBUG streamflow run workflow.yml

**In Docker:**

.. code-block:: bash

   docker run -e STREAMFLOW_LOG_LEVEL=DEBUG alphaunito/streamflow:latest

**In Shell Script:**

.. code-block:: bash

   #!/bin/bash
   export STREAMFLOW_LOG_LEVEL=DEBUG
   export STREAMFLOW_DATABASE=/data/streamflow.db
   streamflow run streamflow.yml

Related Documentation
=====================

**Configuration:**
   - :doc:`streamflow-yml` - Main configuration file

**CLI:**
   - :doc:`/reference/cli/run` - Run command options

See Also
========

* Many settings can also be configured via CLI options
* Database location can be specified in ``streamflow.yml``
