==========
Quickstart
==========

.. meta::
   :description: Get started with StreamFlow in 10 minutes - install, create your first workflow, and run it
   :keywords: StreamFlow, quickstart, tutorial, getting started, first workflow
   :audience: users
   :difficulty: beginner
   :reading_time_minutes: 10

**Prerequisites:**

* :doc:`installation` - StreamFlow installed

**What You'll Learn:**

* Create a simple CWL workflow
* Configure and run a workflow with StreamFlow
* View workflow results

Step 1: Create Project Directory
=================================

.. code-block:: bash

   mkdir streamflow-quickstart
   cd streamflow-quickstart

Step 2: Create CWL Workflow
============================

Create ``hello-workflow.cwl``:

.. literalinclude:: ../../examples/quickstart/hello-workflow.cwl
   :language: yaml
   :caption: hello-workflow.cwl - CWL Workflow Definition
   :linenos:

Step 3: Create Workflow Inputs
===============================

Create ``inputs.yml``:

.. literalinclude:: ../../examples/quickstart/inputs.yml
   :language: yaml
   :caption: inputs.yml - Workflow Inputs
   :linenos:

Step 4: Create StreamFlow Configuration
========================================

Create ``streamflow.yml``:

.. literalinclude:: ../../examples/quickstart/streamflow.yml
   :language: yaml
   :caption: streamflow.yml - StreamFlow Configuration
   :linenos:

Step 5: Run the Workflow
=========================

.. code-block:: bash

   $ streamflow run streamflow.yml

   2026-02-24 10:52:43.304 INFO     Processing workflow fde2a338-33f7-42c3-80be-8a59cccabf53
   2026-02-24 10:52:43.322 INFO     EXECUTING workflow fde2a338-33f7-42c3-80be-8a59cccabf53
   2026-02-24 10:52:43.382 INFO     EXECUTING step / (job /0) locally
   2026-02-24 10:52:43.770 INFO     COMPLETED Step /
   2026-02-24 10:52:43.773 INFO     COMPLETED workflow execution

Step 6: Check Results
======================

.. code-block:: bash

   $ cat output.txt

   Hello from StreamFlow!

Success! Your first workflow executed and produced the expected output.

Using Docker (Optional)
========================

To run the same workflow in a Docker container, update ``streamflow.yml``:

.. code-block:: yaml
   :caption: streamflow.yml - With Docker
   :emphasize-lines: 10-14,16-19

   version: v1.0

   workflows:
     hello-workflow:
       type: cwl
       config:
         file: hello-workflow.cwl
         settings: inputs.yml

   deployments:
     docker-env:
       type: docker
       config: {}

   bindings:
     - step: /
       target:
         deployment: docker-env

Run again:

.. code-block:: bash

   $ streamflow run streamflow.yml

   2026-02-24 10:55:12.445 INFO     EXECUTING step / (job /0) on docker-env
   2026-02-24 10:55:13.102 INFO     COMPLETED Step /

The workflow now runs in a Docker container instead of locally.

Inspect Workflow Execution
===========================

List executed workflows:

.. code-block:: bash

   $ streamflow list

   NAME             STATUS      START                  END
   hello-workflow   COMPLETED   2026-02-24 10:52:43    2026-02-24 10:52:43

View detailed report:

.. code-block:: bash

   $ streamflow report hello-workflow

   Report generated: hello-workflow.html

Common Issues
=============

**Workflow not found:** Ensure you're in the directory containing ``streamflow.yml``

**Docker errors:** See :doc:`troubleshooting` for Docker setup issues

**CWL validation errors:** Validate your workflow with ``cwltool --validate hello-workflow.cwl``

Next Steps
==========

* :doc:`writing-workflows` - Learn CWL workflow syntax
* :doc:`configuring-deployments` - Set up remote execution environments
* :doc:`binding-workflows` - Control where workflow steps execute
* :doc:`troubleshooting` - Solutions to common issues
