git s# User Guide File Templates

This reference contains complete templates for User Guide documentation files.

## Standard User Guide Template

```rst
==============
Document Title
==============

.. meta::
   :keywords: keyword1, keyword2, keyword3
   :description: Brief 1-2 sentence description of what this document teaches

Prerequisites
=============

Before reading this guide:

* :doc:`prerequisite-doc-1` - What you need from it
* :doc:`prerequisite-doc-2` - What you need from it

What You'll Learn
=================

After completing this guide, you will:

* Learning objective 1 (specific and measurable)
* Learning objective 2 (specific and measurable)
* Learning objective 3 (specific and measurable)

[MAIN CONTENT SECTIONS]

Quick Reference
===============

[Summary table or command list]

Next Steps
==========

* :doc:`next-logical-document` - What to learn next
* :doc:`related-document` - Related concepts

Related Documentation
=====================

**User Guide:**
   - :doc:`related-guide-1`
   - :doc:`related-guide-2`

**Reference:**
   - :doc:`/reference/related-reference-1`
   - :doc:`/reference/related-reference-2`
```

## Installation Guide Template

```rst
============
Installation
============

.. meta::
   :keywords: installation, setup, pip, uv
   :description: Install StreamFlow on Linux or macOS

What You'll Learn
=================

* Install StreamFlow using pip or uv
* Verify installation
* Install optional dependencies

System Requirements
===================

* Python 3.10-3.14
* Linux or macOS (Windows not supported)
* 4GB RAM minimum

Installation Methods
====================

Using pip
---------

.. code-block:: bash

   $ pip install streamflow

   [ACTUAL OUTPUT]

Using uv
--------

.. code-block:: bash

   $ uv pip install streamflow

   [ACTUAL OUTPUT]

Verification
============

.. code-block:: bash

   $ streamflow version

   StreamFlow version 0.2.0

Next Steps
==========

* :doc:`quickstart` - Run your first workflow
```

## Quickstart Template

```rst
==========
Quickstart
==========

.. meta::
   :keywords: quickstart, getting started, first workflow
   :description: Run your first StreamFlow workflow in 10 minutes

Prerequisites
=============

* :doc:`installation` - StreamFlow must be installed

What You'll Learn
=================

* Create a simple workflow
* Configure StreamFlow
* Run a workflow
* View results

Create Workflow Files
=====================

Create ``workflow.cwl``:

.. code-block:: yaml
   :caption: workflow.cwl - Simple workflow

   [COMPLETE EXAMPLE]

Create ``streamflow.yml``:

.. code-block:: yaml
   :caption: streamflow.yml - Configuration

   [COMPLETE EXAMPLE]

Run the Workflow
================

.. code-block:: bash

   $ streamflow run streamflow.yml

   [ACTUAL OUTPUT]

Next Steps
==========

* :doc:`writing-workflows` - Learn CWL syntax
* :doc:`configuring-deployments` - Configure execution environments
```

## Configuration Guide Template

```rst
=======================
Configuring Deployments
=======================

.. meta::
   :keywords: deployments, configuration, docker, kubernetes
   :description: Configure execution environments for StreamFlow workflows

Prerequisites
=============

* :doc:`installation` - StreamFlow installed
* Docker installed (for container examples)

What You'll Learn
=================

* Define deployments in streamflow.yml
* Configure Docker deployments
* Configure remote deployments
* Use multiple deployments

Deployment Structure
====================

Deployments define where workflow steps execute:

.. code-block:: yaml
   :caption: streamflow.yml - Basic structure

   deployments:
     deployment-name:
       type: connector-type
       config:
         # Configuration options

Basic Docker Deployment
=======================

.. code-block:: yaml
   :caption: streamflow.yml - Docker deployment

   deployments:
     docker-python:
       type: docker
       config:
         image: python:3.10

For complete configuration options, see :doc:`/reference/connectors/docker`.

Next Steps
==========

* :doc:`binding-workflows` - Bind steps to deployments
* :doc:`/reference/connectors/index` - All connector options
```

## Troubleshooting Template

```rst
===============
Troubleshooting
===============

.. meta::
   :keywords: troubleshooting, errors, debugging, problems
   :description: Common problems and solutions for StreamFlow

Prerequisites
=============

* :doc:`running-workflows` - Understanding workflow execution

Common Problems
===============

Command Not Found
-----------------

**Symptom:**

.. code-block:: text

   streamflow: command not found

**Solution:**

Ensure StreamFlow is installed:

.. code-block:: bash

   $ pip list | grep streamflow

If not installed, run:

.. code-block:: bash

   $ pip install streamflow

Docker Connection Failed
------------------------

**Symptom:**

.. code-block:: text

   Error: Cannot connect to Docker daemon

**Solution:**

1. Verify Docker is running:

   .. code-block:: bash

      $ docker ps

2. Check user permissions:

   .. code-block:: bash

      $ groups | grep docker

3. Add user to docker group (Linux):

   .. code-block:: bash

      $ sudo usermod -aG docker $USER
      $ newgrp docker

Related Documentation
=====================

* :doc:`installation` - Installation troubleshooting
* :doc:`running-workflows` - Execution troubleshooting
```

## Advanced Pattern Template

```rst
=================
[Pattern Name]
=================

.. meta::
   :keywords: advanced, pattern-name, specific-keywords
   :description: Brief description of advanced pattern

Prerequisites
=============

* :doc:`/user-guide/running-workflows` - Basic workflow execution
* Understanding of [specific concept]

What You'll Learn
=================

* Specific technique 1
* Specific technique 2
* When to use this pattern

Overview
========

[1-2 paragraph explanation of the pattern]

Basic Example
=============

.. code-block:: yaml
   :caption: streamflow.yml - Basic pattern

   [MINIMAL WORKING EXAMPLE]

Advanced Example
================

.. code-block:: yaml
   :caption: streamflow.yml - Advanced usage

   [MORE COMPLEX EXAMPLE]

Use Cases
=========

Use this pattern when:

* Scenario 1
* Scenario 2
* Scenario 3

Related Documentation
=====================

* :doc:`/user-guide/related-guide`
* :doc:`/reference/related-reference`
```
