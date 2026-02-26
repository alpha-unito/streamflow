==================
Workflow Configuration
==================

.. meta::
   :keywords: StreamFlow, workflow, configuration, CWL
   :description: Workflow configuration reference for StreamFlow

Overview
========

Workflow configuration defines how CWL workflows are executed in StreamFlow.

Configuration Structure
=======================

.. code-block:: yaml

   workflows:
     workflow-name:
       type: cwl
       config:
         file: path/to/workflow.cwl
         settings: path/to/inputs.yml

Fields Reference
================

``type``
   Workflow type identifier.
   
   **Type:** String  
   **Required:** Yes  
   **Values:** ``cwl`` (only supported type)

``config``
   Workflow-specific configuration object.
   
   **Type:** Object  
   **Required:** Yes

``config.file``
   Path to CWL workflow definition file.
   
   **Type:** String  
   **Required:** Yes  
   **Format:** Relative or absolute path to ``.cwl`` file

``config.settings``
   Path to workflow inputs file.
   
   **Type:** String or Object  
   **Required:** Yes  
   **Format:** Path to YAML/JSON file, or inline object

Examples
========

**Basic Configuration:**

.. code-block:: yaml

   workflows:
     my-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml

**Inline Settings:**

.. code-block:: yaml

   workflows:
     my-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings:
           input_file: data.txt
           num_threads: 4

**Absolute Paths:**

.. code-block:: yaml

   workflows:
     my-workflow:
       type: cwl
       config:
         file: /path/to/workflow.cwl
         settings: /path/to/inputs.yml

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/writing-workflows` - Writing CWL workflows

**Configuration:**
   - :doc:`streamflow-yml` - Main configuration file

See Also
========

* `CWL Specification <https://www.commonwl.org/>`_ - CWL workflow format
