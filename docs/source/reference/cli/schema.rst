==================
streamflow schema
==================

.. meta::
   :keywords: StreamFlow, CLI, schema, JSON schema, validation
   :description: Dump StreamFlow configuration schema

Synopsis
========

.. code-block:: bash

   streamflow schema [OPTIONS] [VERSION]

Description
===========

Dump the JSON schema for StreamFlow configuration files. Useful for validation and IDE autocomplete.

Arguments
=========

``VERSION``
   Version of the StreamFlow schema to print.
   
   **Optional:** Defaults to latest version

Options
=======

``-h, --help``
   Show help message and exit.

``--pretty``
   Format JSON output with indentation for readability.
   
   **Default:** Disabled

Examples
========

**Dump Current Schema:**

.. code-block:: bash

   streamflow schema

**Pretty-Printed Schema:**

.. code-block:: bash

   streamflow schema --pretty

**Specific Version:**

.. code-block:: bash

   streamflow schema v1.0

**Save to File:**

.. code-block:: bash

   streamflow schema --pretty > streamflow-schema.json

Usage
=====

The generated JSON schema can be used for:

* **Validation:** Validate configuration files against the schema
* **IDE Support:** Enable autocomplete in editors with JSON schema support
* **Documentation:** Generate configuration documentation
* **Tools:** Build configuration tools and validators

JSON Schema Support
===================

Many editors support JSON schema for YAML files:

* **VS Code:** Add ``$schema`` field to your YAML files
* **IntelliJ IDEA:** Configure schema mapping
* **vim/neovim:** Use YAML language server with schema support

Related Documentation
=====================

**Configuration:**
   - :doc:`/reference/configuration/index` - Configuration reference
   - :doc:`/reference/configuration/streamflow-yml` - Main config file

See Also
========

* `JSON Schema <https://json-schema.org/>`_ - JSON Schema specification
