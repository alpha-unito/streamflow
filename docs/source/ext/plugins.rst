=======
Plugins
=======

The StreamFlow control plane can be extended by means of self-contained plugins. This mechanism allows advanced users to implement application-specific behaviour and researchers to experiment new workflow orchestration ideas in a battle-tested ecosystem. Plus, it improves maintainability and reduces the memory footprint, as the StreamFlow codebase can implement just the core features.

A StreamFlow plugin is a Python package with two characteristics:

* A Python class extending the ``StreamFlowPlugin`` base class;
* An entry in the ``entry_points`` section of the ``setup.py`` configuration file, with key ``unito.streamflow.plugin`` and value ``<plugin_name>=<module_name>:<class_name>``, where ``plugin_name`` can be chosen at discretion of the user and ``<module_name>:<class_name>`` points to the class implementing the plugin.

The installed plugins can be explored using the StreamFlow Command Line Interface (CLI). To see the list of installed plugins, use the following subcommand:

.. code-block:: bash

    streamflow plugin list

To print additional details about a specific plugin called ``<name>``, including all the provided extensions, use the following subcommand:

.. code-block:: bash

    streamflow plugin show <name>


Extension points
================

StreamFlow supports several extension points, whose interfaces are described in a dedicated section of this documentation. For each type of extension point, the ``StreamFlowPlugin`` class exposes a method to register the custom implementation. All methods have the following signature:

.. code-block:: python

  def register_extension_point(name: str, cls: type[ExtensionPointType]) -> None:

The ``cls`` attribute points to the class that implements the extension point, while the ``name`` attribute defines the keyword that users should put in the ``type`` field of the ``streamflow.yml`` file to instantiate this implementation.

.. note::

    In order to avoid name conflicts, it is a good practice to prefix the ``name`` variable with an organization-specific prefix. Unprefixed names are reserved for StreamFlow core components. However, since plugins are always loaded after core components, creating a plugin with the same name of a core implementation will override it. The same will happen when two third-party plugins have the same name, but the loading order is undefined and may vary from one Python environment to another, hindering reproducibility.

The ``register`` method of the ``StreamFlowPlugin`` implementation must contain all the required calls to the ``register_extension_point`` methods in order to make the extension points available to the StreamFlow control plane. Note that a single ``StreamFlowPlugin`` can register multiple extension points, and even multiple implementations for the same extension point.

Each extension point class extends the ``SchemaEntity`` abstract class, whose method ``get_schema`` returns the class configuration in `JSON Schema 2019-09 <https://json-schema.org/draft/2019-09/release-notes.html>`_ format. The root entity of this file must be an ``object`` defining a set of properties, which will be exposed to the user in the ``config`` section of the ``streamflow.yml`` file, type-checked by StreamFlow at the beginning of each workflow run, and passed to the class constructor at every instantiation.

The base class for each extension point defined by StreamFlow and the respective registration method exposed by the ``StreamFlowPlugin`` class are reported in the table below:

=============================================================================================     ===================================
Base Class                                                                                        Registration Method
=============================================================================================     ===================================
:ref:`streamflow.core.deployment.BindingFilter <BindingFilter>`                                   ``register_binding_filter``
:ref:`streamflow.core.recovery.CheckpointManager <CheckpointManager>`                             ``register_checkpoint_manager``
:ref:`streamflow.cwl.requirement.docker.translator.CWLDockerTranslator <CWLDockerTranslator>`     ``register_cwl_docker_translator``
:ref:`streamflow.core.deployment.Connector <Connector>`                                           ``register_connector``
:ref:`streamflow.core.data.DataManager <DataManager>`                                             ``register_data_manager``
:ref:`streamflow.core.persistence.Database <Database>`                                            ``register_database``
:ref:`streamflow.core.deployment.DeploymentManager <DeploymentManager>`                           ``register_deployment_manager``
:ref:`streamflow.core.recovery.FailureManager <FailureManager>`                                   ``register_failure_manager``
:ref:`streamflow.core.scheduling.Policy <Policy>`                                                 ``register_policy``
:ref:`streamflow.core.scheduling.Scheduler <Scheduler>`                                           ``register_scheduler``
=============================================================================================     ===================================

In addition, a ``register_schema`` method allows to register additional JSON Schemas, which are not directly referenced by any ``SchemaEntity`` class through the ``get_schema`` method. This feature is useful to, for example, define some base abstract JSON Schema that concrete entities can extend.

Note that there is no official way to make JSON Schema files inherit properties from each other, as vanilla JSON Schema format does not support inheritance. However, it is possible to extend base schemas using the combination of `allOf <https://json-schema.org/understanding-json-schema/reference/combining.html#allof>`_ and `unevaluatedProperties <https://json-schema.org/understanding-json-schema/reference/object.html#unevaluated-properties>`_ directives of JSON Schema 2019-09, as follows:

.. code-block:: json

    {
      "$schema": "https://json-schema.org/draft/2019-09/schema",
      "$id": "my-schema-id.json",
      "type": "object",
      "allOf": [
        {
          "$ref": "my-base-schema-id.json"
        }
      ],
      "properties": {},
      "unevaluatedProperties": false
    }

.. note::

    Since JSON Schema extension is based on the JSON Reference mechanism ``$ref``, which collects schemas through their ``$id`` field, it is a good practice to include an organization-specific fqdn in the ``$id`` field of each JSON Schema to avoid clashes.

StreamFlow extensions can also be explored through the Command Line Interface (CLI). To print the set of installed extension instances divided by the targeted extension points, use the following subcommand:

.. code-block:: bash

    streamflow ext list

To print detailed documentation, including the associated JSON Schema, of an extension instance called ``<name>`` related to the extension point ``<extension-point>``, use the following subcommand:

.. code-block:: bash

    streamflow ext show <extension-point> <name>


Example: a PostgreSQL Plugin
============================

As an example, suppose that a class ``PostgreSQLDatabase`` implements a `PostgreSQL <https://www.postgresql.org/>`_-based implementation of the StreamFlow database. Then, a ``PostgreSQLStreamFlowPlugin`` class will have the following implementation:

.. code-block:: python

    from streamflow.core.persistence import Database
    from streamflow.ext import StreamFlowPlugin

    class PostgreSQLDatabase(Database):
        @classmethod
        def get_schema(cls) -> str:
            return (
                files(__package__)
                .joinpath("schemas")
                .joinpath("postgresql.json")
                .read_text("utf-8")
            )
        ...

    class PostgreSQLStreamFlowPlugin(StreamFlowPlugin):
        def register(self) -> None:
            self.register_database("unito.postgresql", PostgresqlDatabase)

Each extension point class must implement a ``get_schema`` method, which returns a `JSON Schema <https://json-schema.org/>`_ with all the configurable parameters that can be specified by the user in the ``streamflow.yml`` file. Such parameters will be propagated to the class constructor at each invocation. For example, the ``PostgreSQLDatabase`` class specified above points to a ``schemas/postgresql.json`` schema file in the same Python module.

A schema file should follow the `2019-09 <https://json-schema.org/draft/2019-09/release-notes.html>`_ version of JSON Schema. StreamFlow uses schema files to validate the ``streamflow.yml`` file at runtime before executing a workflow instance. Plus, it relies on schema ``properties`` to print documentation when a user invokes the ``streamflow ext show`` CLI subcommand. An example of schema file for the ``PostreSQLDatabase`` class is the following:

.. code-block:: json

    {
      "$schema": "https://json-schema.org/draft/2019-09/schema",
      "$id": "https://streamflow.di.unito.it/plugins/schemas/persistence/postgresql.json",
      "type": "object",
      "properties": {
        "dbname": {
          "type": "string",
          "description": "The name of the database to use"
        },
        "hostname": {
          "type": "string",
          "description": "The database hostname or IP address"
        },
        "maxConnections": {
          "type": "integer",
          "description": "Maximum size of the PostgreSQL connection pool. 0 means unlimited pool size",
          "default": 10
        },
        "password": {
          "type": "string",
          "description": "Password to use when connecting to the database"
        },
        "timeout": {
          "type": "integer",
          "description": "The timeout (in seconds) for connection operations",
          "default": 20
        },
        "username": {
          "type": "string",
          "description": "Username to use when connecting to the database"
        }
      },
      "required": [
        "dbname",
        "hostname",
        "username",
        "password"
      ],
      "additionalProperties": false
    }

Suppose that the ``PostgreSQLStreamFlowPlugin`` class is defined in a ``plugin.py`` file, which is part of a ``streamflow_postgresql`` module. Then, the ``pyproject.toml`` file will contain the following declaration:

.. code-block:: toml

    [project.entry-points]
    "unito.streamflow.plugin" = {"unito.postgresql" = "streamflow_postgresql.plugin:PostgreSQLStreamFlowPlugin"}

Imagine now that the code described above has been published in a package called ``streamflow-postgresql``. Then, the plugin can be installed with ``pip`` as a normal package:

.. code-block:: bash

    pip install streamflow-postgresql

Then, StreamFlow users can instantiate a PostgreSQL database connector for their workflow executions by adding the following lines in the ``streamflow.yml`` file:

.. code-block:: yaml

    database:
      type: unito.postgresql
      config:
        dbname: "sf"
        hostname: "localhost"
        username: "sf-user"
        password: "1234!"
        maxConnections: 50

The full source code of the StreamFlow PostgreSQL example plugin is available in on `GitHub <https://github.com/alpha-unito/streamflow-postgresql>`_.