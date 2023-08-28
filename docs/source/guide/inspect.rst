=====================
Inspect workflow runs
=====================

The StreamFlow Command Line Interface (CLI) offers some feature to inspect workflow runs and collect metadata from them. It is possible to :ref:`list <List executed workflows>` past workflow executions, retrieving generic metadata such as execution time and completion status. It is also possible to generate a :ref:`report <Generate a report>` of a specific execution. Finally, it is possible to generate a :ref:`provenance archive <Collect provenance data>` for a given workflow, ready to be shared and published.

List executed workflows
=======================

The history of workflow executions initiated by the user can be printed using the following subcommand:

.. code-block:: bash

    streamflow list

The resulting table will contain, for each workflow name, the workflow type and the number of executions associated with that name. For example:

===================     ====     ==========
NAME                    TYPE     EXECUTIONS
===================     ====     ==========
my-workflow-example     cwl      2
===================     ====     ==========

To obtain more details related to the different runs of the ``<name>`` workflows, i.e., start time, end time, and final status, use the following subcommand:

.. code-block:: bash

    streamflow list <name>

For example:

================================     ================================     ==========
START_TIME                           END_TIME                             STATUS
================================     ================================     ==========
2023-03-14T10:44:11.304081+00:00     2023-03-14T10:44:18.345231+00:00     FAILED
2023-03-14T10:45:28.305321+00:00     2023-03-14T10:46:21.274293+00:00     COMPLETED
================================     ================================     ==========

Generate a report
=================

To generate a timeline report of a workflow execution, use the following subcommand:

.. code-block:: bash

    streamflow report

By default, an interactive ``HTML`` report is generated, but users can specify a different format through the ``--format`` option.

Collect provenance data
=======================

StreamFlow supports the `Workflow Run RO-Crate <https://www.researchobject.org/workflow-run-crate/>`_ provenance format, an `RO-Crate <https://www.researchobject.org/ro-crate/>`_ profile for capturing the provenance of an execution of a computational workflow.

To generate a provenance archive containing the last execution of a given workflow name (see :ref:`above <List executed workflows>`), use the following command:

.. code-block:: bash

    streamflow prov

The ``--all`` option can instead be used to include the whole history of workflow execution inside a single archive.

The ``--name`` option defines the name of the archive. By default, the archive will take the workflow name as basename and ``.crate.zip`` as extension.

The ``--outdir`` option states in which location the archive will be placed (by default, it will be created in the current directory).