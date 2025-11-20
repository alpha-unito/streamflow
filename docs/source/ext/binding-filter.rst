=============
BindingFilter
=============

StreamFlow lets users map steps to :ref:`multiple targets <Multiple targets>`. A ``BindingFilter`` object implements a strategy to manipulate and reorder the list of targets bound to a given step before the StreamFlow :ref:`Scheduler <Scheduler>` component evaluates them. The ``BindingFilter`` interface specified in the ``streamflow.core.deployment`` module contains a single ``get_targets`` method:

.. code-block:: python

    async def get_targets(
        self, job: Job, targets: MutableSequence[Target]
    ) -> MutableSequence[Target]:
        ...

The ``get_targets`` method receives a ``Job`` object and a list of ``Target`` objects, the list of targets specified by the user, and returns another list of ``Target`` objects. The :ref:`Scheduler <Scheduler>` component will evaluate the returned list of targets to find an allocation for the ``Job`` object.

By default, if no ``BindingFilter`` is specified for a multi-target step binding, all the ``Target`` objects will be evaluated in the original order.

Implementations
===============

=========     ==========================================================
Type          Class
=========     ==========================================================
shuffle       streamflow.deployment.filter.shuffle.ShuffleBindingFilter
matching      streamflow.deployment.filter.shuffle.MatchingBindingFilter
=========     ==========================================================


ShuffleBindingFilter
^^^^^^^^^^^^^^^^^^^^
The ``ShuffleBindingFilter`` implementation does not discard any location, but simply randomizes the evaluation order at each invocation.

MatchingBindingFilter
^^^^^^^^^^^^^^^^^^^^^
The ``MatchingBindingFilter`` filters ``deployments`` by applying regex conditions to the job input ``ports``. A ``deployment`` is considered a match if its name matches one of the ``targets`` and the job input ``port`` values satisfy the specified regular expressions.

Make sure that the bound step has the input ``port`` with the same name defined in the filter.

The filter works with string data types, automatically casting non-string values, though this should be used with caution. It does not support matching on file, list, or object types.

If a ``service`` is provided for a ``deployment``, the filter ensures that both the ``deployment`` name and the ``service`` name match the target.

The main structure of the YAML configuration consists of a list of ``targets``. Each ``target` can either be defined by a single string representing the ``deployment`` name or by a record that specifies both a ``deployment`` name and an optional ``service`` name.

A ``job`` is a list that specifies ``port`` names and ``regex`` conditions. The ``ports`` and ``regex`` are used to filter the ``deployments`` based on the values of the specified ``ports``. Each ``deployment`` is considered a match for a given job if it satisfies all the ``regex`` conditions for all ``ports`` defined in that `job`.


An example of the filter follows:

.. code-block:: yaml

  bindingFilters:
    f_local:
      type: matching
      config:
        targets:
        - target: locally
          job:
          - port: extractfile
            regex: ".*.java"
        - target:
          deployment: leonardo
          job:
          - port: extractfile
            regex: ".*.c"
          - port: compiler
            regex: "gcc"

