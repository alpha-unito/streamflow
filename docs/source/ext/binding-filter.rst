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
The ``MatchingBindingFilter`` filters ``deployments`` by applying ``regex`` conditions to the job input ``ports``. A ``deployment`` is considered a match if its name matches one of the ``targets`` and the input ``port`` values of the ``Job`` object satisfy the specified regular expressions. Otherwise, it is discarded.

If a ``service`` is provided for a ``deployment``, the filter ensures that both the ``deployment`` name and the ``service`` name match the ``target``.

The main structure of the YAML configuration consists of a list of ``filters``.

Each filter defines a ``target`` and a ``job``:

* A ``target`` can either be a single string representing the ``deployment`` name or a record with keys ``deployment`` and ``service``, specifying both a ``deployment`` name and an optional ``service`` name.
* A ``job`` is a list that specifies ``port`` names and ``regex`` conditions. The ``port`` is an input of the step; ensure that the bound step has the input ``port`` with the same name as defined in the filter. The ``regex`` is used to match the value inside the ``port``. The filter works with string data types, automatically casting non-string values, although this should be used with caution. It does not support matching file, list, or object types. It is possible to define multiple ``ports``, and the ``deployment`` is chosen if all the ``regex`` conditions for the ``ports`` are satisfied.

An example of the filter follows:

.. code-block:: yaml

  bindingFilters:
    myfilter:
      type: matching
      config:
        filters:
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

.. jsonschema:: https://streamflow.di.unito.it/schemas/deployment/filter/matching.json
