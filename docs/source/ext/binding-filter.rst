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
The ``MatchingBindingFilter`` filters ``deployments`` by applying an exact ``match`` between an expected value and the job input ``ports``. A ``deployment`` is considered a match if its name matches one of the ``targets`` and the input ``port`` values of the ``Job`` object satisfy the specified value. Otherwise, the target ``deployment`` is discarded.

The ``MatchingBindingFilter`` allows to define a list of ``filters``, where each filter has a ``target`` and a ``job`` definition:

* A ``target`` should be a ``deployment`` on which the step is bound. If the ``deployment`` does not match any target in the filters list, it is discarded. The filter can be stricter, including the ``service`` of the ``deployment`` as well.
* A ``job`` is a list that specifies ``port`` names and ``match`` values. The ``port`` is an input of the step; ensure that the bound step has an input ``port`` with the same name as defined in the filter. The ``match`` value must be equal to the value inside the ``port``. The ``filter`` works with string data types, automatically casting non-string values, although this should be done with caution. It does not support matching file, list, or object types. Multiple ``ports`` can be defined, and the ``deployment`` is chosen only if all the ``ports`` match the expected values.

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
            match: "Hello.java"
        - target:
            deployment: lumi
          job:
          - port: extractfile
            match: "hello.c"
          - port: compiler
            match: "gcc"
        - target:
          	deployment: leonardo
            service: boost
          job:
          - port: extractfile
            match: "hello.c"
          - port: compiler
            match: "gcc"

.. jsonschema:: https://streamflow.di.unito.it/schemas/deployment/filter/matching.json
