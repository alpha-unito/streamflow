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
The ``MatchingBindingFilter`` filters ``target`` objects by applying an exact ``match`` between an expected value and job input ``ports``. A ``deployment`` is considered a match if its name matches one of the ``targets`` and the input values of the ``Job`` object satisfy the specified value. Otherwise, the ``target`` is discarded.
The ``MatchingBindingFilter`` allows to define a list of ``filters``, where each filter has a ``target`` and a ``job`` definition:

* A ``target`` should be a ``deployment`` to which the step is bound. If the ``deployment`` does not match any target in the filters list, it is discarded. The filter can be stricter, including a ``service`` name as well.
* A ``job`` is a list that specifies ``port`` names and ``match`` values. The ``port`` option must specify the name of an input of the bound step. The ``match`` value must be equal to the value inside the ``port``. The ``filter`` works with string data types. Non-string values are automatically cast to strings, but this process may lead to unexpected errors. Lists, objects, and other complex types are not supported. When multiple ``ports`` are defined, the ``deployment`` is chosen only if all the ``ports`` match the expected values.

If the same ``deployment`` is part of multiple ``filters`` with different conditions, it will be selected if it satisfies at least one of such conditions.

The following snippet reports an example of ``MatchingBindingFilter`` configuration

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
        - target: lumi
          job:
          - port: extractfile
            match: "hello.rs"

The first ``filter`` targets a ``deployment`` named ``locally``, which is selected whenever the ``extractfile`` input port contains the string ``Hello.java``.

The third ``filter`` targets a ``deployment`` named ``leonardo`` and a ``service`` named ``boost``. Since the ``job`` contains two rules, the ``deployment`` will be chosen only when the ``extractfile`` port contains the ``hello.c`` string *AND* the ``compiler`` port contains the ``gcc`` string.

The second and fourth ``filters`` both target a ``deployment`` named ``lumi``. Only a single satisfied rule is sufficient for the ``deployment`` to be selected. Therefore, the ``lumi`` deployment will be targeted whenever the ``extractfile`` port contains the ``hello.rs`` string *OR* when the ``extractfile`` port contains the ``hello.c`` string *AND* the ``compiler`` port contains the ``gcc`` string.

.. jsonschema:: https://streamflow.di.unito.it/schemas/deployment/filter/matching.json
    :lift_description: true
    :lift_definitions: true
    :auto_reference: true
    :auto_target: true