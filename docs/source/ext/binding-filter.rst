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

By default, if no ``BindingFilter`` is specified for a multi-target step binding, all the ``Target`` objects will be evaluated in the original order. In addition, StreamFlow defines a ``ShuffleBindingFilter`` implementation to randomise the evaluation order at any invocation.

Implementations
===============

=======     =========================================================
Type        Class
=======     =========================================================
shuffle     streamflow.deployment.filter.shuffle.ShuffleBindingFilter
=======     =========================================================
