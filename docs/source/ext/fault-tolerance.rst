===============
Fault tolerance
===============

StreamFlow allows users to handle execution failures through two main extension points: :ref:`CheckpointManager <CheckpointManager>` and :ref:`FailureManager <FailureManager>`. These components can be used independently or in combination to provide robust execution.

A key challenge in handling failures within hybrid workflows, which these two components must address, is how to recover from a data loss within the potentially high heterogeneity of StreamFlow execution locations.

CheckpointManager
=================

The ``CheckpointManager`` interface defines the strategy for persisting the intermediate data of a workflow execution at specific persistent locations. When used in combination with the :ref:`FailureManager <FailureManager>` (for example, through a rollback strategy), it guarantees that data is not lost and is shielded from the domino effect of re-executing the entire workflow.

.. code-block:: python

    async def close(self) -> None:
        ...

    def register(self, data_location: DataLocation) -> None:
        ...

Implementations
---------------

=======    ===============================================================
Type       Class
=======    ===============================================================
default    streamflow.recovery.checkpoint_manager.DefaultCheckpointManager
dummy      streamflow.recovery.checkpoint_manager.DummyCheckpointManager
=======    ===============================================================

If the user does not specify a checkpoint manager in the StreamFlow file, the ``dummy`` implementation is used by default. This manager has no effect and performs no operations.

If the user defines the ``DefaultCheckpointManager``, the standard behavior is to save all intermediate files to a local persistent location.

.. jsonschema:: https://streamflow.di.unito.it/schemas/recovery/default_checkpoint_manager.json
    :lift_description: true

FailureManager
==============

The ``FailureManager`` interface manages the recovery logic for failed workflow steps, whether the failure originates from the step application or the unavailability of an execution location. It is defined in the ``streamflow.core.recovery`` module and exposes several public methods:

.. code-block:: python

    async def close(self) -> None:
        ...

    async def is_recovering(self, job_name: str) -> bool:
        ...

    async def notify(
        self,
        output_port: str,
        output_token: Token,
        job_token: JobToken | None = None,
    ) -> None:
        ...

    async def recover(self, job: Job, step: Step, exception: BaseException) -> None:
        ...

The ``is_recovering`` method returns ``True`` if the ``job_name`` is already undergoing recovery; otherwise, it returns ``False``.

The ``notify`` method is called whenever a ``Job`` terminates execution correctly. The method receives three input parameters: the output ``Token`` object generated, the output ``Port`` object where the token is placed, and finally, the ``JobToken`` object which contains the ``Job``.

The ``recover`` method serves as the primary entry point for handling job failures. The method receives three input parameters: the failed ``Job`` object, the ``Step`` object related to the ``job``, and the ``Exception`` that triggered the failure.

Implementations
---------------

========   ==========================================================
Type       Class
========   ==========================================================
dummy      streamflow.recovery.failure_manager.DummyFailureManager
rollback   streamflow.recovery.failure_manager.RollbackFailureManager
========   ==========================================================

If the user does not specify a failure manager in the StreamFlow file, the ``dummy`` implementation is used by default. This manager simply propagates the error.

The ``rollback`` failure manager implements a retry-rollback strategy. When a job fails, the manager verifies the availability of all required inputs. Data loss may occur if a location with a volatile filesystem fails; in such cases, the jobs responsible for generating that data are rolled back to facilitate recovery.

.. jsonschema:: https://streamflow.di.unito.it/schemas/recovery/rollback_failure_manager.json
    :lift_description: true

