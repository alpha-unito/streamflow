===============
Fault tolerance
===============

StreamFlow allows users to handle execution failures through two main extension points: :ref:`CheckpointManager <CheckpointManager>` and :ref:`FailureManager <FailureManager>`. These components can be used independently or in combination to provide a robust execution.

A key challenge in handling failures within hybrid workflows, which these two components must address, is the potential for data loss caused by the heterogeneity of execution locations.

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
======     ===============================================================

If the user does not specify a checkpoint manager in the StreamFlow file, the ``dummy`` implementation is used by default. This manager has no effect and performs no operations.

If the user defines the ``DefaultCheckpointManager``, the standard behavior is to save all intermediate files to a local persistent location.

.. jsonschema:: https://streamflow.di.unito.it/schemas/recovery/default_checkpoint_manager.json

FailureManager
==============

``recoverable`` decorator

.. code-block::python
    async def close(self) -> None:
    	...

    def get_request(self, job_name: str) -> RetryRequest:
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

    async def update_request(self, job_name: str) -> None:
    	...

The ``get_request`` method returns an instance of ``RetryRequest``, which contains information helpful for recovering the ``job_name``.

The ``is_recovering`` method returns ``True`` if the ``job_name`` is already undergoing recovery; otherwise, it returns ``False``.

The ``notify`` method is called when a ``job`` terminates and it generates the output data.

The ``recover`` method is called to recover from a ``job`` failure.

The ``update_request`` method is called when a ``job`` is involved in a recovery process.

Implementations
---------------

=======    =========================================================
Type       Class
=======    =========================================================
default    streamflow.recovery.failure_manager.DefaultFailureManager
dummy      streamflow.recovery.failure_manager.DummyFailureManager
======     =========================================================

If the user does not specify a failure manager in the StreamFlow file, the ``dummy`` implementation is used by default. This manager just propagates the error.

The ``default`` failure manager implements a retry-rollback strategy

.. jsonschema:: https://streamflow.di.unito.it/schemas/recovery/default_failure_manager.json

