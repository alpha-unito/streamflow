===============
Fault tolerance
===============

.. note::
   The failure tolerance features implemented in StreamFlow are the result of the research published in the paper *"A formal framework for fault tolerance in hybrid scientific workflows"* by Alberto Mulone, Doriana Medić, Iacopo Colonnelli, and Marco Aldinucci (Future Generation Computer Systems, 2026). For an in-depth discussion of the formal model and detailed implementation specifics, please refer to the `full article <https://doi.org/10.1016/j.future.2025.108188>`_.

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

Consider the following example:

.. code-block:: yaml

  checkpointManager:
    type: default
    config:
      checkpoint_dir: data/checkpoint

The ``DefaultCheckpointManager`` is enabled using the ``default`` type. The configuration specifies a ``checkpoint_dir`` using a relative path, which is resolved against the location of the ``streamflow.yml`` file. This directory serves as the location where all intermediate data is stored during execution.

Beyond the example, the ``checkpoint_dir`` property is optional. If it is not provided, the manager defaults to ``$TMPDIR/streamflow/checkpoint`` for storing intermediate data

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

Consider the following example:

.. code-block:: yaml

  failureManager:
    type: rollback
    config:
      max_retries: 10
      retry_delay: 5

The ``RollbackFailureManager`` is enabled using the ``rollback`` type. The ``max_retries`` is set to 10, which means that a job can be re-executed 10 times, including the rollback to recover from failures of other jobs. The ``retry_delay`` is set to 5, so the failure manager waits 5 seconds before starting a recovery process. This delay is helpful when a location fails. If the location has a restart mechanism, the failure manager waits to allow it to recover. By waiting for the original location to become available, the manager can access potentially to unique data not replicated elsewhere. This prevents the need for unnecessary rollbacks.

Both properties are optional. If the ``retry_delay`` is not specified, the failure manager retries immediately after the failure. If the ``max_retries`` is omitted, the failure manager allows for unbounded retries. Regarding the latter, use caution; if the step application encounters a deterministic failure (e.g., a configuration error), the failure manager will retry indefinitely.

.. jsonschema:: https://streamflow.di.unito.it/schemas/recovery/rollback_failure_manager.json
    :lift_description: true

