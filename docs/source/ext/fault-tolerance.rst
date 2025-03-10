===============
Fault tolerance
===============

CheckpointManager
=================

WIP

FailureManager
==============

The ``FailureManager`` interface has the methods to handle the failure of a step.

.. code-block:: python

    async def close(self) -> None: ...

    def is_recoverable(self, token: Token) -> bool: ...

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> None: ...

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> None: ...

    async def notify(
        self,
        output_port: str,
        output_token: Token,
        recoverable: bool = True,
        job_token: JobToken | None = None,
    ) -> None: ...


The ``close`` method receives no input parameter and does not return anything. It frees stateful resources potentially allocated during the object’s lifetime, e.g., network or database connections.

The ``is_recoverable`` method takes as input a ``Token`` and returns ``True`` if the token can be reused instead of re-executing its step.

The ``handle_exception`` method takes as input a job that failed because an exception was thrown while the job was running.

The ``handle_failure`` method takes as input a job that failed because an invalid state occurred.

The ``notify`` method takes as input a token produced by a step, along with its relative port, and a variable which stores if the token is can be reused instead of re-executing its step.

Implementations
===============

=======     =========================================================
Type        Class
=======     =========================================================
default     streamflow.recovery.failure_manager.DefaultFailureManager
=======     =========================================================

In the ``DefaultFailureManager`` implementation, stores the tokens which are recoverable, i.e. the token can be reused instead of re-execute the step.