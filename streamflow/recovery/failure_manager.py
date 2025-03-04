from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import MutableMapping, MutableSequence
from importlib.resources import files

from streamflow.core.command import CommandOutput
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import (
    FailureHandlingException,
)
from streamflow.core.recovery import FailureManager
from streamflow.core.workflow import Job, Status, Step, Token
from streamflow.log_handler import logger
from streamflow.recovery.recovery import (
    PortRecovery,
    RollbackRecoveryPolicy,
)
from streamflow.recovery.utils import _execute_recovered_workflow, _is_token_available
from streamflow.workflow.token import JobToken, TerminationToken


class RetryRequest:
    def __init__(self):
        self.version = 1
        self.job_token: JobToken | None = None
        self.token_output: MutableMapping[str, Token] = {}
        self.lock = asyncio.Lock()
        self.is_running = True
        # Other workflows can queue to the output port of the step while the job is running.
        self.queue: MutableSequence[PortRecovery] = []
        self.workflow = None


class DefaultFailureManager(FailureManager):
    def __init__(
        self,
        context: StreamFlowContext,
        max_retries: int | None = None,
        retry_delay: int | None = None,
    ):
        super().__init__(context)
        self.max_retries: int = max_retries
        self.retry_delay: int | None = retry_delay
        self.retry_requests: MutableMapping[str, RetryRequest] = {}

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("default_failure_manager.json")
            .read_text("utf-8")
        )

    async def is_running_token(self, token: Token, valid_data) -> bool:
        if (
            isinstance(token, JobToken)
            and token.value.name in self.retry_requests.keys()
        ):
            async with (self.retry_requests[token.value.name].lock):
                if self.retry_requests[token.value.name].is_running:
                    return True
                # elif self.retry_requests[token.value.name].token_output:
                #     tasks = [
                #         asyncio.create_task(
                #             _is_token_available(t, self.context, valid_data)
                #         )
                #         for t in self.retry_requests[
                #             token.value.name
                #         ].token_output.values()
                #     ]
                #     while tasks:
                #         finished, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                #         for task in finished:
                #             if not task.result():
                #                 for _task in tasks:
                #                     _task.cancel()
                #                 return False
                #     return True
                elif self.retry_requests[token.value.name].token_output and all(
                        await _is_token_available(t, self.context, valid_data)
                        for t in self.retry_requests[
                            token.value.name
                        ].token_output.values()
                ):
                    return True
        return False

    def _get_retry_request(self, job_name, default_is_running=True) -> RetryRequest:
        if job_name not in self.retry_requests.keys():
            request = RetryRequest()
            request.is_running = default_is_running
            return self.retry_requests.setdefault(job_name, request)
        return self.retry_requests[job_name]

    async def update_job_status(self, job_name):
        if (
            self.max_retries is None
            or self.retry_requests[job_name].version < self.max_retries
        ):
            self.retry_requests[job_name].version += 1
            logger.debug(
                f"Updated Job {job_name} at {self.retry_requests[job_name].version} times"
            )
            # free resources scheduler
            await self.context.scheduler.notify_status(job_name, Status.ROLLBACK)
        else:
            logger.error(
                f"FAILED Job {job_name} {self.retry_requests[job_name].version} times. Execution aborted"
            )
            raise FailureHandlingException(
                f"FAILED Job {job_name} {self.retry_requests[job_name].version} times. Execution aborted"
            )

    async def _recover_jobs(self, failed_job: Job, failed_step: Step):
        rrp = RollbackRecoveryPolicy(self.context)
        # Generate new workflow
        new_workflow, last_iteration = await rrp.recover_workflow(
            failed_job, failed_step
        )
        # Execute new workflow
        await _execute_recovered_workflow(
            new_workflow, failed_step.name, failed_step.output_ports
        )
        return new_workflow

    async def close(self): ...

    async def notify_jobs(self, job_token, out_port_name, token):
        job_name = job_token.value.name
        logger.debug(f"Notify end job {job_name}")
        if job_name in self.retry_requests.keys():
            async with self.retry_requests[job_name].lock:
                if self.retry_requests[job_name].job_token is None:
                    self.retry_requests[job_name].job_token = job_token
                if self.retry_requests[job_name].token_output is None:
                    self.retry_requests[job_name].token_output = {}
                self.retry_requests[job_name].token_output.setdefault(
                    out_port_name, token
                )

                # todo: fare a tutte le port nella queue la put del token
                elems = []
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Job {job_name} is notifying on port {out_port_name}. "
                        f"There are {len(self.retry_requests[job_name].queue)} workflows in waiting"
                    )
                if len(self.retry_requests[job_name].queue):
                    str_port = "".join(
                        [
                            (
                                f"\n\tQueue[{i}]:"
                                f" port {elem.port.name} (id {elem.port.persistent_id})"
                                f" the token_list {elem.port.token_list} "
                                f" of workflow {elem.port.workflow.name}."
                                f" This port waits other {elem.waiting_token} before to add TerminationToken"
                                if elem
                                else f"\n\t\tQueue[{i}]: Elem-None"
                            )
                            for i, elem in enumerate(
                                self.retry_requests[job_name].queue
                            )
                        ]
                    )
                    logger.debug(f"port in coda: {str_port}")

                for elem in self.retry_requests[job_name].queue:
                    if elem.port.name == out_port_name:
                        elem.port.put(token)
                        # todo: non è giusto, potrebbe dover aspettare altri token
                        elem.waiting_token -= 1
                        if elem.waiting_token == 0:
                            elems.append(elem)
                            elem.port.put(TerminationToken())
                        str_t = json.dumps(
                            {
                                "p.name": elem.port.name,
                                "p.id": elem.port.persistent_id,
                                "wf": elem.port.workflow.name,
                                "p.token_list_len": len(elem.port.token_list),
                                "p.queue": list(elem.port.queues.keys()),
                                "Ha ricevuto token": token.persistent_id,
                            },
                            indent=2,
                        )
                        msg_pt2 = (
                            f"Aspetta {elem.waiting_token} tokens prima di mettere il terminationtoken"
                            if elem.waiting_token
                            else "Mandato anche termination token"
                        )
                        logger.debug(
                            f"Token added into Port of another wf {str_t}. {msg_pt2}"
                        )

                for elem in elems:
                    self.retry_requests[job_name].queue.remove(elem)
                logger.debug(f"notify - job {job_name} is not running anymore")
                self.retry_requests[job_name].is_running = False
                logger.debug(f"Notify end job {job_name} - done")

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow = await self._recover_jobs(job, step)
            command_output = CommandOutput(
                value=None,
                status=(
                    new_workflow.steps[step.name].status
                    if new_workflow.steps.keys()
                    else Status.COMPLETED
                ),
            )
            # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # except WorkflowTransferException as e:
        #     logger.exception(e)
        #     logger.debug("WorkflowTransferException ma stavo gestendo execute job")
        #     raise
        except Exception as e:
            logger.exception(e)
            # return await self.handle_exception(job, step, e)
            raise
        return command_output

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {type(exception).__name__} failure for job {job.name}"
            )
        if job.name in self.retry_requests.keys():
            self.retry_requests[job.name].is_running = False
        return await self._do_handle_failure(job, step)

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Handling command failure for job {job.name}")

        if job.name in self.retry_requests.keys():
            self.retry_requests[job.name].is_running = False
        return await self._do_handle_failure(job, step)

    async def handle_failure_transfer(
        self, job: Job, step: Step, port_name: str
    ) -> Token:
        if job.name in self.retry_requests.keys():
            logger.info(
                f"handle_failure_transfer: job {job.name} is not running anymore"
            )
            async with self.retry_requests[job.name].lock:
                self.retry_requests[job.name].is_running = False
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow = await self._recover_jobs(job, step)
            token = next(
                iter(
                    new_workflow.steps[step.name].get_output_port(port_name).token_list
                )
            )
        # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # except (WorkflowTransferException, WorkflowExecutionException) as e:
        #     logger.exception(e)
        #     return await self.handle_failure_transfer(job, step, port_name)
        except Exception as e:
            logger.exception(e)
            raise e
        return token


class DummyFailureManager(FailureManager):

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("dummy_failure_manager.json")
            .read_text("utf-8")
        )

    async def close(self): ...

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.WARNING):
            logger.warning(
                f"Job {job.name} failure can not be recovered. Failure manager is not enabled."
            )
        raise exception

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.WARNING):
            logger.warning(
                f"Job {job.name} failure can not be recovered. Failure manager is not enabled."
            )
        raise FailureHandlingException(
            f"FAILED Job {job.name} with error:\n\t{command_output.value}"
        )

    async def notify_jobs(self, job_name, out_port_name, token): ...

    async def handle_failure_transfer(self, job: Job, step: Step, port_name: str):
        return None
