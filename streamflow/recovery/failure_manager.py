from __future__ import annotations

import json
import asyncio
import logging
from typing import MutableMapping, MutableSequence

from importlib_resources import files

from streamflow.core.context import StreamFlowContext
from streamflow.core.recovery import FailureManager
from streamflow.core.workflow import CommandOutput, Job, Status, Step, Token
from streamflow.core.exception import (
    FailureHandlingException,
    WorkflowTransferException,
    WorkflowExecutionException,
)

from streamflow.log_handler import logger
from streamflow.recovery.utils import _is_token_available, _execute_recovered_workflow
from streamflow.recovery.recovery import (
    RollbackRecoveryPolicy,
    PortRecovery,
)
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.token import TerminationToken, JobToken


async def _execute_transfer_step(failed_step, new_workflow, port_name):
    token_list = (
        new_workflow.steps[failed_step.name].get_output_port(port_name).token_list
    )
    if len(token_list) != 2:
        # raise FailureHandlingException(
        #     f"Step recovery {failed_step.name} did not generate the right number of tokens: {len(token_list)}"
        # )
        pass
    # if not isinstance(token_list[1], TerminationToken):
    # raise FailureHandlingException(
    #     f"Step recovery {failed_step.name} did not work well. It moved two tokens instead of one: {[t.persistent_id for t in token_list]}"
    # )
    # pass
    return token_list[0]


class JobRequest:
    def __init__(self):
        self.version = 1
        self.job_token: JobToken | None = None
        self.token_output: MutableMapping[str, Token] = {}
        self.lock = asyncio.Condition()
        self.is_running = True
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

        self.create_request_lock = asyncio.Condition()
        # { job.name : RequestJob }
        self.job_requests: MutableMapping[str, JobRequest] = {}

    async def is_running_token(self, token, valid_data):
        if isinstance(token, JobToken) and token.value.name in self.job_requests.keys():
            async with self.job_requests[token.value.name].lock:
                if self.job_requests[token.value.name].is_running:
                    return True
                elif self.job_requests[token.value.name].token_output and all(
                    [
                        await _is_token_available(t, self.context, valid_data)
                        for t in self.job_requests[
                            token.value.name
                        ].token_output.values()
                    ]
                ):
                    return True
        return False

    async def setup_job_request(self, job_name, default_is_running=True):
        if job_name not in self.job_requests.keys():
            async with self.create_request_lock:
                request = JobRequest()
                request.is_running = default_is_running
                return self.job_requests.setdefault(job_name, request)
        return self.job_requests[job_name]

    async def update_job_status(self, job_name, lock):
        if (
            self.max_retries is None
            or self.job_requests[job_name].version < self.max_retries
        ):
            self.job_requests[job_name].version += 1
            logger.debug(
                f"Updated Job {job_name} at {self.job_requests[job_name].version} times"
            )
            # free resources scheduler
            await self.context.scheduler.notify_status(job_name, Status.ROLLBACK)
            self.context.scheduler.deallocate_job(job_name, keep_job_allocation=True)
        else:
            logger.error(
                f"FAILED Job {job_name} {self.job_requests[job_name].version} times. Execution aborted"
            )
            raise FailureHandlingException()

    # todo: situazione problematica
    #  A -> B
    #  A -> C
    #  B -> C
    # A ha successo, B fallisce (cade ambiente), viene rieseguito A, in C che input di A arriva?
    # quello vecchio? quello vecchio e quello nuovo? In teoria solo quello vecchio, da gestire comunque?
    # oppure lasciamo che fallisce e poi il failure manager prende l'output nuovo di A?
    async def _recover_jobs(self, failed_job: Job, failed_step: Step):
        loading_context = DefaultDatabaseLoadingContext()
        rrp = RollbackRecoveryPolicy(self.context)
        # Generate new workflow
        new_workflow, last_iteration = await rrp.recover_workflow(
            failed_job, failed_step, loading_context
        )
        # Execute new workflow
        await _execute_recovered_workflow(
            new_workflow, failed_step.name, failed_step.output_ports
        )
        return new_workflow

    async def close(self):
        ...

    async def notify_jobs(self, job_token, out_port_name, token):
        job_name = job_token.value.name
        logger.info(f"Notify end job {job_name}")
        if job_name in self.job_requests.keys():
            async with self.job_requests[job_name].lock:
                if self.job_requests[job_name].job_token is None:
                    self.job_requests[job_name].job_token = job_token
                if self.job_requests[job_name].token_output is None:
                    self.job_requests[job_name].token_output = {}
                self.job_requests[job_name].token_output.setdefault(
                    out_port_name, token
                )

                # todo: fare a tutte le port nella queue la put del token
                elems = []
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Job {job_name} is notifing on port {out_port_name}. There are {len(self.job_requests[job_name].queue)} workflows in waiting"
                    )
                if len(self.job_requests[job_name].queue):
                    str_port = "".join(
                        [
                            f"\n\tHa trovato port_name {elem.port.name} port_id {elem.port.persistent_id} workflow {elem.port.workflow.name} token_list {elem.port.token_list} queues {elem.port.queues}. Waiting per {elem.waiting_token} prima del terminationtoken"
                            if elem
                            else "\n\t\tElem-None"
                            for elem in self.job_requests[job_name].queue
                        ]
                    )
                    logger.debug(f"port in coda: {str_port}")

                for elem in self.job_requests[job_name].queue:
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
                    self.job_requests[job_name].queue.remove(elem)
                logger.info(f"notify - job {job_name} is not running anymore")
                self.job_requests[job_name].is_running = False
                logger.info(f"Notify end job {job_name} - done")

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("default_failure_manager.json")
            .read_text("utf-8")
        )

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow = await self._recover_jobs(job, step)

            # debug
            # if new_workflow.steps.keys():
            #     async with self.job_requests[job.name].lock:
            #         new_job_token = get_job_token(
            #             job.name,
            #             new_workflow.steps[step.name]
            #             .get_input_port("__job__")
            #             .token_list,
            #         )
            #         if self.job_requests[job.name].job_token is None:
            #             raise FailureHandlingException(
            #                 f"Job {job.name} has not a job_token. In the workflow {new_workflow.name} has been found job_token {new_job_token.persistent_id}."
            #             )
            # new_job_token = None
            # if step.name in new_workflow.steps.keys():
            #     new_job_token = get_job_token_no_excep(
            #         job.name,
            #         new_workflow.steps[step.name].get_input_port("__job__").token_list,
            #     )

            command_output = CommandOutput(
                value=None,
                status=new_workflow.steps[step.name].status
                if new_workflow.steps.keys()
                else Status.COMPLETED,
            )
            # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except WorkflowTransferException as e:
            logger.exception(e)
            logger.debug("WorkflowTransferException ma stavo gestendo execute job")
            raise
        except Exception as e:
            logger.exception(e)
            return await self.handle_exception(job, step, e)
        return command_output

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {type(exception).__name__} failure for job {job.name}"
            )
        if job.name in self.job_requests.keys():
            logger.info(f"handle_exception: job {job.name} is not running anymore")
            async with self.job_requests[job.name].lock:
                self.job_requests[job.name].is_running = False
        return await self._do_handle_failure(job, step)

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Handling command failure for job {job.name}")

        if job.name in self.job_requests.keys():
            logger.info(f"handle_failure: job {job.name} is not running anymore")
            async with self.job_requests[job.name].lock:
                self.job_requests[job.name].is_running = False
        return await self._do_handle_failure(job, step)

    async def handle_failure_transfer(
        self, job: Job, step: Step, port_name: str
    ) -> Token:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {WorkflowTransferException.__name__} failure for job {job.name}"
            )
        if job.name in self.job_requests.keys():
            logger.info(
                f"handle_failure_transfer: job {job.name} is not running anymore"
            )
            async with self.job_requests[job.name].lock:
                self.job_requests[job.name].is_running = False
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow = await self._recover_jobs(job, step)
            token = await _execute_transfer_step(step, new_workflow, port_name)
        # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except (WorkflowTransferException, WorkflowExecutionException) as e:
            logger.exception(e)
            return await self.handle_failure_transfer(job, step, port_name)
        except Exception as e:
            logger.exception(e)
            raise e
        return token


class DummyFailureManager(FailureManager):
    async def close(self):
        ...

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("dummy_failure_manager.json")
            .read_text("utf-8")
        )

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        raise exception

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        return command_output

    async def notify_jobs(self, job_name, out_port_name, token):
        ...

    async def handle_failure_transfer(self, job: Job, step: Step, port_name: str):
        return None
