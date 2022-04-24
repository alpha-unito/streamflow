from __future__ import annotations

import asyncio
from asyncio import Condition
from typing import Optional, MutableMapping

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException, UnrecoverableTokenException, WorkflowException
from streamflow.core.recovery import FailureManager, ReplayRequest, ReplayResponse
from streamflow.core.workflow import Status, Job, CommandOutput, Token, Step
from streamflow.log_handler import logger
from streamflow.recovery.recovery import JobVersion


class DefaultFailureManager(FailureManager):

    def __init__(self,
                 context: StreamFlowContext,
                 max_retries: Optional[int] = None,
                 retry_delay: Optional[int] = None):
        super().__init__(context)
        self.jobs: MutableMapping[str, JobVersion] = {}
        self.max_retries: int = max_retries
        self.replay_cache: MutableMapping[str, ReplayResponse] = {}
        self.retry_delay: Optional[int] = retry_delay
        self.wait_queues: MutableMapping[str, Condition] = {}

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        if job.name not in self.jobs:
            self.jobs[job.name] = JobVersion(
                job=job,
                outputs=None,
                step=step,
                version=1)
        command_output = await self._replay_job(self.jobs[job.name])
        return command_output

    async def _replay_job(self, job_version: JobVersion) -> CommandOutput:
        job = job_version.job
        # Retry job execution until the max number of retries is reached
        if self.max_retries is None or self.jobs[job.name].version < self.max_retries:
            # Update version
            self.jobs[job.name].version += 1
            try:
                # Manage job rescheduling
                allocation = self.context.scheduler.get_allocation(job.name)
                connector = self.context.scheduler.get_connector(job.name)
                available_locations = await connector.get_available_locations(
                    service=allocation.target.service,
                    input_directory=job.input_directory,
                    output_directory=job.output_directory,
                    tmp_directory=job.tmp_directory)
                active_locations = self.context.scheduler.get_locations(job.name, [Status.RUNNING])
                # If some locations are dead, notify job failure and schedule it on new locations
                if not active_locations or not all(res in available_locations for res in active_locations):
                    if active_locations:
                        await self.context.scheduler.notify_status(job.name, Status.FAILED)
                    await self.context.scheduler.schedule(job, allocation.target, allocation.hardware)
                # Initialize directories
                await job.initialize()
                # Recover input tokens
                recovered_tokens = []
                for token_name, token in job.inputs.items():
                    token_processor = job.step.input_token_processors[token_name]
                    version = 0
                    while True:
                        try:
                            recovered_tokens.append(await token_processor.recover_token(job, locations, token))
                            break
                        except UnrecoverableTokenException as e:
                            version += 1
                            reply_response = await self.replay_job(
                                ReplayRequest(job.name, e.token.job, version))
                            input_port = job.step.workflow.ports[job.step.input_ports[token.name]]
                            recovered_token = reply_response.outputs[input_port.name]
                            token = await _replace_token(job, token_processor, token, recovered_token)
                            token.name = input_port.name
                job.inputs = recovered_tokens
                # Run job
                return await job.run()
            # When receiving a FailureHandlingException, simply fail
            except FailureHandlingException as e:
                logger.exception(e)
                raise
            # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
            except KeyboardInterrupt:
                raise
            # When receiving a WorkflowException, simply print the error
            except WorkflowException as e:
                logger.error(e)
                return await self.handle_exception(job, job_version.step, e)
            except BaseException as e:
                logger.exception(e)
                return await self.handle_exception(job, job_version.step, e)
        else:
            logger.error("Job {name} failed {version} times. Execution aborted".format(
                name=job.name, version=self.jobs[job.name].version))
            raise FailureHandlingException()

    async def handle_exception(self, job: Job, step: Step, exception: BaseException) -> CommandOutput:
        logger.info("Handling {exception} failure for job {job}".format(
            job=job.name, exception=type(exception).__name__))
        return await self._do_handle_failure(job, step)

    async def handle_failure(self, job: Job, step: Step, command_output: CommandOutput) -> CommandOutput:
        logger.info("Handling command failure for job {job}".format(job=job.name))
        return await self._do_handle_failure(job, step)

    def register_job(self, job: Job, step: Step, outputs: MutableMapping[str, Token]):
        self.jobs[job.name] = JobVersion(
            job=job,
            outputs=outputs,
            step=step,
            version=1)

    async def replay_job(self, replay_request: ReplayRequest) -> ReplayResponse:
        sender_job = replay_request.sender
        target_job = replay_request.target
        if target_job not in self.wait_queues:
            self.wait_queues[target_job] = Condition()
        wait_queue = self.wait_queues[target_job]
        async with wait_queue:
            if (target_job not in self.replay_cache or
                    self.replay_cache[target_job].version < replay_request.version):
                # Reschedule job
                logger.info("Rescheduling job {job}".format(job=target_job))
                command_output = CommandOutput(value=None, status=Status.FAILED)
                self.replay_cache[target_job] = ReplayResponse(
                    job=target_job,
                    outputs=None,
                    version=self.jobs[target_job].version + 1)
                try:
                    await self.context.scheduler.notify_status(sender_job, Status.WAITING)
                    command_output = await self._replay_job(self.jobs[target_job])
                finally:
                    await self.context.scheduler.notify_status(target_job, command_output.status)
                # Retrieve output
                output_ports = target_job.step.output_ports
                output_tasks = []
                for output_port in output_ports:
                    output_tasks.append(asyncio.create_task(
                        target_job.step.output_token_processors[output_port].compute_token(target_job, command_output)))
                self.replay_cache[target_job].outputs = {port.name: token for (port, token) in
                                                         zip(output_ports, await asyncio.gather(*output_tasks))}
                wait_queue.notify_all()
            elif self.replay_cache[target_job].outputs is None:
                # Wait for job completion
                await wait_queue.wait()
            return self.replay_cache[target_job]


class DummyFailureManager(FailureManager):

    async def handle_exception(self,
                               job: Job,
                               step: Step,
                               exception: BaseException) -> CommandOutput:
        raise exception

    async def handle_failure(self,
                             job: Job,
                             step: Step,
                             command_output: CommandOutput) -> CommandOutput:
        return command_output

    def register_job(self,
                     job: Job,
                     step: Step,
                     outputs: MutableMapping[str, Token]):
        pass
