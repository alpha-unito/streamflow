from __future__ import annotations

import asyncio
import os
from asyncio import Condition
from typing import MutableMapping, Optional, cast

import pkg_resources

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Connector
from streamflow.core.exception import FailureHandlingException, UnrecoverableTokenException
from streamflow.core.recovery import FailureManager, ReplayRequest, ReplayResponse
from streamflow.core.workflow import CommandOutput, Job, Status, Step
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.recovery.recovery import JobVersion
from streamflow.workflow.step import ExecuteStep


async def _cleanup_dir(connector: Connector,
                       location: str,
                       directory: str) -> None:
    await remotepath.rm(connector, location, await remotepath.listdir(connector, location, directory))


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
                job=Job(
                    name=job.name,
                    inputs=dict(job.inputs),
                    input_directory=job.input_directory,
                    output_directory=job.output_directory,
                    tmp_directory=job.tmp_directory),
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
                locations = self.context.scheduler.get_locations(job.name)
                available_locations = await connector.get_available_locations(
                    service=allocation.target.service)
                active_locations = self.context.scheduler.get_locations(job.name, [Status.RUNNING])
                # If there are active locations, the job just failed
                if active_locations:
                    # If some locations are dead
                    if not all(res in available_locations for res in active_locations):
                        # Notify job failure
                        await self.context.scheduler.notify_status(job.name, Status.FAILED)
                        # Invalidate locations
                        for location in (set(active_locations) - set(available_locations)):
                            self.context.data_manager.invalidate_location(location, '/')
                        # TODO
                    # Otherwise, empty output and tmp folders and re-execute the job
                    cleanup_tasks = []
                    for location in locations:
                        for directory in [job.output_directory, job.tmp_directory]:
                            cleanup_tasks.append(asyncio.create_task(_cleanup_dir(connector, location, directory)))
                            self.context.data_manager.invalidate_location(location, directory)
                    await asyncio.gather(*cleanup_tasks)
                    # TODO: try to do this in a more general way
                    return await cast(ExecuteStep, job_version.step).command.execute(job)
                # Otherwise, the job already completed but must be rescheduled
                else:
                    # TODO
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
            # When receiving a FailureHandlingException, simply fail
            except FailureHandlingException as e:
                logger.exception(e)
                raise
            # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
            except KeyboardInterrupt:
                raise
            except BaseException as e:
                logger.exception(e)
                return await self.handle_exception(job, job_version.step, e)
        else:
            logger.error("Job {name} failed {version} times. Execution aborted".format(
                name=job.name, version=self.jobs[job.name].version))
            raise FailureHandlingException()

    async def close(self):
        pass

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'default_failure_manager.json'))

    async def handle_exception(self, job: Job, step: Step, exception: BaseException) -> CommandOutput:
        logger.info("Handling {exception} failure for job {job}".format(
            job=job.name, exception=type(exception).__name__))
        return await self._do_handle_failure(job, step)

    async def handle_failure(self, job: Job, step: Step, command_output: CommandOutput) -> CommandOutput:
        logger.info("Handling command failure for job {job}".format(job=job.name))
        return await self._do_handle_failure(job, step)

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

    async def close(self):
        ...

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'dummy_failure_manager.json'))

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
