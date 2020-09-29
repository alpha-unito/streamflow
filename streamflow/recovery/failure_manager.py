import asyncio
from asyncio import Condition
from typing import Optional, MutableMapping, MutableSequence

from typing_extensions import Text

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException, UnrecoverableTokenException
from streamflow.core.recovery import FailureManager, JobVersion, ReplayRequest, ReplayResponse
from streamflow.core.workflow import Status, Job, CommandOutput, Token, TokenProcessor
from streamflow.log_handler import logger
from streamflow.workflow.port import ListTokenProcessor, MapTokenProcessor


async def _replace_token(job: Job, token_processor: TokenProcessor, old_token: Token, new_token: Token):
    if isinstance(old_token.job, MutableSequence):
        token_value = []
        if isinstance(token_processor, ListTokenProcessor):
            for (t, tp) in zip(old_token.value, token_processor.processors):
                token_value.append(await _replace_token(job, tp, t, new_token))
        elif isinstance(token_processor, MapTokenProcessor):
            for t in old_token.value:
                token_value.append(await _replace_token(job, token_processor.processor, t, new_token))
        else:
            for t in old_token.value:
                token_value.append(await _replace_token(job, token_processor, t, new_token))
        return old_token.update(token_value)
    elif new_token.job == old_token.job:
        return await token_processor.update_token(job, new_token)
    else:
        return old_token


class DummyFailureManager(FailureManager):

    async def handle_exception(self, job: Job, exception: BaseException) -> CommandOutput:
        raise exception

    async def handle_failure(self, job: Job, command_output: CommandOutput) -> CommandOutput:
        return command_output

    async def replay_job(self, replay_request: ReplayRequest) -> JobVersion:
        pass


class DefaultFailureManager(FailureManager):

    def __init__(self,
                 context: StreamFlowContext,
                 max_retries: Optional[int] = None,
                 retry_delay: Optional[int] = None):
        super().__init__(context)
        self.jobs: MutableMapping[Text, JobVersion] = {}
        self.max_retries: int = max_retries
        self.replay_cache: MutableMapping[Text, ReplayResponse] = {}
        self.retry_delay: Optional[int] = retry_delay
        self.wait_queues: MutableMapping[Text, Condition] = {}

    async def _do_handle_failure(self, job: Job) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        if job.name not in self.jobs:
            self.jobs[job.name] = JobVersion(job.name)
        command_output = await self._replay_job(self.jobs[job.name])
        return command_output

    async def _replay_job(self, job_version: JobVersion) -> CommandOutput:
        job = self.context.scheduler.get_job(job_version.job)
        # Retry job execution until the max number of etries is reached
        if self.max_retries is None or self.jobs[job.name].version < self.max_retries:
            # Update version
            self.jobs[job.name].version += 1
            try:
                resources = job.get_resources() or [None]
                # If job must be executed remotely, manage job rescheduling
                if job.step.target is not None:
                    connector = job.step.get_connector()
                    available_resources = await connector.get_available_resources(job.step.target.service)
                    active_resources = job.get_resources([Status.RUNNING])
                    # If some resources are dead, notify job failure and schedule it on new resources
                    if not active_resources or not all(res in available_resources for res in active_resources):
                        if active_resources:
                            await self.context.scheduler.notify_status(job.name, Status.FAILED)
                        await self.context.scheduler.schedule(job)
                # Initialize directories
                await job.initialize()
                # Recover input tokens
                recovered_tokens = []
                for token in job.inputs:
                    token_processor = job.step.input_ports[token.name].token_processor
                    version = 0
                    while True:
                        try:
                            recovered_tokens.append(await token_processor.recover_token(job, resources, token))
                            break
                        except UnrecoverableTokenException as e:
                            version += 1
                            reply_response = await self.replay_job(
                                ReplayRequest(job.name, e.token.job, version))
                            input_port = job.step.input_ports[token.name]
                            recovered_token = reply_response.outputs[input_port.dependee.name]
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
            except BaseException as e:
                logger.exception(e)
                return await self.handle_exception(job, e)
        else:
            logger.error("Job {name} failed {version} times. Execution aborted".format(
                name=job.name, version=self.jobs[job.name].version))
            raise FailureHandlingException()

    async def handle_exception(self, job: Job, exception: BaseException) -> CommandOutput:
        logger.info("Handling {exception} failure for job {job}".format(
            job=job.name, exception=type(exception).__name__))
        return await self._do_handle_failure(job)

    async def handle_failure(self, job: Job, command_output: CommandOutput) -> CommandOutput:
        logger.info("Handling command failure for job {job}".format(job=job.name))
        return await self._do_handle_failure(job)

    async def replay_job(self, replay_request: ReplayRequest) -> ReplayResponse:
        sender_job = self.context.scheduler.get_job(replay_request.sender)
        target_job = self.context.scheduler.get_job(replay_request.target)
        if target_job.name not in self.jobs:
            self.jobs[target_job.name] = JobVersion(target_job.name, version=0)
        if target_job.name not in self.wait_queues:
            self.wait_queues[target_job.name] = Condition()
        wait_queue = self.wait_queues[target_job.name]
        async with wait_queue:
            if (target_job.name not in self.replay_cache or
                    self.replay_cache[target_job.name].version < replay_request.version):
                # Reschedule job
                logger.info("Rescheduling job {job}".format(job=target_job.name))
                command_output = CommandOutput(value=None, status=Status.FAILED)
                self.replay_cache[target_job.name] = ReplayResponse(
                    job=target_job.name,
                    outputs=None,
                    version=self.jobs[target_job.name].version + 1)
                try:
                    if sender_job.step.target is not None:
                        await self.context.scheduler.notify_status(sender_job.name, Status.WAITING)
                    command_output = await self._replay_job(self.jobs[target_job.name])
                finally:
                    if target_job.step.target is not None:
                        await self.context.scheduler.notify_status(target_job.name, command_output.status)
                # Retrieve output
                output_ports = target_job.step.output_ports.values()
                output_tasks = []
                for output_port in output_ports:
                    output_tasks.append(asyncio.create_task(
                        output_port.token_processor.compute_token(target_job, command_output)))
                self.replay_cache[target_job.name].outputs = {port.name: token for (port, token) in
                                                              zip(output_ports, await asyncio.gather(*output_tasks))}
                wait_queue.notify_all()
            elif self.replay_cache[target_job.name].outputs is None:
                # Wait for job completion
                await wait_queue.wait()
            return self.replay_cache[target_job.name]
