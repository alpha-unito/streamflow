from __future__ import annotations

import asyncio
import os
import posixpath
import tempfile
from asyncio import CancelledError
from typing import TYPE_CHECKING

from streamflow.core import utils
from streamflow.core.exception import WorkflowExecutionException, FailureHandlingException
from streamflow.core.workflow import Step, Job, Token, TerminationToken, CommandOutput, Status
from streamflow.data import remotepath
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.deployment import Connector
    from streamflow.core.workflow import OutputPort
    from typing import Optional, MutableSequence
    from typing_extensions import Text


def _get_step_status(statuses: MutableSequence[Status]):
    num_skipped = 0
    for status in statuses:
        if status == Status.FAILED:
            return Status.FAILED
        elif status == Status.SKIPPED:
            num_skipped += 1
    if num_skipped == len(statuses):
        return Status.SKIPPED
    else:
        return Status.COMPLETED


async def _retrieve_output(
        job: Job,
        output_port: OutputPort,
        command_output: CommandOutput) -> None:
    token = await output_port.token_processor.compute_token(job, command_output)
    if token is not None:
        output_port.put(token)


class BaseJob(Job):

    def _init_dir(self) -> Text:
        if self.step.target is not None:
            path_processor = posixpath
            workdir = self.step.workdir or path_processor.join('/tmp', 'streamflow')
        else:
            path_processor = os.path
            workdir = self.step.workdir or path_processor.join(tempfile.gettempdir(), 'streamflow')
        dir_path = path_processor.join(workdir, utils.random_name())
        return dir_path

    async def initialize(self):
        self.input_directory = self._init_dir() if self.input_directory is None else self.input_directory
        self.output_directory = self._init_dir() if self.output_directory is None else self.output_directory
        self.tmp_directory = self._init_dir() if self.tmp_directory is None else self.tmp_directory
        await remotepath.mkdirs(
            connector=self.step.get_connector(),
            targets=self.get_resources(),
            paths=[self.input_directory, self.output_directory, self.tmp_directory])

    async def run(self):
        # Initialise command output with defualt values
        command_output = CommandOutput(value=None, status=Status.FAILED)
        try:
            # Execute job
            if not self.step.terminated:
                self.step.status = Status.RUNNING
            if self.step.target is not None:
                await self.step.context.scheduler.notify_status(self.name, Status.RUNNING)
            command_output = await self.step.command.execute(self)
            if command_output.status == Status.FAILED:
                logger.error("Job {name} failed {error}".format(
                    name=self.name,
                    error="with error:\n\t{error}".format(error=command_output.value)))
                command_output = await self.step.context.failure_manager.handle_failure(self, command_output)
        # When receiving a CancelledError, propagate it to the step
        except CancelledError:
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException:
            command_output.status = Status.FAILED
        # When receiving any other exception, log it and try to recover
        except BaseException as e:
            logger.exception(e)
            try:
                command_output = await self.step.context.failure_manager.handle_exception(self, e)
            # If failure cannot be recovered, simply fail
            except BaseException as ie:
                if ie != e:
                    logger.exception(ie)
                command_output.status = Status.FAILED
        return command_output


class BaseStep(Step):

    async def _run_job(self, inputs: MutableSequence[Token]) -> Status:
        # Create job
        job = BaseJob(
            name=posixpath.join(self.name, asyncio.current_task().get_name()),
            step=self,
            inputs=inputs)
        logger.info("Job {name} created".format(name=job.name))
        # Initialise command output with defualt values
        command_output = CommandOutput(value=None, status=Status.FAILED)
        try:
            # Setup runtime environment
            if self.target is not None:
                await self.context.deployment_manager.deploy(self.target.model)
                await self.context.scheduler.schedule(job)
            # Initialize job
            await job.initialize()
            # Update tokens after target assignment
            job.inputs = await asyncio.gather(*[asyncio.create_task(
                self.input_ports[token.name].token_processor.update_token(job, token)
            ) for token in inputs])
            # Run job
            command_output = await job.run()
            if command_output.status == Status.FAILED:
                self.terminate(command_output.status)
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Skipped
        except CancelledError:
            command_output.status = Status.SKIPPED
            self.terminate(command_output.status)
        # When receiving a FailureHandling exception, mark the step as Failed
        except FailureHandlingException:
            command_output.status = Status.FAILED
            self.terminate(command_output.status)
        # When receiving a generic exception, try to handle it
        except BaseException as e:
            logger.exception(e)
            try:
                command_output = await self.context.failure_manager.handle_exception(job, e)
            # If failure cannot be recovered, simply fail
            except BaseException as ie:
                if ie != e:
                    logger.exception(ie)
                command_output.status = Status.FAILED
                self.terminate(command_output.status)
        finally:
            # Notify completion to scheduler
            if self.target is not None:
                await self.context.scheduler.notify_status(job.name, command_output.status)
        # Retrieve output tokens
        if not self.terminated:
            try:
                await asyncio.gather(*[asyncio.create_task(
                    _retrieve_output(job, output_port, command_output)
                ) for output_port in self.output_ports.values()])
            except BaseException as e:
                logger.exception(e)
                command_output.status = Status.FAILED
        # Return job status
        logger.info("Job {name} terminated with status {status}".format(
            name=job.name, status=command_output.status.name))
        return command_output.status

    def get_connector(self) -> Optional[Connector]:
        if self.target is not None:
            return self.context.deployment_manager.get_connector(self.target.model.name)
        else:
            return None

    async def run(self) -> None:
        jobs = []
        # If there are input ports create jobs until termination token are received
        if self.input_ports:
            if self.input_combinator is None:
                raise WorkflowExecutionException("No InputCombinator specified for step {step}".format(step=self.name))
            while True:
                # Retrieve input tokens
                inputs = await self.input_combinator.get()
                # Check for termination
                if utils.check_termination(inputs):
                    break
                # Set status to fireable
                self.status = Status.FIREABLE
                # Run job
                jobs.append(asyncio.create_task(
                    self._run_job(inputs),
                    name=utils.random_name()))
        # Otherwise simply run job
        else:
            jobs.append(asyncio.create_task(
                self._run_job([]),
                name=utils.random_name()))
        # Wait for jobs termination
        statuses = await asyncio.gather(*jobs)
        # Terminate step
        self.terminate(_get_step_status(statuses))

    def terminate(self, status: Status):
        if not self.terminated:
            # Add a TerminationToken to each output port
            for port in self.output_ports.values():
                port.put(TerminationToken(name=port.name))
            self.status = status
            self.terminated = True
            logger.info("Step {name} terminated with status {status}".format(name=self.name, status=status.name))
