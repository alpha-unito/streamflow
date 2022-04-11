from __future__ import annotations

import asyncio
import logging
import posixpath
from abc import ABC, abstractmethod
from asyncio import CancelledError, FIRST_COMPLETED, Task
from typing import cast, MutableSequence, Optional, MutableMapping, AsyncIterable, Set, Any

from streamflow.core import utils
from streamflow.core.deployment import Target, DeploymentConfig
from streamflow.core.exception import FailureHandlingException, WorkflowDefinitionException, \
    WorkflowExecutionException, WorkflowException
from streamflow.core.scheduling import HardwareRequirement
from streamflow.core.utils import random_name
from streamflow.core.workflow import Step, Job, Token, CommandOutput, Status, \
    Workflow, Command, Port, CommandOutputProcessor
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.token import TerminationToken, ListToken


def _get_step_status(statuses: MutableSequence[Status]):
    num_skipped = 0
    for status in statuses:
        if status == Status.FAILED:
            return Status.FAILED
        elif status == Status.CANCELLED:
            return Status.CANCELLED
        elif status == Status.SKIPPED:
            num_skipped += 1
    if num_skipped == len(statuses):
        return Status.SKIPPED
    else:
        return Status.COMPLETED


def _group_by_tag(inputs: MutableMapping[str, Token],
                  inputs_map: MutableMapping[str, MutableMapping[str, Token]]) -> None:
    for name, token in inputs.items():
        if token.tag not in inputs_map:
            inputs_map[token.tag] = {}
        inputs_map[token.tag][name] = token


class BaseStep(Step, ABC):

    def __init__(self,
                 name: str,
                 workflow: Workflow):
        super().__init__(name, workflow)
        self._log_level: int = logging.DEBUG

    async def _get_inputs(self,
                          input_ports: MutableMapping[str, Port]):
        return {k: v for k, v in zip(input_ports, await asyncio.gather(*(
            asyncio.create_task(p.get(posixpath.join(self.name, port_name)))
            for port_name, p in input_ports.items())))}

    def terminate(self, status: Status):
        if not self.terminated:
            # If not explicitly cancelled, close input ports
            if status != Status.CANCELLED:
                for port_name, port in self.get_input_ports().items():
                    port.close(posixpath.join(self.name, port_name))
            # Add a TerminationToken to each output port
            for port in self.get_output_ports().values():
                port.put(TerminationToken())
            self._set_status(status)
            self.terminated = True
            logger.log(self._log_level, "Step {name} terminated with status {status}".format(
                name=self.name, status=status.name))


class Combinator(ABC):

    def __init__(self,
                 name: str,
                 workflow: Workflow):
        self.name: str = name
        self.workflow: Workflow = workflow
        self.items: MutableSequence[str] = []
        self.combinators: MutableMapping[str, Combinator] = {}
        self.combinators_map: MutableMapping[str, str] = {}

    def add_combinator(self, combinator: Combinator, items: Set[str]) -> None:
        self.combinators[combinator.name] = combinator
        self.items.append(combinator.name)
        self.combinators_map = {**self.combinators_map, **{p: combinator.name for p in items}}

    def add_item(self, item: str) -> None:
        self.items.append(item)

    def get_combinator(self, item: str) -> Optional[Combinator]:
        return self.combinators.get(self.combinators_map.get(item, ''))

    def get_items(self, recursive: bool = False) -> Set[str]:
        items = set(self.items)
        if recursive:
            for combinator in self.combinators.values():
                items.update(combinator.get_items(recursive))
        return items

    @abstractmethod
    async def combine(self,
                      port_name: str,
                      token: Token) -> AsyncIterable[MutableMapping[str, Token]]:
        ...


class CombinatorStep(BaseStep):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 combinator: Combinator):
        super().__init__(name, workflow)
        self.combinator: Combinator = combinator

    async def run(self):
        # Set default status to SKIPPED
        status = Status.SKIPPED
        if self.input_ports:
            input_tasks, terminated = [], []
            for port_name, port in self.get_input_ports().items():
                input_tasks.append(asyncio.create_task(
                    port.get(posixpath.join(self.name, port_name)), name=port_name))
            while input_tasks:
                # Wait for the next token
                finished, unfinished = await asyncio.wait(input_tasks, return_when=FIRST_COMPLETED)
                input_tasks = list(unfinished)
                for task in finished:
                    task_name = cast(Task, task).get_name()
                    token = task.result()
                    # If a TerminationToken is received, the corresponding port terminated its outputs
                    if utils.check_termination(token):
                        terminated.append(task_name)
                    # Otherwise, build combination and set default status to COMPLETED
                    else:
                        status = Status.COMPLETED
                        async for schema in cast(AsyncIterable, self.combinator.combine(task_name, token)):
                            for port_name, token in schema.items():
                                self.get_output_port(port_name).put(token)
                    # Create a new task in place of the completed one if the port is not terminated
                    if task_name not in terminated:
                        input_tasks.append(asyncio.create_task(
                            self.get_input_ports()[task_name].get(
                                posixpath.join(self.name, task_name)), name=task_name))
        # Terminate step
        self.terminate(status)


class ConditionalStep(BaseStep):

    def __init__(self,
                 name: str,
                 workflow: Workflow):
        super().__init__(name, workflow)

    @abstractmethod
    async def _eval(self, inputs: MutableMapping[str, Token]):
        ...

    @abstractmethod
    async def _on_true(self, inputs: MutableMapping[str, Token]):
        ...

    @abstractmethod
    async def _on_false(self, inputs: MutableMapping[str, Token]):
        ...

    async def run(self):
        try:
            if self.input_ports:
                inputs_map = {}
                while True:
                    # Retrieve input tokens
                    inputs = await self._get_inputs(self.get_input_ports())
                    # Check for termination
                    if utils.check_termination(inputs.values()):
                        break
                    # Group inputs by tag
                    _group_by_tag(inputs, inputs_map)
                    # Process tags
                    for tag in list(inputs_map.keys()):
                        if len(inputs_map[tag]) == len(self.input_ports):
                            inputs = inputs_map.pop(tag)
                            # If condition is satisfied (or null)
                            if await self._eval(inputs):
                                await self._on_true(inputs)
                            # Otherwise
                            else:
                                await self._on_false(inputs)
            else:
                # If condition is satisfied (or null)
                if await self._eval({}):
                    await self._on_true({})
                # Otherwise
                else:
                    await self._on_false({})
            self.terminate(Status.COMPLETED)
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Cancelled
        except CancelledError:
            self.terminate(Status.CANCELLED)
        # When receiving a WorkflowException, simply print the error
        except WorkflowException as e:
            logger.error(e)
            self.terminate(Status.FAILED)
        except BaseException as e:
            logger.exception(e)
            self.terminate(Status.FAILED)


class DefaultCommandOutputProcessor(CommandOutputProcessor):

    async def process(self, job: Job, command_output: CommandOutput) -> Optional[Token]:
        return Token(
            tag=utils.get_tag(job.inputs.values()),
            value=command_output.value)


class DeployStep(BaseStep):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 deployment_config: DeploymentConfig):
        super().__init__(name, workflow)
        self.deployment_config: DeploymentConfig = deployment_config
        self.add_output_port(deployment_config.name, workflow.create_port(cls=ConnectorPort))

    def add_output_port(self, name: str, port: ConnectorPort) -> None:
        if not self.output_ports or port.name in self.output_ports:
            super().add_output_port(name, port)
        else:
            raise WorkflowDefinitionException("Deploy step must contain a single output port.")

    def get_output_port(self, name: Optional[str] = None) -> ConnectorPort:
        return cast(ConnectorPort, super().get_output_port(name))

    async def run(self):
        if len(self.output_ports) != 1:
            raise WorkflowDefinitionException("Scatter step must contain a single output port.")
        try:
            if self.input_ports:
                inputs_map = {}
                while True:
                    # Wait for input tokens to be available
                    inputs = await self._get_inputs(self.get_input_ports())
                    # Check for termination
                    if utils.check_termination(inputs.values()):
                        break
                    # Group inputs by tag
                    _group_by_tag(inputs, inputs_map)
                    # Process tags
                    for tag in list(inputs_map.keys()):
                        if len(inputs_map[tag]) == len(self.input_ports):
                            inputs_map.pop(tag)
                            # Deploy the target
                            await self.workflow.context.deployment_manager.deploy(self.deployment_config)
                            # Propagate the connector in the output port
                            self.get_output_port().put(Token(
                                value=self.workflow.context.deployment_manager.get_connector(
                                    self.deployment_config.name)))
            else:
                # Deploy the target
                await self.workflow.context.deployment_manager.deploy(self.deployment_config)
                # Propagate the connector in the output port
                self.get_output_port().put_connector(self.deployment_config.name)
            self.terminate(Status.COMPLETED)
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Cancelled
        except CancelledError:
            self.terminate(Status.CANCELLED)
        # When reseiving a WorkflowException, simply print the error
        except WorkflowException as e:
            logger.error(e)
            self.terminate(Status.FAILED)
        except BaseException as e:
            logger.exception(e)
            self.terminate(Status.FAILED)


class ExecuteStep(BaseStep):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 job_port: JobPort):
        super().__init__(name, workflow)
        self._log_level: int = logging.INFO
        self.command: Optional[Command] = None
        self.output_processors: MutableMapping[str, CommandOutputProcessor] = {}
        self.add_input_port("__job__", job_port)

    async def _retrieve_output(self,
                               job: Job,
                               output_name: str,
                               output_port: Port,
                               command_output: CommandOutput) -> None:
        if (token := await self.output_processors[output_name].process(job, command_output)) is not None:
            output_port.put(token)

    async def _run_job(self, inputs: MutableMapping[str, Token]) -> Status:
        # Update job
        job = await cast(JobPort, self.get_input_port("__job__")).get_job(self.name)
        if job is None:
            raise WorkflowExecutionException("Step {} received a null job".format(self.name))
        job = Job(
            name=job.name,
            inputs=inputs,
            input_directory=job.input_directory,
            output_directory=job.output_directory,
            tmp_directory=job.tmp_directory)
        logger.debug("Job {name} started".format(name=job.name))
        # Initialise command output with defualt values
        command_output = CommandOutput(value=None, status=Status.FAILED)
        try:
            # Execute job
            if not self.terminated:
                self.status = Status.RUNNING
            await self.workflow.context.scheduler.notify_status(self.name, Status.RUNNING)
            command_output = await self.command.execute(job)
            if command_output.status == Status.FAILED:
                logger.error("Job {name} failed {error}".format(
                    name=job.name,
                    error="with error:\n\t{error}".format(error=command_output.value)))
                command_output = await self.workflow.context.failure_manager.handle_failure(job, self, command_output)
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Cancelled
        except CancelledError:
            command_output.status = Status.CANCELLED
            self.terminate(command_output.status)
        # When receiving a FailureHandling exception, mark the step as Failed
        except FailureHandlingException:
            command_output.status = Status.FAILED
            self.terminate(command_output.status)
        # When receiving a generic exception, try to handle it
        except BaseException as e:
            # When receiving a WorkflowException, simply print the error
            if isinstance(e, WorkflowException):
                logger.error(e)
            else:
                logger.exception(e)
            try:
                command_output = await self.workflow.context.failure_manager.handle_exception(job, self, e)
            # If failure cannot be recovered, simply fail
            except BaseException as ie:
                if ie != e:
                    if isinstance(ie, WorkflowException):
                        logger.error(ie)
                    else:
                        logger.exception(ie)
                command_output.status = Status.FAILED
                self.terminate(command_output.status)
        finally:
            # Notify completion to scheduler
            await self.workflow.context.scheduler.notify_status(job.name, command_output.status)
        # Retrieve output tokens
        if not self.terminated:
            try:
                await asyncio.gather(*(asyncio.create_task(
                    self._retrieve_output(job, output_name, self.workflow.ports[output_port], command_output)
                ) for output_name, output_port in self.output_ports.items()))
            except BaseException as e:
                logger.exception(e)
                command_output.status = Status.FAILED
        # Return job status
        logger.debug("Job {name} terminated with status {status}".format(
            name=job.name, status=command_output.status.name))
        return command_output.status

    def add_output_port(self,
                        name: str,
                        port: Port,
                        output_processor: CommandOutputProcessor = None) -> None:
        super().add_output_port(name, port)
        self.output_processors[name] = output_processor or DefaultCommandOutputProcessor(name, self.workflow)

    async def run(self) -> None:
        jobs = []
        # If there are input ports create jobs until termination token are received
        input_ports = {k: v for k, v in self.get_input_ports().items() if k != "__job__"}
        if input_ports:
            inputs_map = {}
            while True:
                # Retrieve input tokens
                inputs = await self._get_inputs(input_ports)
                # Check for termination
                if utils.check_termination(inputs.values()):
                    break
                # Group inputs by tag
                _group_by_tag(inputs, inputs_map)
                # Process tags
                for tag in list(inputs_map.keys()):
                    if len(inputs_map[tag]) == len(input_ports):
                        inputs = inputs_map.pop(tag)
                        # Set status to fireable
                        self._set_status(Status.FIREABLE)
                        # Run job
                        jobs.append(asyncio.create_task(
                            self._run_job(inputs),
                            name=utils.random_name()))
        # Otherwise simply run job
        else:
            jobs.append(asyncio.create_task(
                self._run_job({}),
                name=utils.random_name()))
        # Wait for jobs termination
        statuses = cast(MutableSequence[Status], await asyncio.gather(*jobs))
        # Terminate step
        self.terminate(_get_step_status(statuses))


class GatherStep(BaseStep):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 depth: int = 1):
        super().__init__(name, workflow)
        self.depth: int = depth
        self.token_map: MutableMapping[str, MutableSequence[Token]] = {}


    def add_input_port(self, name: str, port: Port) -> None:
        if not self.input_ports or name in self.input_ports:
            super().add_input_port(name, port)
        else:
            raise WorkflowDefinitionException("Gather step must contain a single input port.")

    def add_output_port(self, name: str, port: Port) -> None:
        if not self.output_ports or port.name in self.output_ports:
            super().add_output_port(name, port)
        else:
            raise WorkflowDefinitionException("Gather step must contain a single output port.")

    async def run(self):
        if len(self.input_ports) != 1:
            raise WorkflowDefinitionException("Gather step must contain a single input port.")
        if len(self.output_ports) != 1:
            raise WorkflowDefinitionException("Gather step must contain a single output port.")
        input_port = self.get_input_port()
        while True:
            token = await input_port.get(posixpath.join(self.name, next(iter(self.input_ports))))
            if isinstance(token, TerminationToken):
                output_port = self.get_output_port()
                for tag, tokens in self.token_map.items():
                    output_port.put(ListToken(
                        tag=tag,
                        value=sorted(tokens, key=lambda cur: cur.tag)))
                break
            else:
                key = '.'.join(token.tag.split('.')[:-self.depth])
                if key not in self.token_map:
                    self.token_map[key] = []
                self.token_map[key].append(token)
        # Terminate step
        self.terminate(Status.SKIPPED if self.get_output_port().empty() else Status.COMPLETED)


class InputInjectorStep(BaseStep, ABC):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 job_port: JobPort):
        super().__init__(name, workflow)
        self.add_input_port("__job__", job_port)

    def add_output_port(self, name: str, port: Port) -> None:
        if not self.output_ports or port.name in self.output_ports:
            super().add_output_port(name, port)
        else:
            raise WorkflowDefinitionException("{} step must contain a single output port.".format(self.name))

    @abstractmethod
    async def process_input(self,
                            job: Job,
                            token_value: Any) -> Token:
        ...

    async def run(self):
        input_ports = {k: v for k, v in self.get_input_ports().items() if k != "__job__"}
        if len(input_ports) != 1:
            raise WorkflowDefinitionException("{} step must contain a single input port.".format(self.name))
        if len(self.output_ports) != 1:
            raise WorkflowDefinitionException("{} step must contain a single output port.".format(self.name))
        if input_ports:
            while True:
                # Retrieve input token
                token = next(iter((await self._get_inputs(input_ports)).values()))
                # Check for termination
                if utils.check_termination(token):
                    break
                # Retrieve job
                job = await cast(JobPort, self.get_input_port("__job__")).get_job(self.name)
                if job is None:
                    raise WorkflowExecutionException("Step {} received a null job".format(self.name))
                # Process value and inject token in the output port
                self.get_output_port().put(await self.process_input(job, token.value))
        # Terminate step
        self.terminate(Status.SKIPPED if self.get_output_port().empty() else Status.COMPLETED)


class ScheduleStep(BaseStep):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 target: Target,
                 connector_port: ConnectorPort,
                 hardware_requirement: Optional[HardwareRequirement] = None,
                 input_directory: Optional[str] = None,
                 output_directory: Optional[str] = None,
                 tmp_directory: Optional[str] = None):
        super().__init__(name, workflow)
        self.target: Target = target
        self.hardware_requirement: Optional[HardwareRequirement] = hardware_requirement
        self.input_directory: Optional[str] = input_directory
        self.output_directory: Optional[str] = output_directory
        self.tmp_directory: Optional[str] = tmp_directory
        self.add_input_port("__connector__", connector_port)
        self.add_output_port("__job__", workflow.create_port(cls=JobPort))

    def _get_directory(self,
                       path_processor,
                       directory: Optional[str]):
        return directory or path_processor.join(self.target.workdir, utils.random_name())

    def get_output_port(self, name: Optional[str] = None) -> JobPort:
        return cast(JobPort, super().get_output_port(name))

    async def run(self):
        try:
            # Retrieve connector
            connector = await cast(ConnectorPort, self.get_input_port("__connector__")).get_connector(self.name)
            path_processor = utils.get_path_processor(connector)
            # If there are input ports
            input_ports = {k: v for k, v in self.get_input_ports().items() if k != "__connector__"}
            if input_ports:
                inputs_map = {}
                while True:
                    # Retrieve input tokens
                    inputs = await self._get_inputs(input_ports)
                    # Check for termination
                    if utils.check_termination(inputs.values()):
                        break
                    # Group inputs by tag
                    _group_by_tag(inputs, inputs_map)
                    # Process tags
                    for tag in list(inputs_map.keys()):
                        if len(inputs_map[tag]) == len(input_ports):
                            inputs = inputs_map.pop(tag)
                            # Create Job
                            job = Job(
                                name=random_name(),
                                inputs=inputs,
                                input_directory=self._get_directory(path_processor, self.input_directory),
                                output_directory=self._get_directory(path_processor, self.output_directory),
                                tmp_directory=self._get_directory(path_processor, self.tmp_directory))
                            # Schedule
                            hardware_requirement = (self.hardware_requirement.eval(inputs)
                                                    if self.hardware_requirement else None)
                            await self.workflow.context.scheduler.schedule(job, self.target, hardware_requirement)
                            locations = self.workflow.context.scheduler.get_locations(job.name)
                            await remotepath.mkdirs(
                                connector=connector,
                                locations=locations,
                                paths=[job.input_directory, job.output_directory, job.tmp_directory])
                            # Propagate job
                            self.get_output_port().put_job(job)
            else:
                # Create Job
                job = Job(
                    name=random_name(),
                    inputs={},
                    input_directory=self._get_directory(path_processor, self.input_directory),
                    output_directory=self._get_directory(path_processor, self.output_directory),
                    tmp_directory=self._get_directory(path_processor, self.tmp_directory))
                # Schedule
                await self.workflow.context.scheduler.schedule(
                    job, self.target, self.hardware_requirement.eval({}) if self.hardware_requirement else None)
                locations = self.workflow.context.scheduler.get_locations(job.name)
                # Create directories
                await remotepath.mkdirs(
                    connector=connector,
                    locations=locations,
                    paths=[job.input_directory, job.output_directory, job.tmp_directory])
                # Propagate job
                self.get_output_port().put_job(job)
            self.terminate(Status.SKIPPED if self.get_output_port().empty() else Status.COMPLETED)
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Cancelled
        except CancelledError:
            self.terminate(Status.CANCELLED)
        # When receiving a WorkflowException, simply print the error
        except WorkflowException as e:
            logger.error(e)
            self.terminate(Status.FAILED)
        except BaseException as e:
            logger.exception(e)
            self.terminate(Status.FAILED)


class ScatterStep(BaseStep):

    def _scatter(self, token: Token):
        if isinstance(token.value, Token):
            self._scatter(token.value)
        elif isinstance(token, ListToken):
            output_port = self.get_output_port()
            for i, t in enumerate(token.value):
                output_port.put(t.retag(token.tag + '.' + str(i)))
        else:
            raise WorkflowDefinitionException("Scatter ports require iterable inputs")

    def add_input_port(self, name: str, port: Port) -> None:
        if not self.input_ports:
            super().add_input_port(name, port)
        else:
            raise WorkflowDefinitionException("Scatter step must contain a single input port.")

    def add_output_port(self, name: str, port: Port) -> None:
        if not self.output_ports or port.name in self.output_ports:
            super().add_output_port(name, port)
        else:
            raise WorkflowDefinitionException("Scatter step must contain a single output port.")

    async def run(self):
        if len(self.input_ports) != 1:
            raise WorkflowDefinitionException("Scatter step must contain a single input port.")
        if len(self.output_ports) != 1:
            raise WorkflowDefinitionException("Scatter step must contain a single output port.")
        input_port = self.get_input_port()
        output_port = self.get_output_port()
        while True:
            token = await input_port.get(posixpath.join(self.name, next(iter(self.input_ports))))
            if isinstance(token, TerminationToken):
                break
            else:
                self._scatter(token)
        # Terminate step
        self.terminate(Status.SKIPPED if output_port.empty() else Status.COMPLETED)


class TransferStep(BaseStep, ABC):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 job_port: JobPort):
        super().__init__(name, workflow)
        self.add_input_port("__job__", job_port)

    async def run(self):
        # Set default status as SKIPPED
        status = Status.SKIPPED
        # Retrieve input ports
        input_ports = {k: v for k, v in self.get_input_ports().items() if k != "__job__"}
        if input_ports:
            inputs_map = {}
            try:
                while True:
                    # Retrieve input tokens
                    inputs = await self._get_inputs(input_ports)
                    # Check for termination
                    if utils.check_termination(inputs.values()):
                        break
                    # Group inputs by tag
                    _group_by_tag(inputs, inputs_map)
                    # Process tags
                    for tag in list(inputs_map.keys()):
                        if len(inputs_map[tag]) == len(input_ports):
                            inputs = inputs_map.pop(tag)
                            # Retrieve job
                            job = await cast(JobPort, self.get_input_port("__job__")).get_job(self.name)
                            if job is None:
                                raise WorkflowExecutionException("Step {} received a null job".format(self.name))
                            # Change default status to COMPLETED
                            status = Status.COMPLETED
                            # Transfer token
                            for port_name, token in inputs.items():
                                self.get_output_port(port_name).put(await self.transfer(job, token))
            # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
            except KeyboardInterrupt:
                raise
            # When receiving a CancelledError, mark the step as Cancelled
            except CancelledError:
                self.terminate(Status.CANCELLED)
            # When receiving a WorkflowException, simply print the error
            except WorkflowException as e:
                logger.error(e)
                self.terminate(Status.FAILED)
            except BaseException as e:
                logger.exception(e)
                self.terminate(Status.FAILED)
        # Terminate step
        self.terminate(status)

    @abstractmethod
    async def transfer(self, job: Job, token: Token) -> Token:
        ...


class Transformer(BaseStep, ABC):

    def __init__(self,
                 name: str,
                 workflow: Workflow):
        super().__init__(name, workflow)

    async def run(self):
        try:
            if self.input_ports:
                inputs_map = {}
                while True:
                    # Retrieve input tokens
                    inputs = await self._get_inputs(self.get_input_ports())
                    # Check for termination
                    if utils.check_termination(inputs.values()):
                        break
                    # Group inputs by tag
                    _group_by_tag(inputs, inputs_map)
                    # Process tags
                    for tag in list(inputs_map.keys()):
                        if len(inputs_map[tag]) == len(self.input_ports):
                            inputs = inputs_map.pop(tag)
                            # Apply transformation and propagate outputs
                            for port_name, token in (await self.transform(inputs)).items():
                                self.get_output_port(port_name).put(token)
            else:
                for port_name, token in (await self.transform({})).items():
                    self.get_output_port(port_name).put(token)
            # Terminate step
            self.terminate(Status.SKIPPED if any(p.empty() for p in self.get_output_ports().values())
                           else Status.COMPLETED)
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Cancelled
        except CancelledError:
            self.terminate(Status.CANCELLED)
        # When receiving a WorkflowException, simply print the error
        except WorkflowException as e:
            logger.error(e)
            self.terminate(Status.FAILED)
        except BaseException as e:
            logger.exception(e)
            self.terminate(Status.FAILED)

    @abstractmethod
    async def transform(self, inputs: MutableMapping[str, Token]) -> MutableMapping[str, Token]:
        ...
