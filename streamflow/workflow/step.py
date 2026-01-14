from __future__ import annotations

import asyncio
import logging
import posixpath
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import (
    AsyncIterable,
    MutableMapping,
    MutableSequence,
    MutableSet,
)
from functools import cmp_to_key
from types import ModuleType
from typing import Any, cast

from typing_extensions import Self

from streamflow.core import utils
from streamflow.core.config import BindingConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.core.deployment import (
    Connector,
    DeploymentConfig,
    ExecutionLocation,
    Target,
)
from streamflow.core.exception import (
    FailureHandlingException,
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.processor import CommandOutputProcessor
from streamflow.core.recovery import recoverable
from streamflow.core.scheduling import HardwareRequirement
from streamflow.core.utils import compare_tags, get_entity_ids
from streamflow.core.workflow import (
    Command,
    CommandOutput,
    Job,
    Port,
    Status,
    Step,
    Token,
    Workflow,
)
from streamflow.data.remotepath import StreamFlowPath
from streamflow.deployment.utils import get_path_processor
from streamflow.log_handler import logger
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.token import JobToken, ListToken, TerminationToken
from streamflow.workflow.utils import (
    check_iteration_termination,
    check_termination,
    get_job_token,
)


def _get_directory(
    path_processor: ModuleType, directory: str | None, target: Target
) -> str:
    return directory or path_processor.join(target.workdir, utils.random_name())


def _group_by_tag(
    inputs: MutableMapping[str, Token],
    inputs_map: MutableMapping[str, MutableMapping[str, Token]],
) -> None:
    for name, token in inputs.items():
        if token.tag not in inputs_map:
            inputs_map[token.tag] = {}
        inputs_map[token.tag][name] = token


def _is_parent_tag(tag: str, parent: str) -> bool:
    parent_idx = parent.split(".")
    return tag.split(".")[: len(parent_idx)] == parent_idx


def _reduce_statuses(statuses: MutableSequence[Status]) -> Status:
    num_skipped = 0
    for status in statuses:
        match status:
            case Status.FAILED:
                return Status.FAILED
            case Status.CANCELLED:
                return Status.CANCELLED
            case Status.SKIPPED:
                num_skipped += 1
    if num_skipped == len(statuses):
        return Status.SKIPPED
    else:
        return Status.COMPLETED


class BaseStep(Step, ABC):
    def __init__(self, name: str, workflow: Workflow):
        super().__init__(name, workflow)
        self._log_level: int = logging.DEBUG

    async def _get_inputs(
        self, input_ports: MutableMapping[str, Port]
    ) -> MutableMapping[str, Token]:
        inputs = {
            k: v
            for k, v in zip(
                input_ports.keys(),
                await asyncio.gather(
                    *(
                        asyncio.create_task(p.get(posixpath.join(self.name, port_name)))
                        for port_name, p in input_ports.items()
                    )
                ),
                strict=True,
            )
        }
        if logger.isEnabledFor(logging.DEBUG):
            if check_termination(inputs.values()):
                logger.debug(
                    f"Step {self.name} received termination token with Status {_reduce_statuses([t.value for t in inputs.values()]).name}"
                )
            else:
                logger.debug(
                    f"Step {self.name} received inputs {[t.tag for t in inputs.values()]}"
                )
        return inputs

    def _get_status(self, status: Status) -> Status:
        if status == Status.FAILED:
            return status
        elif any(p.empty() for p in self.get_output_ports().values()):
            return Status.SKIPPED
        else:
            return status

    async def _persist_token(
        self, token: Token, port: Port, input_token_ids: MutableSequence[int]
    ) -> Token:
        if token.persistent_id:
            raise WorkflowDefinitionException(
                f"Token already has an id: {token.persistent_id}"
            )
        await token.save(self.workflow.context, port_id=port.persistent_id)
        if input_token_ids:
            await self.workflow.context.database.add_provenance(
                inputs=input_token_ids, token=token.persistent_id
            )
        return token

    async def terminate(self, status: Status) -> None:
        if not self.terminated:
            self.terminated = True
            # If not explicitly cancelled, close input ports
            if status != Status.CANCELLED:
                for port_name, port in self.get_input_ports().items():
                    port.close(posixpath.join(self.name, port_name))
            # Add a TerminationToken to each output port
            for port in self.get_output_ports().values():
                port.put(TerminationToken(status))
            # Set termination status
            await self._set_status(status)
            logger.log(
                self._log_level,
                f"{status.name} Step {self.name}",
            )


class Combinator(ABC):
    def __init__(self, name: str, workflow: Workflow):
        self.name: str = name
        self.workflow: Workflow = workflow
        self.items: MutableSequence[str] = []
        self.combinators: MutableMapping[str, Combinator] = {}
        self.combinators_map: MutableMapping[str, str] = {}
        self._token_values: MutableMapping[
            str, MutableMapping[str, MutableSequence[Any]]
        ] = {}

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {
            "name": self.name,
            "combinators": {
                k: v
                for k, v in zip(
                    self.combinators.keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(comb.save(context))
                            for comb in self.combinators.values()
                        )
                    ),
                    strict=True,
                )
            },
            "combinators_map": self.combinators_map,
            "items": self.items,
            "workflow": self.workflow.persistent_id,
        }

    def add_combinator(self, combinator: Combinator, items: set[str]) -> None:
        self.combinators[combinator.name] = combinator
        self.items.append(combinator.name)
        self.combinators_map |= {p: combinator.name for p in items}

    def add_item(self, item: str) -> None:
        self.items.append(item)

    def get_combinator(self, item: str) -> Combinator | None:
        return self.combinators.get(self.combinators_map.get(item, ""))

    def get_items(self, recursive: bool = False) -> set[str]:
        items = set(self.items)
        if recursive:
            for combinator in self.combinators.values():
                items.update(combinator.get_items(recursive))
        return items

    @abstractmethod
    async def combine(
        self, port_name: str, token: Token
    ) -> AsyncIterable[MutableMapping[str, Token]]: ...

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        type_ = cast(Self, utils.get_class_from_name(row["type"]))
        combinator = await type_._load(context, row["params"], loading_context)
        combinator.items = row["params"]["items"]
        combinator.combinators_map = row["params"]["combinators_map"]
        combinator.combinators = dict(
            zip(
                row["params"]["combinators"].keys(),
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            Combinator.load(context, c, loading_context)
                        )
                        for c in row["params"]["combinators"].values()
                    )
                ),
                strict=True,
            )
        )
        return combinator

    async def save(self, context: StreamFlowContext) -> MutableMapping[str, Any]:
        return {
            "type": utils.get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }

    def _add_to_list(
        self,
        token: Token | MutableMapping[str, Token],
        port_name: str,
        depth: int = 0,
    ) -> None:
        tag = (
            utils.get_tag([t["token"] for t in token.values()])
            if isinstance(token, MutableMapping)
            else token.tag
        )
        if depth:
            tag = ".".join(tag.split(".")[:-depth])
        for key in list(self._token_values.keys()):
            if tag == key:
                continue
            elif _is_parent_tag(key, tag):
                self._add_to_port(token, self._token_values[key], port_name)
            elif _is_parent_tag(tag, key):
                if tag not in self._token_values:
                    self._token_values[tag] = {}
                for p in self._token_values[key]:
                    for t in self._token_values[key][p]:
                        self._add_to_port(t, self._token_values[tag], p)
        if tag not in self._token_values:
            self._token_values[tag] = {}
        self._add_to_port(token, self._token_values[tag], port_name)

    def _add_to_port(
        self,
        token: Token | MutableMapping[str, Token],
        tag_values: MutableMapping[str, MutableSequence[Any]],
        port_name: str,
    ) -> None:
        if port_name not in tag_values:
            tag_values[port_name] = deque()
        tag_values[port_name].append(token)


class CombinatorStep(BaseStep):
    def __init__(self, name: str, workflow: Workflow, combinator: Combinator):
        super().__init__(name, workflow)
        self.combinator: Combinator = combinator

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            combinator=await Combinator.load(
                context, row["params"]["combinator"], loading_context
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "combinator": await self.combinator.save(context)
        }

    async def run(self) -> None:
        # Set default status to SKIPPED
        status = Status.SKIPPED
        if self.input_ports:
            input_tasks, terminated = [], []
            for port_name, port in self.get_input_ports().items():
                input_tasks.append(
                    asyncio.create_task(
                        port.get(posixpath.join(self.name, port_name)), name=port_name
                    )
                )
            while input_tasks:
                # Wait for the next token
                finished, unfinished = await asyncio.wait(
                    input_tasks, return_when=asyncio.FIRST_COMPLETED
                )
                input_tasks = list(unfinished)
                for task in finished:
                    task_name = task.get_name()
                    token = task.result()
                    # If a TerminationToken is received, the corresponding port terminated its outputs
                    if check_termination(token):
                        status = _reduce_statuses([status, token.value])
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"Step {self.name} received termination token for port {task_name}"
                            )
                        terminated.append(task_name)
                    # Otherwise, build combination and set default status to COMPLETED
                    else:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"Step {self.name} received token {token.tag} on port {task_name}"
                            )
                        status = Status.COMPLETED
                        async for schema in cast(
                            AsyncIterable,
                            self.combinator.combine(task_name, token),
                        ):
                            ins = [id for t in schema.values() for id in t["input_ids"]]
                            for port_name, token in schema.items():
                                self.get_output_port(port_name).put(
                                    await self._persist_token(
                                        token=token["token"],
                                        port=self.get_output_port(port_name),
                                        input_token_ids=ins,
                                    )
                                )

                    # Create a new task in place of the completed one if the port is not terminated
                    if task_name not in terminated:
                        input_tasks.append(
                            asyncio.create_task(
                                self.get_input_ports()[task_name].get(
                                    posixpath.join(self.name, task_name)
                                ),
                                name=task_name,
                            )
                        )
        # Terminate step
        await self.terminate(self._get_status(status))


class ConditionalStep(BaseStep):
    def __init__(self, name: str, workflow: Workflow):
        super().__init__(name, workflow)

    @abstractmethod
    async def _eval(self, inputs: MutableMapping[str, Token]) -> bool: ...

    @abstractmethod
    async def _on_true(self, inputs: MutableMapping[str, Token]) -> None: ...

    @abstractmethod
    async def _on_false(self, inputs: MutableMapping[str, Token]) -> None: ...

    async def run(self) -> None:
        try:
            if self.input_ports:
                inputs_map = {}
                while True:
                    # Retrieve input tokens
                    inputs = await self._get_inputs(self.get_input_ports())
                    # Check for termination
                    if check_termination(inputs.values()):
                        status = _reduce_statuses([t.value for t in inputs.values()])
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
                status = Status.COMPLETED
            await self.terminate(self._get_status(status))
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Cancelled
        except asyncio.CancelledError:
            await self.terminate(Status.CANCELLED)
        except Exception as e:
            logger.exception(e)
            await self.terminate(Status.FAILED)


class DefaultCommandOutputProcessor(CommandOutputProcessor):
    async def process(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
        recoverable: bool = False,
    ) -> Token | None:
        return Token(
            tag=utils.get_tag(job.inputs.values()),
            value=(await command_output).value,
            recoverable=recoverable,
        )


class DeployStep(BaseStep):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        deployment_config: DeploymentConfig,
        connector_port: ConnectorPort | None = None,
    ):
        super().__init__(name, workflow)
        self.deployment_config: DeploymentConfig = deployment_config
        self.add_output_port(
            deployment_config.name,
            connector_port or workflow.create_port(cls=ConnectorPort),
        )

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        params = row["params"]
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            deployment_config=await loading_context.load_deployment(
                context, params["deployment_config"]
            ),
            connector_port=cast(
                ConnectorPort,
                await loading_context.load_port(context, params["connector_port"]),
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        await self.deployment_config.save(context)
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "deployment_config": self.deployment_config.persistent_id,
            "connector_port": self.get_output_port(
                self.deployment_config.name
            ).persistent_id,
        }

    def add_output_port(self, name: str, port: ConnectorPort) -> None:
        if not self.output_ports or port.name in self.output_ports:
            super().add_output_port(name, port)
        else:
            raise WorkflowDefinitionException(
                "Deploy step must contain a single output port."
            )

    def get_output_port(self, name: str | None = None) -> ConnectorPort:
        return cast(ConnectorPort, super().get_output_port(name))

    async def run(self) -> None:
        if len(self.output_ports) != 1:
            raise WorkflowDefinitionException(
                "Scatter step must contain a single output port."
            )
        try:
            if self.input_ports:
                inputs_map = {}
                while True:
                    # Wait for input tokens to be available
                    inputs = await self._get_inputs(self.get_input_ports())
                    # Check for termination
                    if check_termination(inputs.values()):
                        status = _reduce_statuses([t.value for t in inputs.values()])
                        break
                    # Group inputs by tag
                    _group_by_tag(inputs, inputs_map)
                    # Process tags
                    for tag in list(inputs_map.keys()):
                        if len(inputs_map[tag]) == len(self.input_ports):
                            inputs_map.pop(tag)
                            # Deploy the target
                            await self.workflow.context.deployment_manager.deploy(
                                self.deployment_config
                            )
                            # Propagate the connector in the output port
                            self.get_output_port().put(
                                await self._persist_token(
                                    token=Token(
                                        value=self.deployment_config.name,
                                        recoverable=True,
                                    ),
                                    port=self.get_output_port(),
                                    input_token_ids=get_entity_ids(inputs.values()),
                                )
                            )
            else:
                # Deploy the target
                await self.workflow.context.deployment_manager.deploy(
                    self.deployment_config
                )
                # Propagate the connector in the output port
                self.get_output_port().put(
                    await self._persist_token(
                        token=Token(
                            value=self.deployment_config.name, recoverable=True
                        ),
                        port=self.get_output_port(),
                        input_token_ids=[],
                    )
                )
                status = Status.COMPLETED
            await self.terminate(self._get_status(status))
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Cancelled
        except asyncio.CancelledError:
            await self.terminate(Status.CANCELLED)
        except Exception as e:
            logger.exception(e)
            await self.terminate(Status.FAILED)


class ExecuteStep(BaseStep):
    def __init__(self, name: str, workflow: Workflow, job_port: JobPort):
        super().__init__(name, workflow)
        self._log_level: int = logging.INFO
        self.command: Command | None = None
        self.output_connectors: MutableMapping[str, str] = {}
        self.output_processors: MutableMapping[str, CommandOutputProcessor] = {}
        self.add_input_port("__job__", job_port)

    async def _check_inputs(
        self,
        inputs: MutableMapping[str, Token],
        input_ports: MutableMapping[str, Port],
        inputs_map: MutableMapping[str, MutableMapping[str, Token]],
        connectors: MutableMapping[str, Connector],
        unfinished: MutableSet[asyncio.Task[MutableMapping[str, Token] | Status]],
    ) -> None:
        if (
            job := await cast(JobPort, self.get_input_port("__job__")).get_job(
                self.name
            )
        ) is not None:
            # Group inputs by tag
            _group_by_tag(inputs, inputs_map)
            # Process tags
            for tag in list(inputs_map.keys()):
                if len(inputs_map[tag]) == len(input_ports):
                    inputs = inputs_map.pop(tag)
                    # Set status to fireable
                    await self._set_status(Status.FIREABLE)
                    # Run job
                    unfinished.add(
                        asyncio.create_task(
                            self._run_job(job, inputs, connectors),
                            name=job.name,
                        )
                    )
            unfinished.add(
                asyncio.create_task(
                    self._get_inputs(input_ports), name="retrieve_inputs"
                )
            )

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        params = row["params"]
        step = cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            job_port=cast(
                JobPort, await loading_context.load_port(context, params["job_port"])
            ),
        )
        step.output_connectors = params["output_connectors"]
        step.output_processors = {
            k: v
            for k, v in zip(
                params["output_processors"].keys(),
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            CommandOutputProcessor.load(context, p, loading_context)
                        )
                        for p in params["output_processors"].values()
                    )
                ),
                strict=True,
            )
        }
        if params["command"]:
            step.command = await Command.load(
                context, params["command"], loading_context, step
            )
        return step

    async def _retrieve_output(
        self,
        job: Job,
        output_name: str,
        output_port: Port,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
    ) -> None:
        if (
            token := await self.output_processors[output_name].process(
                job, command_output, connector, recoverable=True
            )
        ) is not None:
            job_token = get_job_token(
                job.name, self.get_input_port("__job__").token_list
            )
            output_port.put(
                await self._persist_token(
                    token=token,
                    port=output_port,
                    input_token_ids=get_entity_ids((*job.inputs.values(), job_token)),
                )
            )
            await self.workflow.context.failure_manager.notify(
                output_port.name,
                token,
                job_token,
            )

    @recoverable
    async def _execute_command(
        self, job: Job, connectors: MutableMapping[str, Connector]
    ) -> None:
        command_task = asyncio.create_task(self.command.execute(job))
        output_tasks = (
            asyncio.create_task(
                self._retrieve_output(
                    job=job,
                    output_name=output_name,
                    output_port=self.workflow.ports[output_port],
                    command_output=command_task,
                    connector=connectors.get(output_name),
                )
            )
            for output_name, output_port in self.output_ports.items()
        )
        if (
            command_output := await command_task
        ).status == Status.COMPLETED and not self.terminated:
            await asyncio.gather(*output_tasks)
        else:
            for output_task in output_tasks:
                output_task.cancel()
            if command_output.status == Status.FAILED:
                raise WorkflowExecutionException(
                    f"FAILED Job {job.name} with error:\n\t{command_output.value}"
                )

    async def _run_job(
        self,
        job: Job,
        inputs: MutableMapping[str, Token],
        connectors: MutableMapping[str, Connector],
    ) -> Status:
        # Update job
        job = Job(
            name=job.name,
            workflow_id=self.workflow.persistent_id,
            inputs=inputs,
            input_directory=job.input_directory,
            output_directory=job.output_directory,
            tmp_directory=job.tmp_directory,
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Job {job.name} started")
        job_status = Status.FAILED
        # TODO: Trigger location deployment in case of lazy environments
        try:
            # Execute job
            if not self.terminated:
                self.status = Status.RUNNING
            await self.workflow.context.scheduler.notify_status(
                job.name, Status.RUNNING
            )
            await self._execute_command(job, connectors)
            job_status = Status.COMPLETED
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Cancelled
        except asyncio.CancelledError:
            job_status = Status.CANCELLED
        # When receiving a FailureHandling exception, mark the step as Failed
        except FailureHandlingException as err:
            logger.error(err)
            job_status = Status.FAILED
        # When receiving a generic exception, mark the step as Failed
        except Exception as err:
            logger.error(err)
            job_status = Status.FAILED
        finally:
            # Notify completion to scheduler
            await self.workflow.context.scheduler.notify_status(job.name, job_status)
        # Return job status
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"{job_status.name} Job {job.name} terminated")
        return job_status

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "job_port": self.get_input_port("__job__").persistent_id,
            "output_connectors": self.output_connectors,
            "output_processors": {
                k: v
                for k, v in zip(
                    self.output_processors.keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(p.save(context))
                            for p in self.output_processors.values()
                        )
                    ),
                    strict=True,
                )
            },
            "command": await self.command.save(context) if self.command else None,
        }

    def add_output_port(
        self,
        name: str,
        port: Port,
        output_processor: CommandOutputProcessor | None = None,
    ) -> None:
        super().add_output_port(name, port)
        self.output_processors[name] = (
            output_processor or DefaultCommandOutputProcessor(name, self.workflow)
        )

    def get_job_port(self):
        return self.get_input_port("__job__")

    async def run(self) -> None:
        # If there are input connector ports, retrieve connectors
        connector_ports = {
            k: self.get_input_port(v)
            for k, v in self.output_connectors.items()
            if isinstance(self.get_input_port(v), ConnectorPort)
        }
        connectors = {
            k: v
            for k, v in zip(
                connector_ports.keys(),
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            cast(ConnectorPort, p).get_connector(
                                posixpath.join(self.name, port_name)
                            )
                        )
                        for port_name, p in connector_ports.items()
                    )
                ),
                strict=True,
            )
        }
        # If there are input ports create jobs until termination token are received
        if input_ports := {
            k: v
            for k, v in self.get_input_ports().items()
            if k != "__job__" and not isinstance(v, ConnectorPort)
        }:
            statuses = []
            inputs_map = {}
            unfinished = {
                asyncio.create_task(
                    self._get_inputs(input_ports), name="retrieve_inputs"
                )
            }
            while unfinished:
                finished, unfinished = await asyncio.wait(
                    unfinished, return_when=asyncio.FIRST_COMPLETED
                )
                for task in finished:
                    if task.cancelled():
                        continue
                    if task.get_name() == "retrieve_inputs":
                        inputs = task.result()
                        # Check for termination
                        if check_termination(inputs.values()):
                            statuses.append(
                                _reduce_statuses([t.value for t in inputs.values()])
                            )
                            if statuses[-1] in (Status.CANCELLED, Status.FAILED):
                                for t in unfinished:
                                    t.cancel()
                        else:
                            await self._check_inputs(
                                inputs,
                                input_ports,
                                inputs_map,
                                connectors,
                                unfinished,
                            )
                    else:
                        # check job exit status
                        job_status = task.result()
                        if job_status in (Status.CANCELLED, Status.FAILED):
                            for t in unfinished:
                                t.cancel()
                        statuses.append(job_status)
        # Otherwise simply run job
        else:
            # Retrieve job
            if (
                job := await cast(JobPort, self.get_input_port("__job__")).get_job(
                    self.name
                )
            ) is not None:
                statuses = [await self._run_job(job, {}, connectors)]
            else:
                statuses = [Status.SKIPPED]
        # If there are connector ports, retrieve termination tokens from them
        await asyncio.gather(
            *(
                asyncio.create_task(p.get(posixpath.join(self.name, port_name)))
                for port_name, p in connector_ports.items()
            )
        )
        # Terminate step
        await self.terminate(self._get_status(_reduce_statuses(statuses)))


class GatherStep(BaseStep):
    def __init__(self, name: str, workflow: Workflow, size_port: Port, depth: int = 1):
        super().__init__(name, workflow)
        self.depth: int = depth
        self.size_map: MutableMapping[str, Token] = {}
        self.token_map: MutableMapping[str, MutableSequence[Token]] = {}
        self.add_input_port("__size__", size_port)

    def _get_input_port_name(self) -> str:
        return next(n for n in self.input_ports if n != "__size__")

    async def _gather(self, key: str) -> None:
        output_port = self.get_output_port()
        output_port.put(
            await self._persist_token(
                token=ListToken(
                    tag=key,
                    value=sorted(
                        self.token_map[key],
                        key=cmp_to_key(lambda x, y: compare_tags(x.tag, y.tag)),
                    ),
                ),
                port=output_port,
                input_token_ids=get_entity_ids(
                    [self.size_map[key], *self.token_map[key]]
                ),
            )
        )

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        params = row["params"]
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            depth=params["depth"],
            size_port=await loading_context.load_port(context, params["size_port"]),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        await self.get_size_port().save(context)
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "depth": self.depth,
            "size_port": self.get_size_port().persistent_id,
        }

    def add_input_port(self, name: str, port: Port) -> None:
        if len(self.input_ports) < 2 or name in self.input_ports:
            super().add_input_port(name, port)
        else:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single input port."
            )

    def add_output_port(self, name: str, port: Port) -> None:
        if not self.output_ports or port.name in self.output_ports:
            super().add_output_port(name, port)
        else:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single output port."
            )

    def get_size_port(self) -> Port:
        return self.get_input_port("__size__")

    def get_input_port(self, name: str | None = None) -> Port:
        return super().get_input_port(
            self._get_input_port_name() if name is None else name
        )

    async def run(self) -> None:
        if len(self.input_ports) != 2:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single input port."
            )
        if len(self.output_ports) != 1:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single output port."
            )
        input_port = self.get_input_port()
        size_port = self.get_size_port()
        port_name = self._get_input_port_name()
        tasks = {
            asyncio.create_task(
                size_port.get(posixpath.join(self.name, "__size__")), name="__size__"
            ),
            asyncio.create_task(
                input_port.get(posixpath.join(self.name, port_name)), name=port_name
            ),
        }
        keys_completed = set()
        status = Status.SKIPPED
        while tasks:
            finished, unfinished = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in finished:
                if task.cancelled():
                    continue
                task_name = task.get_name()
                token = task.result()
                if check_termination(token):
                    status = _reduce_statuses([status, token.value])
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            f"Step {self.name} received termination token on port {task_name}"
                        )
                else:
                    if task_name == "__size__":
                        self.size_map[token.tag] = token
                        port = size_port
                        if len(self.token_map.setdefault(token.tag, [])) == token.value:
                            await self._gather(token.tag)
                            keys_completed.add(token.tag)
                    else:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Step {self.name} received input {token.tag}")
                        key = ".".join(token.tag.split(".")[: -self.depth])
                        self.token_map.setdefault(key, []).append(token)
                        port = input_port
                        size_value = (
                            self.size_map[key].value if key in self.size_map else None
                        )
                        if len(self.token_map.setdefault(key, [])) == size_value:
                            await self._gather(key)
                            keys_completed.add(key)
                    unfinished.add(
                        asyncio.create_task(
                            port.get(posixpath.join(self.name, task_name)),
                            name=task_name,
                        )
                    )
            tasks = unfinished
        # Gather all the token when the size is unknown
        if status != Status.FAILED:
            for key in (k for k in self.token_map.keys() if k not in keys_completed):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Step {self.name} forces gather on key {key}")

                # Update size_map with the current size
                self.size_map[key] = Token(
                    value=len(self.token_map[key]), tag=key, recoverable=True
                )
                await self.size_map[key].save(
                    self.workflow.context, size_port.persistent_id
                )

                await self._gather(key)
        # Terminate step
        await self.terminate(self._get_status(status))


class InputInjectorStep(BaseStep, ABC):
    def __init__(self, name: str, workflow: Workflow, job_port: JobPort):
        super().__init__(name, workflow)
        self.add_input_port("__job__", job_port)

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            job_port=cast(
                JobPort,
                await loading_context.load_port(context, row["params"]["job_port"]),
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "job_port": self.get_input_port("__job__").persistent_id
        }

    def add_output_port(self, name: str, port: Port) -> None:
        if not self.output_ports or port.name in self.output_ports:
            super().add_output_port(name, port)
        else:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single output port."
            )

    @abstractmethod
    async def process_input(self, job: Job, token_value: Any) -> Token: ...

    async def run(self) -> None:
        input_ports = {
            k: v for k, v in self.get_input_ports().items() if k != "__job__"
        }
        if len(input_ports) != 1:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single input port."
            )
        if len(self.output_ports) != 1:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single output port."
            )
        status = Status.SKIPPED
        if input_ports:
            while True:
                # Retrieve input token
                key_port, token = next(
                    iter((await self._get_inputs(input_ports)).items())
                )
                # Save workflow input
                if not token.persistent_id:
                    await token.save(
                        self.workflow.context,
                        port_id=self.get_input_port(key_port).persistent_id,
                    )
                # Retrieve job
                job = await cast(JobPort, self.get_input_port("__job__")).get_job(
                    self.name
                )
                # Check for termination
                if check_termination(token) or job is None:
                    status = token.value
                    break
                try:
                    await self.workflow.context.scheduler.notify_status(
                        job.name, Status.RUNNING
                    )
                    in_list = [
                        get_job_token(
                            job.name, self.get_input_port("__job__").token_list
                        ),
                        token,
                    ]
                    # Process value and inject token in the output port
                    self.get_output_port().put(
                        await self._persist_token(
                            token=await self.process_input(
                                job=job, token_value=token.value
                            ),
                            port=self.get_output_port(),
                            input_token_ids=get_entity_ids(in_list),
                        )
                    )
                finally:
                    # Notify completion to scheduler
                    await self.workflow.context.scheduler.notify_status(
                        job.name, Status.COMPLETED
                    )
        # Terminate step
        await self.terminate(self._get_status(status))


class LoopCombinatorStep(CombinatorStep):
    def __init__(self, name: str, workflow: Workflow, combinator: Combinator):
        super().__init__(name, workflow, combinator)
        self.iteration_termination_checklist: MutableMapping[str, set[str]] = {}

    async def run(self) -> None:
        # Set default status to SKIPPED
        status = Status.SKIPPED
        if self.input_ports:
            input_tasks, terminated = [], []
            for port_name, port in self.get_input_ports().items():
                self.iteration_termination_checklist[port_name] = set()
                input_tasks.append(
                    asyncio.create_task(
                        port.get(posixpath.join(self.name, port_name)), name=port_name
                    )
                )
            while input_tasks:
                # Wait for the next token
                finished, unfinished = await asyncio.wait(
                    input_tasks, return_when=asyncio.FIRST_COMPLETED
                )
                input_tasks = list(unfinished)
                for task in finished:
                    task_name = task.get_name()
                    token = task.result()
                    # If a TerminationToken is received, the corresponding port terminated its outputs
                    if check_termination(token):
                        status = _reduce_statuses([status, token.value])
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"Step {self.name} received termination token for port {task_name}"
                            )
                        if token.value != Status.COMPLETED:
                            self.iteration_termination_checklist.get(task_name).clear()
                        terminated.append(task_name)
                    # If an IterationTerminationToken is received, mark the corresponding iteration as terminated
                    elif check_iteration_termination(token):
                        if token.tag in self.iteration_termination_checklist[task_name]:
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(
                                    f"Step {self.name} received iteration termination token {token.tag} "
                                    f"for port {task_name}"
                                )
                            self.iteration_termination_checklist[task_name].remove(
                                token.tag
                            )
                    # Otherwise, build combination and set default status to COMPLETED
                    else:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"Step {self.name} received token {token.tag} "
                                f"on port {task_name}"
                            )
                        if (
                            ".".join(token.tag.split(".")[:-1])
                            not in self.iteration_termination_checklist[task_name]
                        ):
                            self.iteration_termination_checklist[task_name].add(
                                token.tag
                            )

                        async for schema in self.combinator.combine(task_name, token):
                            ins = [
                                id_ for t in schema.values() for id_ in t["input_ids"]
                            ]
                            for port_name, token in schema.items():
                                self.get_output_port(port_name).put(
                                    await self._persist_token(
                                        token=token["token"],
                                        port=self.get_output_port(port_name),
                                        input_token_ids=ins,
                                    )
                                )
                    # Create a new task in place of the completed one if the port is not terminated
                    if not (
                        task_name in terminated
                        and len(self.iteration_termination_checklist[task_name]) == 0
                    ):
                        input_tasks.append(
                            asyncio.create_task(
                                self.get_input_ports()[task_name].get(
                                    posixpath.join(self.name, task_name)
                                ),
                                name=task_name,
                            )
                        )
        # Terminate step
        await self.terminate(self._get_status(status))


class LoopOutputStep(BaseStep, ABC):
    def __init__(self, name: str, workflow: Workflow):
        super().__init__(name, workflow)
        self.token_map: MutableMapping[str, MutableSequence[Token]] = {}
        self.size_map: MutableMapping[str, int] = {}
        self.termination_map: MutableMapping[str, bool] = {}

    @abstractmethod
    async def _process_output(self, tag: str) -> Token: ...

    def add_input_port(self, name: str, port: Port) -> None:
        if not self.input_ports or name in self.input_ports:
            super().add_input_port(name, port)
        else:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single input port."
            )

    def add_output_port(self, name: str, port: Port) -> None:
        if not self.output_ports or port.name in self.output_ports:
            super().add_output_port(name, port)
        else:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single output port."
            )

    async def run(self) -> None:
        if len(self.input_ports) != 1:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single input port."
            )
        if len(self.output_ports) != 1:
            raise WorkflowDefinitionException(
                f"{self.name} step must contain a single output port."
            )
        input_port = self.get_input_port()
        status = Status.SKIPPED
        while True:
            token = await input_port.get(
                posixpath.join(self.name, next(iter(self.input_ports)))
            )
            prefix = ".".join(token.tag.split(".")[:-1])
            # If a TerminationToken is received, terminate the step
            if check_termination(token):
                status = _reduce_statuses([status, token.value])
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Step {self.name} received termination token")
                # If no iterations have been performed, just terminate
                if not self.token_map:
                    break
                # Otherwise, build termination map to wait for all iteration terminations
                else:
                    self.termination_map = {
                        k: len(self.token_map[k]) == self.size_map.get(k, -1)
                        for k in self.token_map
                    }
            # If an IterationTerminationToken is received, process loop output for the current port
            elif check_iteration_termination(token):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Step {self.name} received iteration termination token {token.tag}."
                    )
                self.size_map[prefix] = int(token.tag.split(".")[-1])
            # Otherwise, store the new token in the map
            else:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Step {self.name} received token {token.tag}.")
                if prefix not in self.token_map:
                    self.token_map[prefix] = []
                self.token_map[prefix].append(token)
            if len(self.token_map.get(prefix, [])) == self.size_map.get(prefix, -1):
                self.get_output_port().put(
                    await self._persist_token(
                        token=await self._process_output(prefix),
                        port=self.get_output_port(),
                        input_token_ids=get_entity_ids(self.token_map.get(prefix)),
                    )
                )
            # If all iterations are terminated, terminate the step
            if self.termination_map and all(self.termination_map):
                break
        # Terminate step
        await self.terminate(self._get_status(status))


class ScheduleStep(BaseStep):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        binding_config: BindingConfig,
        connector_ports: MutableMapping[str, ConnectorPort],
        job_port: JobPort | None = None,
        job_prefix: str | None = None,
        hardware_requirement: HardwareRequirement | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ):
        super().__init__(name, workflow)
        self.binding_config: BindingConfig = binding_config
        self.job_prefix: str = job_prefix or name
        self.hardware_requirement: HardwareRequirement | None = hardware_requirement
        self.input_directory: str | None = input_directory
        self.output_directory: str | None = output_directory
        self.tmp_directory: str | None = tmp_directory
        for name, port in connector_ports.items():
            self.add_input_port(f"__connector__{name}", port)
        self.add_output_port("__job__", job_port or workflow.create_port(cls=JobPort))

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        params = row["params"]
        if hardware_requirement := params.get("hardware_requirement"):
            hardware_requirement = await HardwareRequirement.load(
                context, hardware_requirement, loading_context
            )
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            binding_config=await BindingConfig.load(
                context, params["binding_config"], loading_context
            ),
            connector_ports={
                k: cast(ConnectorPort, await loading_context.load_port(context, v))
                for k, v in params["connector_ports"].items()
            },
            job_port=cast(
                JobPort, await loading_context.load_port(context, params["job_port"])
            ),
            job_prefix=params["job_prefix"],
            hardware_requirement=hardware_requirement,
            input_directory=params["input_directory"],
            output_directory=params["output_directory"],
            tmp_directory=params["tmp_directory"],
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        await self.get_output_port("__job__").save(context)
        params = cast(
            dict[str, Any], await super()._save_additional_params(context)
        ) | {
            "connector_ports": {
                name: self.get_input_port(name).persistent_id
                for name in self.input_ports
                if name.startswith("__connector__")
            },
            "job_port": self.get_output_port("__job__").persistent_id,
            "binding_config": await self.binding_config.save(context),
            "job_prefix": self.job_prefix,
            "input_directory": self.input_directory,
            "output_directory": self.output_directory,
            "tmp_directory": self.tmp_directory,
        }
        if self.hardware_requirement:
            params["hardware_requirement"] = await self.hardware_requirement.save(
                context
            )
        return params

    @recoverable
    async def _schedule(
        self,
        job: Job,
    ):
        await self.workflow.context.scheduler.schedule(
            job,
            self.binding_config,
            self.hardware_requirement,
        )

        connector = self.workflow.context.scheduler.get_connector(job.name)
        locations = self.workflow.context.scheduler.get_locations(job.name)

        await self._set_job_directories(connector, locations, job)

        # Register paths
        for location in locations:
            for directory in (
                job.input_directory,
                job.output_directory,
                job.tmp_directory,
            ):
                if not self.workflow.context.data_manager.get_data_locations(
                    directory, location.deployment, location.name
                ):
                    realpath = await StreamFlowPath(
                        directory, context=self.workflow.context, location=location
                    ).resolve()
                    if str(realpath) != directory:
                        self.workflow.context.data_manager.register_path(
                            location=location,
                            path=str(realpath),
                            relpath=str(realpath),
                        )
                    self.workflow.context.data_manager.register_path(
                        location=location,
                        path=directory,
                        relpath=directory,
                        data_type=(
                            DataType.PRIMARY
                            if str(realpath) == directory
                            else DataType.SYMBOLIC_LINK
                        ),
                    )
        # Propagate job
        token_inputs = []
        for step_port_name in self.input_ports.keys():
            if step_port_name in job.inputs.keys():
                token_inputs.append(job.inputs[step_port_name])
            else:  # other tokens from connector ports
                for t in self.get_input_port(step_port_name).token_list:
                    if t.persistent_id:
                        token_inputs.append(t)
        self.get_output_port().put(
            await self._persist_token(
                token=JobToken(value=job),
                port=self.get_output_port(),
                input_token_ids=get_entity_ids(token_inputs),
            )
        )

    async def _set_job_directories(
        self,
        connector: Connector,
        locations: MutableSequence[ExecutionLocation],
        job: Job,
    ) -> None:
        allocation = self.workflow.context.scheduler.get_allocation(job.name)
        path_processor = get_path_processor(connector)
        job.input_directory = _get_directory(
            path_processor, job.input_directory, allocation.target
        )
        job.output_directory = _get_directory(
            path_processor, job.output_directory, allocation.target
        )
        job.tmp_directory = _get_directory(
            path_processor, job.tmp_directory, allocation.target
        )
        # Create directories
        create_tasks = []
        for location in locations:
            for directory in [
                job.input_directory,
                job.output_directory,
                job.tmp_directory,
            ]:
                create_tasks.append(
                    asyncio.create_task(
                        StreamFlowPath(
                            directory, context=self.workflow.context, location=location
                        ).mkdir(mode=0o777, parents=True, exist_ok=True)
                    )
                )
        await asyncio.gather(*create_tasks)
        input_directory, output_directory, tmp_directory = (
            str(p) if p else None
            for p in await asyncio.gather(
                *(
                    asyncio.create_task(
                        StreamFlowPath(
                            directory,
                            context=self.workflow.context,
                            location=next(iter(locations)),
                        ).resolve()
                    )
                    for directory in (
                        job.input_directory,
                        job.output_directory,
                        job.tmp_directory,
                    )
                )
            )
        )
        if input_directory is None:
            raise WorkflowExecutionException(
                f"Job {self.name} cannot resolve input directory: {job.input_directory}"
            )
        if output_directory is None:
            raise WorkflowExecutionException(
                f"Job {self.name} cannot resolve output directory: {job.output_directory}"
            )
        if tmp_directory is None:
            raise WorkflowExecutionException(
                f"Job {self.name} cannot resolve tmp directory: {job.tmp_directory}"
            )
        job.input_directory = input_directory
        job.output_directory = output_directory
        job.tmp_directory = tmp_directory

    def get_output_port(self, name: str | None = None) -> JobPort:
        return cast(JobPort, super().get_output_port(name))

    async def run(self) -> None:
        try:
            # Retrieve connector
            connector_ports = cast(
                MutableMapping[str, ConnectorPort],
                {
                    name: self.get_input_port(name)
                    for name in self.input_ports
                    if name.startswith("__connector__")
                },
            )
            # If there are input ports
            input_ports = {
                k: v
                for k, v in self.get_input_ports().items()
                if k not in connector_ports
            }
            await asyncio.gather(
                *(
                    asyncio.create_task(port.get(posixpath.join(self.name, name)))
                    for name, port in connector_ports.items()
                )
            )
            if input_ports:
                inputs_map = {}
                while True:
                    # Retrieve input tokens
                    inputs = await self._get_inputs(input_ports)
                    # Check for termination
                    if check_termination(inputs.values()):
                        status = _reduce_statuses([t.value for t in inputs.values()])
                        break
                    # Group inputs by tag
                    _group_by_tag(inputs, inputs_map)
                    # Process tags
                    for tag in list(inputs_map.keys()):
                        if len(inputs_map[tag]) == len(input_ports):
                            inputs = inputs_map.pop(tag)
                            # Create Job
                            job = Job(
                                name=posixpath.join(self.job_prefix, tag),
                                workflow_id=self.workflow.persistent_id,
                                inputs=inputs,
                                input_directory=self.input_directory,
                                output_directory=self.output_directory,
                                tmp_directory=self.tmp_directory,
                            )
                            # Schedule
                            await self._schedule(job=job)
            else:
                # Create Job
                job = Job(
                    name=posixpath.join(self.job_prefix, "0"),
                    workflow_id=self.workflow.persistent_id,
                    inputs={},
                    input_directory=self.input_directory,
                    output_directory=self.output_directory,
                    tmp_directory=self.tmp_directory,
                )
                # Schedule
                await self._schedule(job=job)
                status = Status.COMPLETED
            await self.terminate(self._get_status(status))
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Cancelled
        except asyncio.CancelledError:
            await self.terminate(Status.CANCELLED)
        except Exception as e:
            logger.exception(e)
            await self.terminate(Status.FAILED)


class ScatterStep(BaseStep):
    def __init__(self, name: str, workflow: Workflow, size_port: Port | None = None):
        super().__init__(name, workflow)
        self.add_output_port("__size__", size_port or workflow.create_port())

    def get_input_port_name(self) -> str:
        return next(n for n in self.input_ports)

    def _get_output_port_name(self) -> str:
        return next(n for n in self.output_ports if n != "__size__")

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            size_port=await loading_context.load_port(
                context, row["params"]["size_port"]
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        await self.get_size_port().save(context)
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "size_port": self.get_size_port().persistent_id
        }

    async def _scatter(self, token: Token) -> None:
        if isinstance(token, ListToken):
            output_port = self.get_output_port()
            for i, t in enumerate(token.value):
                output_port.put(
                    await self._persist_token(
                        token=t.retag(token.tag + "." + str(i)),
                        port=output_port,
                        input_token_ids=get_entity_ids([token]),
                    )
                )
            size_port = self.get_size_port()
            size_port.put(
                await self._persist_token(
                    token=Token(len(token.value), tag=token.tag, recoverable=True),
                    port=size_port,
                    input_token_ids=get_entity_ids([token]),
                )
            )
        else:
            raise WorkflowDefinitionException("Scatter ports require iterable inputs")

    def add_input_port(self, name: str, port: Port) -> None:
        if not self.input_ports:
            super().add_input_port(name, port)
        else:
            raise WorkflowDefinitionException(
                "Scatter step must contain a single input port."
            )

    def add_output_port(self, name: str, port: Port) -> None:
        if len(self.output_ports) < 2 or port.name in self.output_ports:
            super().add_output_port(name, port)
        else:
            raise WorkflowDefinitionException(
                "Scatter step must contain a single output port."
            )

    def get_output_port(self, name: str | None = None) -> Port:
        return super().get_output_port(
            self._get_output_port_name() if name is None else name
        )

    def get_size_port(self) -> Port:
        return self.get_output_port("__size__")

    async def run(self) -> None:
        if len(self.input_ports) != 1:
            raise WorkflowDefinitionException(
                "Scatter step must contain a single input port."
            )
        if len(self.output_ports) != 2:
            raise WorkflowDefinitionException(
                "Scatter step must contain a single output port."
            )
        input_port = self.get_input_port()
        while True:
            token = await input_port.get(
                posixpath.join(self.name, next(iter(self.input_ports)))
            )
            if isinstance(token, TerminationToken):
                status = token.value
                break
            else:
                await self._scatter(token)
        # Terminate step
        await self.terminate(self._get_status(status))


class TransferStep(BaseStep, ABC):
    def __init__(self, name: str, workflow: Workflow, job_port: JobPort):
        super().__init__(name, workflow)
        self.add_input_port("__job__", job_port)

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            job_port=cast(
                JobPort,
                await loading_context.load_port(context, row["params"]["job_port"]),
            ),
        )

    @recoverable
    async def _run_transfer(
        self, job: Job, inputs: MutableMapping[str, Token], port_name: str, token: Token
    ) -> None:
        self.get_output_port(port_name).put(
            await self._persist_token(
                token=await self.transfer(job, token),
                port=self.get_output_port(port_name),
                input_token_ids=get_entity_ids(
                    [
                        get_job_token(
                            job.name,
                            self.get_input_port("__job__").token_list,
                        ),
                        *inputs.values(),
                    ]
                ),
            )
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "job_port": self.get_input_port("__job__").persistent_id
        }

    async def run(self) -> None:
        # Set default status as SKIPPED
        status = Status.SKIPPED
        # Retrieve input ports
        input_ports = {
            k: v for k, v in self.get_input_ports().items() if k != "__job__"
        }
        if input_ports:
            inputs_map = {}
            try:
                while True:
                    # Retrieve input tokens
                    inputs = await self._get_inputs(input_ports)
                    # Retrieve job
                    job = await cast(JobPort, self.get_input_port("__job__")).get_job(
                        self.name
                    )
                    # Check for termination
                    if check_termination(inputs.values()) or job is None:
                        status = _reduce_statuses(
                            [status, *(t.value for t in inputs.values())]
                        )
                        break
                    # Group inputs by tag
                    _group_by_tag(inputs, inputs_map)
                    # Process tags
                    for tag in list(inputs_map.keys()):
                        if len(inputs_map[tag]) == len(input_ports):
                            inputs = inputs_map.pop(tag)
                            for port_name, token in inputs.items():
                                await self._run_transfer(
                                    job=job,
                                    inputs=inputs,
                                    port_name=port_name,
                                    token=token,
                                )
            # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
            except KeyboardInterrupt:
                raise
            # When receiving a CancelledError, mark the step as Cancelled
            except asyncio.CancelledError:
                await self.terminate(Status.CANCELLED)
            except Exception as e:
                logger.exception(e)
                await self.terminate(Status.FAILED)
        # Terminate step
        await self.terminate(self._get_status(status))

    @abstractmethod
    async def transfer(self, job: Job, token: Token) -> Token: ...


class Transformer(BaseStep, ABC):
    def __init__(self, name: str, workflow: Workflow):
        super().__init__(name, workflow)

    def _filter_input_ports(self) -> MutableMapping[str, Port]:
        return {k: v for k, v in self.get_input_ports().items() if k != "__job__"}

    async def run(self) -> None:
        try:
            if input_ports := self._filter_input_ports():
                inputs_map = {}

                while True:
                    # Retrieve input tokens
                    inputs = await self._get_inputs(input_ports)
                    # Check for termination
                    if check_termination(inputs.values()):
                        status = _reduce_statuses([t.value for t in inputs.values()])
                        break
                    # Group inputs by tag
                    _group_by_tag(inputs, inputs_map)
                    # Process tags
                    for tag in list(inputs_map.keys()):
                        if len(inputs_map[tag]) == len(input_ports):
                            inputs = inputs_map.pop(tag)
                            # Check for iteration termination and propagate
                            if check_iteration_termination(inputs.values()):
                                for port_name, token in inputs.items():
                                    self.get_output_port(port_name).put(
                                        await self._persist_token(
                                            token=token.update(token.value),
                                            port=self.get_output_port(port_name),
                                            input_token_ids=get_entity_ids(
                                                inputs.values()
                                            ),
                                        )
                                    )
                            # Otherwise, apply transformation and propagate outputs
                            else:
                                for port_name, token in (
                                    await self.transform(inputs)
                                ).items():
                                    if not isinstance(token, MutableSequence):
                                        token = [token]
                                    for t in token:
                                        self.get_output_port(port_name).put(
                                            await self._persist_token(
                                                token=t,
                                                port=self.get_output_port(port_name),
                                                input_token_ids=get_entity_ids(
                                                    inputs.values()
                                                ),
                                            )
                                        )
            else:
                for port_name, token in (await self.transform({})).items():
                    self.get_output_port(port_name).put(
                        await self._persist_token(
                            token=token,
                            port=self.get_output_port(port_name),
                            input_token_ids=[],
                        )
                    )
                status = Status.COMPLETED
            # Terminate step
            await self.terminate(self._get_status(status))
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Cancelled
        except asyncio.CancelledError:
            await self.terminate(Status.CANCELLED)
        except Exception as e:
            logger.exception(e)
            await self.terminate(Status.FAILED)

    @abstractmethod
    async def transform(
        self, inputs: MutableMapping[str, Token]
    ) -> MutableMapping[str, Token | MutableSequence[Token]]: ...
