from __future__ import annotations

import asyncio
import json
import sys
import uuid
from abc import ABC, abstractmethod
from collections.abc import MutableMapping, MutableSequence
from enum import IntEnum
from typing import TYPE_CHECKING, TypeVar, cast

from streamflow.core import utils
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.persistence import (
    DatabaseLoadingContext,
    DependencyType,
    PersistableEntity,
)

if TYPE_CHECKING:
    from typing import Any

    from streamflow.core.context import StreamFlowContext


class Executor(ABC):
    def __init__(self, workflow: Workflow):
        self.workflow: Workflow = workflow

    @abstractmethod
    async def run(self) -> MutableMapping[str, Any]: ...


class Job:
    __slots__ = (
        "name",
        "workflow_id",
        "inputs",
        "input_directory",
        "output_directory",
        "tmp_directory",
    )

    def __init__(
        self,
        name: str,
        workflow_id: int,
        inputs: MutableMapping[str, Token],
        input_directory: str | None,
        output_directory: str | None,
        tmp_directory: str | None,
    ):
        self.name: str = name
        self.workflow_id: int = workflow_id
        self.inputs: MutableMapping[str, Token] = inputs
        self.input_directory: str | None = input_directory
        self.output_directory: str | None = output_directory
        self.tmp_directory: str | None = tmp_directory

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Job:
        return cls(
            name=row["name"],
            workflow_id=row["workflow_id"],
            inputs={
                k: v
                for k, v in zip(
                    row["inputs"].keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(loading_context.load_token(context, t))
                            for t in row["inputs"].values()
                        )
                    ),
                )
            },
            input_directory=row["input_directory"],
            output_directory=row["output_directory"],
            tmp_directory=row["tmp_directory"],
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        await asyncio.gather(
            *(asyncio.create_task(t.save(context)) for t in self.inputs.values())
        )
        return {
            "name": self.name,
            "workflow_id": self.workflow_id,
            "inputs": {k: v.persistent_id for k, v in self.inputs.items()},
            "input_directory": self.input_directory,
            "output_directory": self.output_directory,
            "tmp_directory": self.tmp_directory,
        }

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        type_ = cast(type[Job], utils.get_class_from_name(row["type"]))
        return await type_._load(context, row["params"], loading_context)

    async def save(self, context: StreamFlowContext):
        return {
            "type": utils.get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }


class Port(PersistableEntity):
    def __init__(self, workflow: Workflow, name: str):
        super().__init__()
        self.queues: MutableMapping[str, asyncio.Queue] = {}
        self.name: str = name
        self.token_list: MutableSequence[Token] = []
        self.workflow: Workflow = workflow

    def _init_consumer(self, consumer: str):
        self.queues[consumer] = asyncio.Queue()
        for t in self.token_list:
            self.queues[consumer].put_nowait(t)

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Port:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {}

    def close(self, consumer: str):
        if consumer in self.queues:
            self.queues[consumer].task_done()

    def empty(self) -> bool:
        return not self.token_list

    async def get(self, consumer: str) -> Token:
        if consumer not in self.queues:
            self._init_consumer(consumer)
            return await self.queues[consumer].get()
        else:
            token = await self.queues[consumer].get()
            self.queues[consumer].task_done()
            return token

    def get_input_steps(self) -> MutableSequence[Step]:
        return [
            s
            for s in self.workflow.steps.values()
            if self.name in s.output_ports.values()
        ]

    def get_output_steps(self) -> MutableSequence[Step]:
        return [
            s
            for s in self.workflow.steps.values()
            if self.name in s.input_ports.values()
        ]

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        persistent_id: int,
        loading_context: DatabaseLoadingContext,
    ) -> Port:
        row = await context.database.get_port(persistent_id)
        type_ = cast(type[Port], utils.get_class_from_name(row["type"]))
        port = await type_._load(context, row, loading_context)
        loading_context.add_port(persistent_id, port)
        return port

    def put(self, token: Token):
        self.token_list.append(token)
        for q in self.queues.values():
            q.put_nowait(token)

    async def save(self, context: StreamFlowContext) -> None:
        async with self.persistence_lock:
            if not self.persistent_id:
                self.persistent_id = await context.database.add_port(
                    name=self.name,
                    workflow_id=self.workflow.persistent_id,
                    type=type(self),
                    params=await self._save_additional_params(context),
                )


class Status(IntEnum):
    WAITING = 0
    FIREABLE = 1
    RUNNING = 2
    SKIPPED = 3
    COMPLETED = 4
    FAILED = 5
    CANCELLED = 6
    ROLLBACK = 7
    RECOVERY = 8


class Step(PersistableEntity, ABC):
    def __init__(self, name: str, workflow: Workflow):
        super().__init__()
        self.input_ports: MutableMapping[str, str] = {}
        self.name: str = name
        self.output_ports: MutableMapping[str, str] = {}
        self.status: Status = Status.WAITING
        self.terminated: bool = False
        self.workflow: Workflow = workflow

    def _add_port(self, name: str, port: Port, type_: DependencyType):
        if port.name not in self.workflow.ports:
            self.workflow.ports[port.name] = port
        if type_ == DependencyType.INPUT:
            self.input_ports[name] = port.name
        else:
            self.output_ports[name] = port.name

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {}

    async def _set_status(self, status: Status):
        self.status = status
        if self.persistent_id is not None:
            await self.workflow.context.database.update_step(
                self.persistent_id, {"status": status.value}
            )

    def add_input_port(self, name: str, port: Port) -> None:
        self._add_port(name, port, DependencyType.INPUT)

    def add_output_port(self, name: str, port: Port) -> None:
        self._add_port(name, port, DependencyType.OUTPUT)

    def get_input_port(self, name: str | None = None) -> Port:
        if name is None:
            if len(self.input_ports) == 1:
                return self.workflow.ports.get(next(iter(self.input_ports.values())))
            else:
                raise WorkflowExecutionException(
                    f"Cannot retrieve default input port as step {self.name} contains multiple input ports."
                )
        return (
            self.workflow.ports.get(self.input_ports[name])
            if name in self.input_ports
            else None
        )

    def get_input_ports(self) -> MutableMapping[str, Port]:
        return {k: self.workflow.ports[v] for k, v in self.input_ports.items()}

    def get_output_port(self, name: str | None = None) -> Port:
        if name is None:
            if len(self.output_ports) == 1:
                return self.workflow.ports.get(next(iter(self.output_ports.values())))
            else:
                raise WorkflowExecutionException(
                    f"Cannot retrieve default output port as step {self.name} contains multiple output ports."
                )
        return (
            self.workflow.ports.get(self.output_ports[name])
            if name in self.output_ports
            else None
        )

    def get_output_ports(self) -> MutableMapping[str, Port]:
        return {k: self.workflow.ports[v] for k, v in self.output_ports.items()}

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        persistent_id: int,
        loading_context: DatabaseLoadingContext,
    ) -> Step:
        row = await context.database.get_step(persistent_id)
        type_ = cast(type[Step], utils.get_class_from_name(row["type"]))
        step = await type_._load(context, row, loading_context)
        step.status = Status(row["status"])
        step.terminated = step.status in [
            Status.COMPLETED,
            Status.FAILED,
            Status.SKIPPED,
        ]
        input_deps = await context.database.get_input_ports(persistent_id)
        loading_context.add_step(persistent_id, step)
        input_ports = await asyncio.gather(
            *(
                asyncio.create_task(loading_context.load_port(context, d["port"]))
                for d in input_deps
            )
        )
        step.input_ports = {d["name"]: p.name for d, p in zip(input_deps, input_ports)}
        output_deps = await context.database.get_output_ports(persistent_id)
        output_ports = await asyncio.gather(
            *(
                asyncio.create_task(loading_context.load_port(context, d["port"]))
                for d in output_deps
            )
        )
        step.output_ports = {
            d["name"]: p.name for d, p in zip(output_deps, output_ports)
        }
        return step

    @abstractmethod
    async def run(self): ...

    async def save(self, context: StreamFlowContext) -> None:
        async with self.persistence_lock:
            if not self.persistent_id:
                self.persistent_id = await context.database.add_step(
                    name=self.name,
                    workflow_id=self.workflow.persistent_id,
                    status=cast(int, self.status.value),
                    type=type(self),
                    params=await self._save_additional_params(context),
                )
        save_tasks = []
        for name, port in self.get_input_ports().items():
            save_tasks.append(
                asyncio.create_task(
                    self.workflow.context.database.add_dependency(
                        step=self.persistent_id,
                        port=port.persistent_id,
                        type=DependencyType.INPUT,
                        name=name,
                    )
                )
            )
        for name, port in self.get_output_ports().items():
            save_tasks.append(
                asyncio.create_task(
                    self.workflow.context.database.add_dependency(
                        step=self.persistent_id,
                        port=port.persistent_id,
                        type=DependencyType.OUTPUT,
                        name=name,
                    )
                )
            )
        await asyncio.gather(*save_tasks)

    @abstractmethod
    async def terminate(self, status: Status): ...


class Token(PersistableEntity):
    __slots__ = ("persistent_id", "recoverable", "value", "tag")

    def __init__(self, value: Any, tag: str = "0", recoverable: bool = False):
        super().__init__()
        self.recoverable: bool = recoverable
        self.value: Any = value
        self.tag: str = tag

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Token:
        value = json.loads(row["value"])
        if isinstance(value, MutableMapping) and "token" in value:
            value = await loading_context.load_token(context, value["token"])
        return cls(tag=row["tag"], value=value, recoverable=row["recoverable"])

    async def _save_value(self, context: StreamFlowContext):
        if isinstance(self.value, Token) and not self.value.persistent_id:
            await self.value.save(context)
        return (
            {"token": self.value.persistent_id}
            if isinstance(self.value, Token)
            else self.value
        )

    async def get_weight(self, context: StreamFlowContext):
        if isinstance(self.value, Token):
            return await self.value.get_weight(context)
        else:
            return sys.getsizeof(self.value)

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        persistent_id: int,
        loading_context: DatabaseLoadingContext,
    ) -> Token:
        row = await context.database.get_token(persistent_id)
        type_ = cast(type[Token], utils.get_class_from_name(row["type"]))
        token = await type_._load(context, row, loading_context)
        loading_context.add_token(persistent_id, token)
        return token

    async def is_available(self, context: StreamFlowContext) -> bool:
        if isinstance(self.value, Token):
            return await self.value.is_available(context)
        else:
            return True

    def retag(self, tag: str, recoverable: bool = False) -> Token:
        return self.__class__(tag=tag, value=self.value, recoverable=recoverable)

    async def save(self, context: StreamFlowContext, port_id: int | None = None):
        async with self.persistence_lock:
            if not self.persistent_id:
                try:
                    self.persistent_id = await context.database.add_token(
                        port=port_id,
                        recoverable=self.recoverable,
                        tag=self.tag,
                        type=type(self),
                        value=json.dumps(await self._save_value(context)),
                    )
                except TypeError as e:
                    raise WorkflowExecutionException from e

    def update(self, value: Any, recoverable: bool = False) -> Token:
        return self.__class__(tag=self.tag, value=value, recoverable=recoverable)


class TokenProcessor(ABC):
    def __init__(self, name: str, workflow: Workflow):
        self.name: str = name
        self.workflow: Workflow = workflow

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> TokenProcessor:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {"name": self.name, "workflow": self.workflow.persistent_id}

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        type_ = cast(type[TokenProcessor], utils.get_class_from_name(row["type"]))
        return await type_._load(context, row["params"], loading_context)

    @abstractmethod
    async def process(
        self, inputs: MutableMapping[str, Token], token: Token
    ) -> Token: ...

    async def save(self, context: StreamFlowContext):
        return {
            "type": utils.get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }


if TYPE_CHECKING:
    P = TypeVar("P", bound=Port)
    S = TypeVar("S", bound=Step)


class Workflow(PersistableEntity):
    def __init__(
        self,
        context: StreamFlowContext,
        config: MutableMapping[str, Any],
        name: str = None,
    ):
        super().__init__()
        self.context: StreamFlowContext = context
        self.config: MutableMapping[str, Any] = config
        self.name: str = name if name is not None else str(uuid.uuid4())
        self.output_ports: MutableMapping[str, str] = {}
        self.ports: MutableMapping[str, Port] = {}
        self.steps: MutableMapping[str, Step] = {}
        self.type: str | None = None

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Workflow:
        params = json.loads(row["params"])
        return cls(context=context, config=params["config"], name=row["name"])

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {"config": self.config, "output_ports": self.output_ports}

    def create_port(self, cls: type[P] = Port, name: str = None, **kwargs) -> P:
        if name is None:
            name = str(uuid.uuid4())
        port = cls(workflow=self, name=name, **kwargs)
        self.ports[name] = port
        return port

    def create_step(self, cls: type[S], name: str = None, **kwargs) -> S:
        if name is None:
            name = str(uuid.uuid4())
        step = cls(name=name, workflow=self, **kwargs)
        self.steps[name] = step
        return step

    def get_output_port(self, name: str) -> Port:
        return self.ports[self.output_ports[name]]

    def get_output_ports(self) -> MutableMapping[str, Port]:
        return {name: self.ports[p] for name, p in self.output_ports.items()}

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        persistent_id: int,
        loading_context: DatabaseLoadingContext,
    ) -> Workflow:
        row = await context.database.get_workflow(persistent_id)
        type_ = cast(type[Workflow], utils.get_class_from_name(row["type"]))
        workflow = await type_._load(context, row, loading_context)
        loading_context.add_workflow(persistent_id, workflow)
        if workflow.persistent_id is None:
            return workflow
        rows = await context.database.get_workflow_ports(persistent_id)
        params = json.loads(row["params"])
        workflow.ports = {
            r["name"]: v
            for r, v in zip(
                rows,
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            loading_context.load_port(context, row["id"])
                        )
                        for row in rows
                    )
                ),
            )
        }
        workflow.output_ports = params["output_ports"]
        rows = await context.database.get_workflow_steps(persistent_id)
        workflow.steps = {
            r["name"]: v
            for r, v in zip(
                rows,
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            loading_context.load_step(context, row["id"])
                        )
                        for row in rows
                    )
                ),
            )
        }
        return workflow

    async def save(self, context: StreamFlowContext) -> None:
        async with self.persistence_lock:
            if not self.persistent_id:
                self.persistent_id = await self.context.database.add_workflow(
                    name=self.name,
                    params=await self._save_additional_params(context),
                    status=Status.WAITING.value,
                    type=type(self),
                )
        await asyncio.gather(
            *(asyncio.create_task(port.save(context)) for port in self.ports.values())
        )
        await asyncio.gather(
            *(asyncio.create_task(step.save(context)) for step in self.steps.values())
        )
