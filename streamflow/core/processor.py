from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import MutableMapping, MutableSequence
from typing import Any, cast

from typing_extensions import Self

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Connector, ExecutionLocation, Target
from streamflow.core.exception import ProcessorTypeError
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.utils import eval_processors, get_tag, make_future
from streamflow.core.workflow import CommandOutput, Job, Token, Workflow
from streamflow.workflow.token import ListToken, ObjectToken


class CommandOutputProcessor(ABC):
    def __init__(self, name: str, workflow: Workflow, target: Target | None = None):
        self.name: str = name
        self.workflow: Workflow = workflow
        self.target: Target | None = target

    def _get_connector(self, connector: Connector | None, job: Job) -> Connector:
        return connector or self.workflow.context.scheduler.get_connector(job.name)

    async def _get_locations(
        self, connector: Connector | None, job: Job
    ) -> MutableSequence[ExecutionLocation]:
        if self.target:
            available_locations = await connector.get_available_locations(
                service=self.target.service
            )
            return [loc.location for loc in available_locations.values()]
        else:
            return self.workflow.context.scheduler.get_locations(job.name)

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
            target=(
                (await loading_context.load_target(context, row["target"]))
                if row["target"]
                else None
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        if self.target:
            await self.target.save(context)
        return {
            "name": self.name,
            "workflow": self.workflow.persistent_id,
            "target": self.target.persistent_id if self.target else None,
        }

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        type_ = cast(Self, utils.get_class_from_name(row["type"]))
        return await type_._load(context, row["params"], loading_context)

    @abstractmethod
    async def process(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
        recoverable: bool = False,
    ) -> Token | None: ...

    async def save(self, context: StreamFlowContext):
        return {
            "type": utils.get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }


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
    ) -> Self:
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
    ) -> Self:
        type_ = cast(Self, utils.get_class_from_name(row["type"]))
        return await type_._load(context, row["params"], loading_context)

    @abstractmethod
    async def process(
        self, inputs: MutableMapping[str, Token], token: Token
    ) -> Token: ...

    async def save(self, context: StreamFlowContext) -> MutableMapping[str, Any]:
        return {
            "type": utils.get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }


class MapCommandOutputProcessor(CommandOutputProcessor):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        processor: CommandOutputProcessor,
        target: Target | None = None,
    ):
        super().__init__(name=name, workflow=workflow, target=target)
        self.processor: CommandOutputProcessor = processor

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
            processor=await CommandOutputProcessor.load(
                context, row["processor"], loading_context
            ),
            target=(
                await loading_context.load_target(context, row["target"])
                if row["target"]
                else None
            ),
        )

    async def process(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
        recoverable: bool = False,
    ) -> Token | None:
        result = await command_output
        values = result.value
        if not isinstance(values, MutableSequence):
            raise ProcessorTypeError(
                f"Invalid value {values} "
                f"for {self.name} command output: expected list"
            )
        return ListToken(
            value=await asyncio.gather(
                *(
                    asyncio.create_task(
                        self.processor.process(
                            job=job,
                            command_output=make_future(result.update(value)),
                            connector=connector,
                            recoverable=recoverable,
                        )
                    )
                    for value in values
                )
            ),
            tag=get_tag(job.inputs.values()),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "processor": await self.processor.save(context)
        }


class MapTokenProcessor(TokenProcessor):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        processor: TokenProcessor,
    ):
        super().__init__(name, workflow)
        self.processor: TokenProcessor = processor

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
            processor=await TokenProcessor.load(
                context, row["processor"], loading_context
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "processor": await self.processor.save(context),
        }

    async def process(self, inputs: MutableMapping[str, Token], token: Token) -> Token:
        # Check if token value is a list
        if not isinstance(token, ListToken):
            raise ProcessorTypeError(
                f"Invalid value {token.value} for token {self.name}: it should be an array"
            )
        # Propagate evaluation to the inner processor
        return token.update(
            await asyncio.gather(
                *(
                    asyncio.create_task(self.processor.process(inputs, v))
                    for v in token.value
                )
            )
        )


class NullTokenProcessor(TokenProcessor):
    async def process(self, inputs: MutableMapping[str, Token], token: Token) -> Token:
        if token.value is None:
            return token.update(token.value)
        else:
            raise ProcessorTypeError(
                f"Invalid value {token.value} for token {self.name}: expected null"
            )


class ObjectCommandOutputProcessor(CommandOutputProcessor):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        processors: MutableMapping[str, CommandOutputProcessor],
        target: Target | None = None,
    ):
        super().__init__(name=name, workflow=workflow, target=target)
        self.processors: MutableMapping[str, CommandOutputProcessor] = processors

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
            target=(
                (await loading_context.load_target(context, row["target"]))
                if row["target"]
                else None
            ),
            processors={
                k: v
                for k, v in zip(
                    row["processors"].keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(
                                CommandOutputProcessor.load(context, v, loading_context)
                            )
                            for v in row["processors"].values()
                        )
                    ),
                    strict=True,
                )
            },
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "processors": {
                k: v
                for k, v in zip(
                    self.processors.keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(p.save(context))
                            for p in self.processors.values()
                        )
                    ),
                    strict=True,
                )
            }
        }

    async def process(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
        recoverable: bool = False,
    ) -> Token | None:
        result = await command_output
        values = result.value
        if not isinstance(values, MutableMapping):
            raise ProcessorTypeError(
                f"Invalid value {values} "
                f"for {self.name} command output: expected object"
            )
        for key in values:
            if key not in self.processors.keys():
                raise ProcessorTypeError(
                    f"Invalid value {values} "
                    f"for {self.name} command output: unexpected key {key}"
                )
        token_tasks = {
            k: asyncio.create_task(
                p.process(
                    job=job,
                    command_output=make_future(result.update(values[k])),
                    connector=connector,
                    recoverable=recoverable,
                )
            )
            for k, p in self.processors.items()
            if k in values
        }
        return ObjectToken(
            value=dict(
                zip(
                    token_tasks.keys(),
                    await asyncio.gather(*token_tasks.values()),
                    strict=True,
                )
            ),
            tag=get_tag(job.inputs.values()),
        )


class ObjectTokenProcessor(TokenProcessor):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        processors: MutableMapping[str, TokenProcessor],
    ):
        super().__init__(name, workflow)
        self.processors: MutableMapping[str, TokenProcessor] = processors

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
            processors={
                k: v
                for k, v in zip(
                    row["processors"].keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(
                                TokenProcessor.load(context, v, loading_context)
                            )
                            for v in row["processors"].values()
                        )
                    ),
                    strict=True,
                )
            },
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "processors": {
                k: v
                for k, v in zip(
                    self.processors.keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(p.save(context))
                            for p in self.processors.values()
                        )
                    ),
                )
            },
        }

    async def process(self, inputs: MutableMapping[str, Token], token: Token) -> Token:
        if not isinstance(token, ObjectToken):
            raise ProcessorTypeError(
                f"Invalid value {token.value} for token {self.name}: it should be a record"
            )
        for key in token.value:
            if key not in self.processors.keys():
                raise ProcessorTypeError(
                    f"Invalid value {token.value} for token {self.name}: unexpected key {key}"
                )
        # Propagate evaluation to the inner processors
        return token.update(
            dict(
                zip(
                    token.value,
                    await asyncio.gather(
                        *(
                            asyncio.create_task(self.processors[k].process(inputs, v))
                            for k, v in token.value.items()
                        )
                    ),
                    strict=True,
                )
            )
        )


class PopCommandOutputProcessor(CommandOutputProcessor):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        processor: CommandOutputProcessor,
        target: Target | None = None,
    ):
        super().__init__(name=name, workflow=workflow, target=target)
        self.processor: CommandOutputProcessor = processor

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
            target=(
                (await loading_context.load_target(context, row["target"]))
                if row["target"]
                else None
            ),
            processor=await CommandOutputProcessor.load(
                context, row["processor"], loading_context
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "processor": await self.processor.save(context)
        }

    async def process(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
        recoverable: bool = False,
    ) -> Token | None:
        result = await command_output
        values = result.value
        if not isinstance(values, MutableMapping):
            raise ProcessorTypeError(
                f"Invalid value {values} "
                f"for {self.name} command output: expected object"
            )
        if self.name not in values:
            raise ProcessorTypeError(
                f"Invalid value {values} "
                f"for {self.name} command output: expected key {self.name}"
            )
        return await self.processor.process(
            job=job,
            command_output=make_future(result.update(values[self.name])),
            connector=connector,
            recoverable=recoverable,
        )


class UnionCommandOutputProcessor(CommandOutputProcessor):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        processors: MutableSequence[CommandOutputProcessor],
        target: Target | None = None,
    ):
        super().__init__(name=name, workflow=workflow, target=target)
        self.processors: MutableSequence[CommandOutputProcessor] = processors

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "processors": await asyncio.gather(
                *(asyncio.create_task(v.save(context)) for v in self.processors)
            )
        }

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
            processors=cast(
                MutableSequence[CommandOutputProcessor],
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            CommandOutputProcessor.load(context, p, loading_context)
                        )
                        for p in row["processors"]
                    )
                ),
            ),
            target=(
                (await loading_context.load_target(context, row["target"]))
                if row["target"]
                else None
            ),
        )

    async def process(
        self,
        job: Job,
        command_output: asyncio.Future[CommandOutput],
        connector: Connector | None = None,
        recoverable: bool = False,
    ) -> Token | None:
        process_tasks = [
            asyncio.create_task(
                p.process(
                    job=job,
                    command_output=command_output,
                    connector=connector,
                    recoverable=recoverable,
                ),
                name=p.__class__.__name__,
            )
            for p in self.processors
        ]
        return await eval_processors(process_tasks, name=self.name)


class UnionTokenProcessor(TokenProcessor):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        processors: MutableSequence[TokenProcessor],
    ):
        super().__init__(name, workflow)
        self.processors: MutableSequence[TokenProcessor] = processors

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
            processors=cast(
                MutableSequence[TokenProcessor],
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            TokenProcessor.load(context, p, loading_context)
                        )
                        for p in row["processors"]
                    )
                ),
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "processors": await asyncio.gather(
                *(asyncio.create_task(v.save(context)) for v in self.processors)
            )
        }

    async def process(self, inputs: MutableMapping[str, Token], token: Token) -> Token:
        process_tasks = [
            asyncio.create_task(
                p.process(inputs=inputs, token=token),
                name=p.__class__.__name__,
            )
            for p in self.processors
        ]
        return await eval_processors(process_tasks, name=self.name)
