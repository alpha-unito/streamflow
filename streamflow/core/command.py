from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import MutableMapping, MutableSequence
from typing import Any, cast

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Connector, ExecutionLocation, Target
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.utils import get_class_from_name, get_class_fullname
from streamflow.core.workflow import Job, Status, Step, Token, Workflow
from streamflow.workflow.token import ListToken, ObjectToken
from streamflow.workflow.utils import get_token_value


class Command(ABC):
    def __init__(self, step: Step):
        super().__init__()
        self.step: Step = step

    @abstractmethod
    async def execute(self, job: Job) -> CommandOutput: ...

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
        step: Step,
    ) -> Command:
        type_ = cast(type[Command], utils.get_class_from_name(row["type"]))
        return await type_._load(context, row["params"], loading_context, step)

    async def save(self, context: StreamFlowContext):
        return {
            "type": utils.get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
        step: Step,
    ):
        return cls(step=step)

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {}


class CommandOptions(ABC):
    pass


class CommandOutput:
    __slots__ = ("value", "status")

    def __init__(self, value: Any, status: Status):
        self.value: Any = value
        self.status: Status = status

    def update(self, value: Any):
        return CommandOutput(value=value, status=self.status)


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
    ) -> CommandOutputProcessor:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            target=(
                (await loading_context.load_target(context, row["workflow"]))
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
    ) -> CommandOutputProcessor:
        type_ = cast(
            type[CommandOutputProcessor], utils.get_class_from_name(row["type"])
        )
        return await type_._load(context, row["params"], loading_context)

    @abstractmethod
    async def process(
        self,
        job: Job,
        command_output: CommandOutput,
        connector: Connector | None = None,
    ) -> Token | None: ...

    async def save(self, context: StreamFlowContext):
        return {
            "type": utils.get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }


class CommandToken:
    __slots__ = ("name", "position", "value")

    def __init__(self, name: str | None, position: int | None, value: Any):
        self.name: str | None = name
        self.position: int | None = position
        self.value: Any = value


class CommandTokenProcessor(ABC):
    def __init__(self, name: str):
        self.name: str = name

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        return cls(name=row["name"])

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {"name": self.name}

    @abstractmethod
    def bind(
        self,
        token: Token | None,
        position: int | None,
        options: CommandOptions,
    ) -> CommandToken: ...

    @abstractmethod
    def check_type(self, token: Token) -> bool: ...

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> CommandTokenProcessor:
        type_ = cast(type[CommandTokenProcessor], get_class_from_name(row["type"]))
        return await type_._load(context, row["params"], loading_context)

    async def save(self, context: StreamFlowContext):
        return {
            "type": get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }


class ListCommandToken(CommandToken):
    pass


class MapCommandTokenProcessor(CommandTokenProcessor):
    def __init__(self, name: str, processor: CommandTokenProcessor):
        super().__init__(name)
        self.processor: CommandTokenProcessor = processor

    def _check_list(self, token: Token):
        if not isinstance(token, ListToken):
            raise WorkflowDefinitionException(
                f"A {self.__class__.__name__} object can only be used to process list inputs"
            )

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        return cls(
            name=row["name"],
            processor=await CommandTokenProcessor.load(
                context=context, row=row["processor"], loading_context=loading_context
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "processor": await self.processor.save(context)
        }

    def _update_options(self, options: CommandOptions, token: Token) -> CommandOptions:
        return options

    def bind(
        self,
        token: Token | None,
        position: int | None,
        options: CommandOptions,
    ) -> ListCommandToken:
        self._check_list(token)
        return ListCommandToken(
            name=self.name,
            position=position,
            value=[
                self.processor.bind(
                    token=inp,
                    position=position,
                    options=self._update_options(options, inp),
                )
                for inp in token.value
            ],
        )

    def check_type(self, token: Token) -> bool:
        if isinstance(token, ListToken):
            if len(token.value) > 0:
                return self.processor.check_type(token.value[0])
            else:
                return True
        else:
            return False


class ObjectCommandToken(CommandToken):
    pass


class ObjectCommandTokenProcessor(CommandTokenProcessor):
    def __init__(
        self, name: str, processors: MutableMapping[str, CommandTokenProcessor]
    ):
        super().__init__(name)
        self.processors: MutableMapping[str, CommandTokenProcessor] = processors

    def _check_dict(self, token: Token):
        if not isinstance(token, ObjectToken):
            raise WorkflowDefinitionException(
                f"A {self.__class__.__name__} object can only be used to process dictionary inputs"
            )

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        return cls(
            name=row["name"],
            processors={
                k: p
                for k, p in zip(
                    row["processors"].keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(
                                CommandTokenProcessor.load(
                                    context=context,
                                    row=p,
                                    loading_context=loading_context,
                                )
                            )
                            for p in row["processors"].values()
                        )
                    ),
                )
            },
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "processors": {
                name: token
                for name, token in zip(
                    self.processors.keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(t.save(context))
                            for t in self.processors.values()
                        )
                    ),
                )
            }
        }

    def _update_options(self, options: CommandOptions, token: Token) -> CommandOptions:
        return options

    def bind(
        self,
        token: Token | None,
        position: int | None,
        options: CommandOptions,
    ) -> ObjectCommandToken:
        self._check_dict(token)
        return ObjectCommandToken(
            name=self.name,
            position=position,
            value={
                key: t.bind(
                    token=token.value[key],
                    position=position,
                    options=self._update_options(options, token.value[key]),
                )
                for key, t in self.processors.items()
                if key in token.value
            },
        )

    def check_type(self, token: Token) -> bool:
        if isinstance(token, ObjectToken):
            for k, v in token.value.items():
                if not (k in self.processors and self.processors[k].check_type(v)):
                    return False
            return True
        else:
            return False


class UnionCommandTokenProcessor(CommandTokenProcessor):
    def __init__(self, name: str, processors: MutableSequence[CommandTokenProcessor]):
        super().__init__(name)
        self.processors: MutableSequence[CommandTokenProcessor] = processors

    def _get_processor(self, token: Token) -> CommandTokenProcessor | None:
        for processor in self.processors:
            if processor.check_type(token):
                return processor
        return None

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        return cls(
            name=row["name"],
            processors=cast(
                MutableSequence[CommandTokenProcessor],
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            CommandTokenProcessor.load(
                                context=context, row=p, loading_context=loading_context
                            )
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
                *(asyncio.create_task(t.save(context)) for t in self.processors)
            )
        }

    def _update_options(self, options: CommandOptions, token: Token) -> CommandOptions:
        return options

    def bind(
        self,
        token: Token | None,
        position: int | None,
        options: CommandOptions,
    ) -> CommandToken:
        if (command_token := self._get_processor(token)) is None:
            raise WorkflowDefinitionException(
                f"No suitable command token for input value {get_token_value(token)}"
            )
        return command_token.bind(token, position, self._update_options(options, token))

    def check_type(self, token: Token) -> bool:
        return self._get_processor(token) is not None


class TokenizedCommand(Command):
    def __init__(
        self,
        step: Step,
        processors: MutableSequence[CommandTokenProcessor] | None = None,
    ):
        super().__init__(step)
        self.processors: MutableSequence[CommandTokenProcessor] = processors or []

    @classmethod
    async def _load_command_token_processors(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> MutableSequence[CommandTokenProcessor]:
        return cast(
            MutableSequence[CommandTokenProcessor],
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        CommandTokenProcessor.load(context, processor, loading_context)
                    )
                    for processor in row["processors"]
                )
            ),
        )

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
        step: Step,
    ):
        return cls(
            step=step,
            processors=await cls._load_command_token_processors(
                context=context, row=row, loading_context=loading_context
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "processors": await asyncio.gather(
                *(
                    asyncio.create_task(processor.save(context))
                    for processor in self.processors
                )
            )
        }
