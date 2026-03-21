from __future__ import annotations

import asyncio
from abc import ABC
from collections.abc import MutableMapping, MutableSequence
from typing import Any, cast

from typing_extensions import Self

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import (
    Command,
    CommandOptions,
    CommandToken,
    CommandTokenProcessor,
    Step,
    Token,
)
from streamflow.workflow.token import ListToken, ObjectToken
from streamflow.workflow.utils import get_token_value


class ListCommandToken(CommandToken):
    pass


class MapCommandTokenProcessor(CommandTokenProcessor):
    def __init__(self, name: str, processor: CommandTokenProcessor):
        super().__init__(name)
        self.processor: CommandTokenProcessor = processor

    def _check_list(self, token: Token) -> None:
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
    ) -> Self:
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
        token: Token,
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

    def _check_dict(self, token: Token) -> None:
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
    ) -> Self:
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
                    strict=True,
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
                    strict=True,
                )
            }
        }

    def _update_options(self, options: CommandOptions, token: Token) -> CommandOptions:
        return options

    def bind(
        self,
        token: Token,
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


class TokenizedCommand(Command, ABC):
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
    ) -> Self:
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
    ) -> Self:
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
        token: Token,
        position: int | None,
        options: CommandOptions,
    ) -> CommandToken:
        if (command_token := self._get_processor(token)) is None:
            raise WorkflowDefinitionException(
                f"No suitable command token for input {self.name} value {get_token_value(token)}"
            )
        return command_token.bind(token, position, self._update_options(options, token))

    def check_type(self, token: Token) -> bool:
        return self._get_processor(token) is not None
