from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import MutableMapping, MutableSequence
from typing import Any

from typing_extensions import Self

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataLocation, DataType
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Job, Status, Token
from streamflow.data.remotepath import StreamFlowPath
from streamflow.log_handler import logger


async def _is_path_available(
    context: StreamFlowContext, data_location: DataLocation
) -> bool:
    try:
        result = await StreamFlowPath(
            data_location.path,
            context=context,
            location=data_location.location,
        ).exists()
    except WorkflowExecutionException as err:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Impossible to check the existence of {data_location.path} on location {data_location.location}: {err}"
            )
        result = False
    if not result:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Invalidated {data_location.path} path on location {data_location.location}"
            )
        context.data_manager.invalidate_location(
            data_location.location, data_location.path
        )
    elif logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            f"Available {data_location.path} path on location {data_location.location}"
        )

    return result


class IterationTerminationToken(Token):
    __slots__ = ()

    def __init__(self, tag: str):
        super().__init__(None, tag)

    def get_weight(self, context: StreamFlowContext) -> int:
        return 0

    def update(self, value: Any) -> Token:
        return self.__class__(tag=self.tag)

    def retag(self, tag: str) -> Token:
        raise NotImplementedError

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(tag=row["tag"])


class FileToken(Token, ABC):
    __slots__ = ()

    @abstractmethod
    async def get_paths(self, context: StreamFlowContext) -> MutableSequence[str]: ...

    async def is_available(self, context: StreamFlowContext) -> bool:
        """The `FileToken` is available if all its paths exist in at least one location."""
        if not self.recoverable:
            return False
        else:
            for path in await self.get_paths(context):
                if (
                    len(
                        data_locations := context.data_manager.get_data_locations(
                            path, data_type=DataType.PRIMARY
                        )
                    )
                    == 0
                ):
                    return False
                else:
                    if not any(
                        await asyncio.gather(
                            *(
                                asyncio.create_task(
                                    _is_path_available(context, data_loc)
                                )
                                for data_loc in data_locations
                            )
                        )
                    ):
                        return False
            return True


class JobToken(Token):
    __slots__ = ()

    async def _save_value(self, context: StreamFlowContext):
        return {"job": await self.value.save(context)}

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            tag=row["tag"],
            value=await Job.load(context, row["value"]["job"], loading_context),
            recoverable=row["recoverable"],
        )


class ListToken(Token):
    __slots__ = ()

    def __init__(self, value: Any, tag: str = "0", recoverable: bool = False):
        if recoverable:
            raise WorkflowExecutionException(
                "The recoverable value of the ListToken depends solely on the recoverable values of the inner tokens."
            )
        super().__init__(value=value, tag=tag, recoverable=False)

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            tag=row["tag"],
            value=await asyncio.gather(
                *(
                    asyncio.create_task(loading_context.load_token(context, t))
                    for t in row["value"]
                )
            ),
        )

    async def _save_value(self, context: StreamFlowContext):
        await asyncio.gather(
            *(asyncio.create_task(t.save(context)) for t in self.value)
        )
        return [t.persistent_id for t in self.value]

    @property
    def recoverable(self) -> bool:
        return all(t.recoverable for t in self.value)

    @recoverable.setter
    def recoverable(self, recoverable: bool) -> None:
        if self.persistent_id is not None and self.recoverable != recoverable:
            raise WorkflowExecutionException(
                "The `recoverable` property can't be changed after the `Token` has been persisted."
            )
        for t in self.value:
            t.recoverable = recoverable

    async def get_weight(self, context: StreamFlowContext) -> int:
        return sum(
            await asyncio.gather(
                *(asyncio.create_task(t.get_weight(context)) for t in self.value)
            )
        )

    async def is_available(self, context: StreamFlowContext) -> bool:
        return all(
            await asyncio.gather(
                *(asyncio.create_task(t.is_available(context)) for t in self.value)
            )
        )

    def update(self, value: Any) -> Token:
        return self.__class__(tag=self.tag, value=value)


class ObjectToken(Token):
    __slots__ = ()

    def __init__(self, value: Any, tag: str = "0", recoverable: bool = False):
        if recoverable:
            raise WorkflowExecutionException(
                "The recoverable value of the ObjectToken depends solely on the recoverable values of the inner tokens."
            )
        super().__init__(value=value, tag=tag, recoverable=False)

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            tag=row["tag"],
            value=dict(
                zip(
                    row["value"].keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(loading_context.load_token(context, v))
                            for v in row["value"].values()
                        )
                    ),
                    strict=True,
                )
            ),
        )

    async def _save_value(self, context: StreamFlowContext):
        await asyncio.gather(
            *(asyncio.create_task(t.save(context)) for t in self.value.values())
        )
        return {k: t.persistent_id for k, t in self.value.items()}

    @property
    def recoverable(self) -> bool:
        return all(t.recoverable for t in self.value.values())

    @recoverable.setter
    def recoverable(self, recoverable: bool) -> None:
        if self.persistent_id is not None and self.recoverable != recoverable:
            raise WorkflowExecutionException(
                "The `recoverable` property can't be changed after the `Token` has been persisted."
            )
        for t in self.value.values():
            t.recoverable = recoverable

    async def get_weight(self, context: StreamFlowContext) -> int:
        return sum(
            await asyncio.gather(
                *(
                    asyncio.create_task(t.get_weight(context))
                    for t in self.value.values()
                )
            )
        )

    async def is_available(self, context: StreamFlowContext) -> bool:
        return all(
            await asyncio.gather(
                *(
                    asyncio.create_task(t.is_available(context))
                    for t in self.value.values()
                )
            )
        )

    def update(self, value: Any) -> Token:
        return self.__class__(tag=self.tag, value=value)


class TerminationToken(Token):
    __slots__ = ()

    def __init__(self, value: Any = Status.COMPLETED):
        if not isinstance(value, Status):
            raise WorkflowExecutionException(
                f"Termination token received an invalid value type {type(value)}: it should be of type `Status`."
            )
        super().__init__(value)

    def get_weight(self, context: StreamFlowContext) -> int:
        return 0

    def update(self, value: Any) -> Token:
        raise NotImplementedError

    def retag(self, tag: str) -> Token:
        raise NotImplementedError

    async def _save_value(self, context: StreamFlowContext):
        return {"status": self.value.value}

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(Status(row["value"]["status"]))
