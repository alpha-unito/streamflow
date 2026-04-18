import asyncio
from collections.abc import MutableSequence

from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Token


async def load_depender_tokens(
    persistent_id: int,
    loading_context: DatabaseLoadingContext,
) -> MutableSequence[Token]:
    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(row["depender"]))
            for row in await loading_context.database.get_dependers(persistent_id)
        )
    )


async def load_dependee_tokens(
    persistent_id: int,
    loading_context: DatabaseLoadingContext,
) -> MutableSequence[Token]:
    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(row["dependee"]))
            for row in await loading_context.database.get_dependees(persistent_id)
        )
    )
