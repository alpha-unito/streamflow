import asyncio
from collections.abc import MutableSequence

from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Token
from streamflow.log_handler import logger


async def load_depender_tokens(
    persistent_id: int,
    context: StreamFlowContext,
    loading_context: DatabaseLoadingContext,
) -> MutableSequence[Token]:
    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["depender"]))
            for row in await context.database.get_dependers(persistent_id)
        )
    )


async def load_dependee_tokens(
    persistent_id: int,
    context: StreamFlowContext,
    loading_context: DatabaseLoadingContext,
) -> MutableSequence[Token]:
    dependee = await context.database.get_dependees(persistent_id)
    if any(row["dependee"] is None for row in dependee):
        logger.debug(
            f"Token {persistent_id} has at least a None dependee: {dict(dependee)}"
        )
    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["dependee"]))
            for row in dependee
        )
    )
