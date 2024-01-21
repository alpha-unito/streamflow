import asyncio

from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import DatabaseLoadingContext


async def load_next_tokens(
    persistent_id: int,
    context: StreamFlowContext,
    loading_context: DatabaseLoadingContext,
):
    rows = await context.database.get_dependers(persistent_id)
    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["depender"]))
            for row in rows
        )
    )


async def load_prev_tokens(
    persistent_id: int,
    context: StreamFlowContext,
    loading_context: DatabaseLoadingContext,
):
    rows = await context.database.get_dependees(persistent_id)
    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["dependee"]))
            for row in rows
        )
    )
