import asyncio
from collections.abc import MutableMapping, MutableSequence
from typing import Any

from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Token


async def get_step_rows(
    port_id: int, context: StreamFlowContext
) -> MutableSequence[MutableMapping[str, Any]]:
    return await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_step(step_id_row["step"]))
            for step_id_row in await context.database.get_input_steps(port_id)
        )
    )


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
    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["dependee"]))
            for row in await context.database.get_dependees(persistent_id)
        )
    )
