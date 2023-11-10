from __future__ import annotations


import asyncio
import json
from typing import MutableMapping, Tuple, MutableSequence, Collection

from streamflow.core.utils import get_class_fullname, get_class_from_name
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import (
    FailureHandlingException,
)
from streamflow.core.workflow import Token

from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.transformer import BackPropagationTransformer
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import ExecuteStep
from streamflow.workflow.token import (
    JobToken,
    ListToken,
    ObjectToken,
)

INIT_DAG_FLAG = "init"
TOKEN_WAITER = "twaiter"

# todo: spostare alcuni metodi in altri file esempio
#  - get_token_by_tag forse meglio in utils core?
#  - get_input_ports in persistence.utils?
#    oppure cambiare query ritornando le row delle ports


async def get_input_ports(step_id, context):
    return await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_port(dep_row["port"]))
            for dep_row in await context.database.get_input_ports(step_id)
        )
    )


async def get_output_ports(step_id, context):
    return await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_port(dep_row["port"]))
            for dep_row in await context.database.get_output_ports(step_id)
        )
    )


def get_files_from_token(token: Token) -> MutableSequence[str]:
    if isinstance(token, CWLFileToken):
        return [token.value["path"]]
    if isinstance(token, ListToken):
        return [
            file
            for inner_token in token.value
            for file in get_files_from_token(inner_token)
        ]
    if isinstance(token, ObjectToken):
        return [
            file
            for inner_token in token.value.values()
            for file in get_files_from_token(inner_token)
        ]
    if isinstance(token.value, Token):
        return get_files_from_token(token.value)
    return []


async def get_steps_from_output_port(port_id, context):
    step_id_rows = await context.database.get_steps_from_output_port(port_id)
    return await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_step(step_id_row["step"]))
            for step_id_row in step_id_rows
        )
    )


async def get_execute_step_out_token_ids(next_token_ids, context):
    execute_step_out_token_ids = set()
    for t_id in next_token_ids:
        port_row = await context.database.get_port_from_token(t_id)
        for step_id_row in await context.database.get_steps_from_output_port(
            port_row["id"]
        ):
            step_row = await context.database.get_step(step_id_row["step"])
            if step_row["type"] == get_class_fullname(ExecuteStep):
                execute_step_out_token_ids.add(t_id)
    return execute_step_out_token_ids


async def _cleanup_dir(
    connector: Connector, location: Location, directory: str
) -> None:
    await remotepath.rm(
        connector, location, await remotepath.listdir(connector, location, directory)
    )


def get_prev_vertices(searched_vertex, dag):
    prev_vertices = set()
    for vertex, next_vertices in dag.items():
        if searched_vertex in next_vertices and vertex != INIT_DAG_FLAG:
            prev_vertices.add(vertex)
    return prev_vertices


def contains_class(class_t, object_instances):
    for instance in object_instances:
        if issubclass(class_t, type(instance)):
            return True
    return False


def search_from_values(value, dictionary):
    for k, v in dictionary.items():
        if value == v:
            return k
    return None


def get_token_by_tag(token_tag, token_list):
    for token in token_list:
        if token_tag == token.tag:
            return token
    return None


async def _is_token_available(token, context):
    return not isinstance(token, JobToken) and await token.is_available(context)


def get_necessary_tokens(
    port_tokens, all_token_visited
) -> MutableMapping[int, Tuple[Token, bool]]:
    d = {
        t_id: all_token_visited[t_id]
        for token_list in port_tokens.values()
        for t_id in token_list
        if t_id != TOKEN_WAITER
    }
    return dict(sorted(d.items()))


def is_next_of_someone(p_name, dag_ports):
    for port_name, next_port_names in dag_ports.items():
        if port_name != INIT_DAG_FLAG and p_name in next_port_names:
            return True
    return False


async def is_output_port_forward(port_id, context):
    step_rows = await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_step(dep_row["step"]))
            for dep_row in await context.database.get_input_steps(port_id)
        )
    )
    result = False
    for step_row in step_rows:
        result = result or issubclass(
            get_class_from_name(step_row["type"]), BackPropagationTransformer
        )
    return result


async def is_input_port_forward(port_id, context):
    step_rows = await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_step(dep_row["step"]))
            for dep_row in await context.database.get_output_steps(port_id)
        )
    )
    result = False
    for step_row in step_rows:
        result = result or issubclass(
            get_class_from_name(step_row["type"]), BackPropagationTransformer
        )
    return result


def get_port_from_token(token, port_tokens, token_visited):
    for port_name, token_ids in port_tokens.items():
        if token.tag in (token_visited[t_id][0].tag for t_id in token_ids):
            return port_name
    raise FailureHandlingException("Token assente")


def get_key_by_value(
    searched_value: int, dictionary: MutableMapping[str, Collection[int]]
):
    for key, values in dictionary.items():
        if searched_value in values:
            return key
    raise FailureHandlingException(
        f"Searched value {searched_value} not found in {dictionary}"
    )


def get_recovery_loop_expression(upper_limit):
    return f"$(inputs.index < {upper_limit})"


def get_last_token(token_list):
    for token in token_list[::-1]:
        if not isinstance(token, (IterationTerminationToken, TerminationToken)):
            return token
    return None


async def _execute_recovered_workflow(new_workflow):
    await new_workflow.save(new_workflow.context)
    executor = StreamFlowExecutor(new_workflow)
    await executor.run()
    logger.debug(f"executor.run {new_workflow.name} terminated")