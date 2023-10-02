from __future__ import annotations


import asyncio
from typing import MutableMapping, Tuple
from streamflow.core.utils import get_class_fullname, get_class_from_name
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import (
    FailureHandlingException,
)
from streamflow.core.workflow import Token

from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.transformer import ForwardTransformer
from streamflow.data import remotepath
from streamflow.workflow.step import ExecuteStep
from streamflow.workflow.token import JobToken


INIT_DAG_FLAG = "init"
TOKEN_WAITER = "twaiter"

# todo: spostare alcuni metodi in altri loghi esempio
#  - get_token_by_tag forse meglio in utils core?
#  - get_input_ports in persistence.utils?
#    oppure cambiare query ritornando le row delle ports


async def get_input_ports(step_id, context):
    dep_ports_step_rows = await context.database.get_input_ports(step_id)
    return await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_port(dependency_row["port"]))
            for dependency_row in dep_ports_step_rows
        )
    )


async def get_steps_from_output_port(port_id, context):
    step_id_rows = await context.database.get_steps_from_output_port(port_id)
    return await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_step(step_id_row["step"]))
            for step_id_row in step_id_rows
        )
    )


# debug
def str_tok(token):
    if isinstance(token, JobToken):
        return token.value.name
    elif isinstance(token, CWLFileToken):
        return token.value["path"]
    else:
        return token.value


# debug
def check_double_reference(dag_ports):
    for tmpp in dag_ports[INIT_DAG_FLAG]:
        for tmpport_name, tmpnext_port_names in dag_ports.items():
            if tmpport_name != INIT_DAG_FLAG and tmpp in tmpnext_port_names:
                msg = f"Port {tmpp} appartiene sia a INIT che a {tmpport_name}"
                print("WARN", msg)
                # print("OOOOOOOOOOOOOOOOOOOOOOOOOOOO" * 100, "\n", msg)
                # raise FailureHandlingException(msg)


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


def cmp(a, b):
    return (a > b) - (a < b)


def compare_tags_relaxed(tag_1, tag_2):
    if get_tag_level(tag_1) < get_tag_level(tag_2):
        return 1  # lower level, then higher tag
    if get_tag_level(tag_1) > get_tag_level(tag_2):
        return -1
    return compare_tags(tag_1, tag_2)


def compare_tags(tag_1, tag_2):
    if get_tag_level(tag_1) != get_tag_level(tag_2):
        raise FailureHandlingException("I tag hanno livelli diversi")
    if get_tag_level(tag_1) == 1:
        return -cmp(int(tag_1), int(tag_2))
    head_1, tail_1 = tag_1.split(".", maxsplit=1)
    head_2, tail_2 = tag_2.split(".", maxsplit=1)
    if head_1 == head_2:
        return compare_tags(tail_1, tail_2)
    return -cmp(int(head_1), int(head_2))


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
            get_class_from_name(step_row["type"]), ForwardTransformer
        )
    return result


def get_port_from_token(token, port_tokens, token_visited):
    for port_name, token_ids in port_tokens.items():
        if token.tag in (token_visited[t_id][0].tag for t_id in token_ids):
            return port_name
    raise FailureHandlingException("Token assente")


def get_tag_level(tag: str):
    return len(tag.split("."))


def get_key_by_value(
    searched_value: int, dictionary: MutableMapping[str, MutableSequence[int]]
):
    for key, values in dictionary.items():
        if searched_value in values:
            return key
    raise FailureHandlingException(
        f"Searched value {searched_value} not found in {dictionary}"
    )
