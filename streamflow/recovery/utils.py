from __future__ import annotations

import asyncio
import logging
import posixpath
from collections.abc import Collection, MutableMapping, MutableSequence, MutableSet

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.core.exception import (
    FailureHandlingException,
)
from streamflow.core.utils import get_class_fullname
from streamflow.core.workflow import Token
from streamflow.cwl import utils
from streamflow.cwl.token import CWLFileToken
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import (
    ExecuteStep,
    InputInjectorStep,
)
from streamflow.workflow.token import (
    JobToken,
    ListToken,
    ObjectToken,
)

INIT_DAG_FLAG = "init"
TOKEN_WAITER = "twaiter"


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


async def get_step_instances_from_output_port(port_id, context):
    step_id_rows = await context.database.get_input_steps(port_id)
    return await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_step(step_id_row["step"]))
            for step_id_row in step_id_rows
        )
    )


async def get_execute_step_out_token_ids(next_token_ids, context):
    execute_step_out_token_ids = set()
    for t_id in next_token_ids:
        if t_id > 0:
            port_row = await context.database.get_port_from_token(t_id)
            for step_id_row in await context.database.get_input_steps(port_row["id"]):
                step_row = await context.database.get_step(step_id_row["step"])
                if step_row["type"] == get_class_fullname(ExecuteStep):
                    execute_step_out_token_ids.add(t_id)
        else:
            execute_step_out_token_ids.add(t_id)
    return execute_step_out_token_ids


# async def _cleanup_dir(
#     connector: Connector, location: Location, directory: str
# ) -> None:
#     await remotepath.rm(
#         connector, location, await remotepath.listdir(connector, location, directory)
#     )


def increase_tag(tag):
    if len(tag_list := tag.rsplit(".", maxsplit=1)) == 2:
        return ".".join((tag_list[0], str(int(tag_list[1]) + 1)))
    return None


async def _is_file_available(data_location, context):
    connector = context.deployment_manager.get_connector(data_location.deployment)
    if not (
        res := await remotepath.exists(
            connector, data_location.location, data_location.path
        )
    ):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Invalidated location {data_location.deployment} (Lost path {data_location.path})"
            )
        context.data_manager.invalidate_location(data_location, "/")
    return res


async def _is_token_available(token: Token, context: StreamFlowContext, token_checked_list: MutableSet[str]) -> bool:
    if isinstance(token, JobToken):
        return False
    elif isinstance(token, CWLFileToken):
        data_locs = []
        token_path = utils.get_path_from_token(token.value)
        for data_loc in context.data_manager.get_data_locations(token_path):
            if data_loc.data_type == DataType.PRIMARY and token_path not in token_checked_list:
                data_locs.append(data_loc)
        for data_loc, is_available in zip(
            data_locs,
            await asyncio.gather(
                *(
                    asyncio.create_task(_is_file_available(data_loc, context))
                    for data_loc in data_locs
                )
            ),
        ):
            if is_available:
                token_checked_list.add(data_loc.path)
    elif isinstance(token, ListToken):
        return all(
            await asyncio.gather(
                *(
                    asyncio.create_task(_is_token_available(t, context, token_checked_list))
                    for t in token.value
                )
            )
        )
    # todo ObjectToken and Token.token
    return await token.is_available(context)


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


def get_value(elem, dictionary):
    for k, v in dictionary.items():
        if v == elem:
            return k
    raise Exception("Value not found in dictionary")


async def _execute_recovered_workflow(new_workflow, step_name, output_ports):
    if not new_workflow.steps.keys():
        logger.info(
            f"Workflow {new_workflow.name} is empty. "
            f"Waiting output ports "
            f"{[p.name for p in new_workflow.ports.values() if not isinstance(p, (JobPort, ConnectorPort))]}"
        )

        # for debug. Versione corretta quella con la gather
        for p in new_workflow.ports.values():
            if not isinstance(p, (JobPort, ConnectorPort)):
                await p.get(posixpath.join(step_name, get_value(p.name, output_ports)))
        # await asyncio.gather(
        #     *(
        #         asyncio.create_task(
        #             p.get(posixpath.join(step_name, get_value(p.name, output_ports)))
        #         )
        #         for p in new_workflow.ports.values()
        #         if not isinstance(p, (JobPort, ConnectorPort))
        #     )
        # )
        logger.info(f"Workflow {new_workflow.name}: Port terminated")
    else:
        await new_workflow.save(new_workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        await executor.run()
        logger.info(f"COMPLETED Workflow execution {new_workflow.name}")


async def load_and_add_ports(port_ids, new_workflow, loading_context):
    for port in await asyncio.gather(
        *(
            asyncio.create_task(
                loading_context.load_port(
                    new_workflow.context,
                    port_id,
                )
            )
            for port_id in port_ids
        )
    ):
        if port.name not in new_workflow.ports.keys():
            new_workflow.ports[port.name] = port
            logger.debug(
                f"populate_workflow: wf {new_workflow.name} add_1 port {port.name}"
            )
        else:
            logger.debug(
                f"populate_workflow: La port {port.name} è già presente nel workflow {new_workflow.name}"
            )
    logger.debug("populate_workflow: Port loaded")


def _missing_dependency_ports(
    dependencies: MutableMapping[str, str], port_names: MutableSequence[str]
):
    dependency_ports = set()
    for dep_name, port_name in dependencies.items():
        if port_name not in port_names:
            dependency_ports.add(dep_name)
    return dependency_ports


async def load_missing_ports(new_workflow, step_name_id, loading_context):
    missing_ports = set()
    for step in new_workflow.steps.values():
        if isinstance(step, InputInjectorStep):
            continue
        if missing_dependency_ports := _missing_dependency_ports(
            step.output_ports, new_workflow.ports.keys()
        ):
            for dependency_row in await new_workflow.context.database.get_output_ports(
                step_name_id[step.name]
            ):
                if dependency_row["name"] in missing_dependency_ports:
                    logger.debug(
                        f"populate_workflow: Aggiungo port {step.output_ports[dependency_row['name']]} al "
                        f"wf {new_workflow.name} perché è un output port dello step {step.name}"
                    )
                    missing_ports.add(dependency_row["port"])
    for port in await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_port(new_workflow.context, p_id))
            for p_id in missing_ports
        )
    ):
        logger.debug(
            f"populate_workflow: wf {new_workflow.name} add_2 port {port.name}"
        )
        new_workflow.ports[port.name] = port
