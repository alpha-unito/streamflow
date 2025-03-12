from __future__ import annotations

import asyncio
import logging
import posixpath
from collections.abc import Iterable, MutableMapping, MutableSet

from streamflow.core.exception import FailureHandlingException
from streamflow.core.utils import get_class_fullname
from streamflow.core.workflow import Job, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import ExecuteStep, InputInjectorStep


async def get_step_instances_from_output_port(port_id, context):
    step_id_rows = await context.database.get_input_steps(port_id)
    return await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_step(step_id_row["step"]))
            for step_id_row in step_id_rows
        )
    )


async def get_execute_step_out_token_ids(next_token_ids, context) -> MutableSet[int]:
    execute_step_out_token_ids = set()
    for token_id in next_token_ids:
        if token_id > 0:
            port_row = await context.database.get_port_from_token(token_id)
            for step_id_row in await context.database.get_input_steps(port_row["id"]):
                step_row = await context.database.get_step(step_id_row["step"])
                if step_row["type"] == get_class_fullname(ExecuteStep):
                    execute_step_out_token_ids.add(token_id)
        else:
            execute_step_out_token_ids.add(token_id)
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


def get_port_from_token(token: Token, port_tokens, token_visited):
    for port_name, token_ids in port_tokens.items():
        if token.tag in (token_visited[t_id][0].tag for t_id in token_ids):
            return port_name
    raise FailureHandlingException(f"Token id {token.persistent_id} is missing")


async def execute_recover_workflow(new_workflow: Workflow, failed_step: Step) -> None:
    if not new_workflow.steps.keys():
        logger.info(
            f"Workflow {new_workflow.name} is empty. "
            f"Waiting output ports {list(failed_step.output_ports.values())}"
        )
        assert set(new_workflow.ports.keys()) == set(failed_step.output_ports.values())
        await asyncio.gather(
            *(
                asyncio.create_task(
                    new_workflow.ports[name].get(
                        posixpath.join(failed_step.name, dependency)
                    )
                )
                for name, dependency in failed_step.output_ports.items()
            )
        )
    else:
        await new_workflow.save(new_workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        await executor.run()


async def load_and_add_ports(
    port_ids: Iterable[int],
    new_workflow: Workflow,
    workflow_builder: WorkflowBuilder,
    failed_job: Job,
) -> None:
    for port in await asyncio.gather(
        *(
            asyncio.create_task(
                workflow_builder.load_port(
                    new_workflow.context,
                    port_id,
                )
            )
            for port_id in port_ids
        )
    ):
        if port.name not in new_workflow.ports.keys():
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Port {port.name} loaded in the recovery workflow of failed job {failed_job.name}"
                )
            new_workflow.ports[port.name] = port
        elif logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Port {port.name} exists in the recovery workflow of failed job {failed_job.name}"
            )


def _missing_dependency_ports(
    dependencies: MutableMapping[str, str], port_names: Iterable[str]
) -> MutableSet[str]:
    return {
        dependency
        for dependency, port_name in dependencies.items()
        if port_name not in port_names
    }


async def load_missing_ports(
    new_workflow: Workflow,
    step_name_id: MutableMapping[str, int],
    workflow_builder: WorkflowBuilder,
) -> None:
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
                        f"Added port {step.output_ports[dependency_row['name']]} because needed in the step {step.name}"
                    )
                    missing_ports.add(dependency_row["port"])
    for port in await asyncio.gather(
        *(
            asyncio.create_task(workflow_builder.load_port(new_workflow.context, p_id))
            for p_id in missing_ports
        )
    ):
        new_workflow.ports[port.name] = port
