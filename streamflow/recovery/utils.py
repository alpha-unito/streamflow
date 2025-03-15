from __future__ import annotations

import asyncio
import logging
import posixpath
from collections.abc import Iterable, MutableMapping, MutableSequence, MutableSet

from streamflow.core.exception import FailureHandlingException
from streamflow.core.utils import get_class_fullname, get_tag
from streamflow.core.workflow import Job, Port, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.port import FilterTokenPort, InterWorkflowPort
from streamflow.workflow.step import ExecuteStep
from streamflow.workflow.token import JobToken


async def get_output_tokens(next_token_ids, context) -> MutableSet[int]:
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


def get_port_from_token(
    token: Token, port_tokens: MutableMapping[str, int], token_visited: MutableSet[int]
) -> str:
    for port_name, token_ids in port_tokens.items():
        if token.tag in (token_visited[t_id][0].tag for t_id in token_ids):
            return port_name
    raise FailureHandlingException(f"Token id {token.persistent_id} is missing")


async def execute_recover_workflow(new_workflow: Workflow, failed_step: Step) -> None:
    if len(new_workflow.steps) == 0:
        if logger.isEnabledFor(logging.INFO):
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


async def populate_workflow(
    step_ids: Iterable[int],
    failed_step: Step,
    new_workflow: Workflow,
    workflow_builder: WorkflowBuilder,
    failed_job: Job,
) -> None:
    await asyncio.gather(
        *(
            asyncio.create_task(
                workflow_builder.load_step(new_workflow.context, step_id)
            )
            for step_id in step_ids
        )
    )

    # Add failed step into new_workflow
    await workflow_builder.load_step(
        new_workflow.context,
        failed_step.persistent_id,
    )
    # Create output port of the failed step in the new workflow
    for port in failed_step.get_output_ports().values():
        stop_tag = get_tag(failed_job.inputs.values())
        new_port = InterWorkflowPort(
            FilterTokenPort(new_workflow, port.name, stop_tags=[stop_tag])
        )
        new_port.add_inter_port(port, stop_tag)
        # todo: make an abstract class of Port and change the type hint of the workflow ports attribute
        new_workflow.ports[new_port.name] = new_port


class PortRecovery:
    def __init__(self, port: Port):
        self.port: Port = port
        self.waiting_token: int = 0


class RetryRequest:
    def __init__(self):
        self.version: int = 1
        self.job_token: JobToken | None = None
        self.token_output: MutableMapping[str, Token] = {}
        self.lock: asyncio.Lock = asyncio.Lock()
        self.is_running: bool = False
        # Other workflows can queue to the output port of the step while the job is running.
        self.queue: MutableSequence[PortRecovery] = []
        self.workflow: Workflow | None = None
