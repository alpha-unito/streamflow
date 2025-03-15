from __future__ import annotations

import logging
from collections.abc import MutableMapping, MutableSet

from streamflow.core.exception import FailureHandlingException
from streamflow.core.utils import get_max_tag, increase_tag
from streamflow.core.workflow import Job, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.persistence.utils import get_step_rows
from streamflow.recovery.rollback_recovery import (
    DirectGraph,
    GraphHomomorphism,
    ProvenanceGraph,
    ProvenanceToken,
    TokenAvailability,
    create_graph_homomorphism,
)
from streamflow.recovery.utils import (
    PortRecovery,
    RetryRequest,
    get_output_tokens,
    populate_workflow,
)
from streamflow.workflow.port import FilterTokenPort, InterWorkflowPort
from streamflow.workflow.step import ScatterStep
from streamflow.workflow.token import (
    IterationTerminationToken,
    JobToken,
    TerminationToken,
)


class RollbackRecoveryPolicy:
    def __init__(self, context):
        self.context = context

    async def recover_workflow(self, failed_job: Job, failed_step: Step) -> Workflow:
        workflow = failed_step.workflow
        workflow_builder = WorkflowBuilder(deep_copy=False)
        new_workflow = await workflow_builder.load_workflow(
            workflow.context, workflow.persistent_id
        )
        # Retrieve tokens
        provenance = ProvenanceGraph(workflow.context)
        # TODO: add a data_manager to store the file checked. Before to check directly a file,
        #  search in this data manager if the file was already checked and return its availability.
        #  It is helpful for performance reason but also for consistency. If the first time that the
        #  file is checked exists, and the second time is lost can create invalid state of the graph
        await provenance.build_graph(inputs=failed_job.inputs.values())
        mapper = await create_graph_homomorphism(
            self.context, provenance, failed_step.get_output_ports().values()
        )
        # Synchronize across multiple recovery workflows
        await self.sync_running_jobs(mapper, new_workflow)
        # Populate new workflow
        ports, steps = await mapper.get_port_and_step_ids(
            failed_step.output_ports.values()
        )
        await populate_workflow(
            steps, failed_step, new_workflow, workflow_builder, failed_job
        )
        await _inject_tokens(
            new_workflow,
            mapper.dcg_port[DirectGraph.ROOT],
            mapper.port_tokens,
            mapper.token_instances,
            mapper.token_available,
        )
        await _set_step_states(new_workflow, mapper)
        return new_workflow

    def add_waiter(
        self,
        job_name: str,
        port_name: str,
        workflow: Workflow,
        mapper: GraphHomomorphism,
        port_recovery: PortRecovery | None = None,
    ) -> PortRecovery:
        if port_recovery is None:
            if (port := workflow.ports.get(port_name)) is not None:
                # todo: to check
                max_tag = (
                    get_max_tag(
                        {
                            mapper.token_instances[t_id]
                            for t_id in mapper.port_tokens.get(port_name, [])
                            if t_id > 0
                        }
                    )
                    or "0"
                )
                stop_tag = increase_tag(max_tag)
                port = InterWorkflowPort(
                    FilterTokenPort(
                        workflow,
                        port_name,
                        stop_tags=[stop_tag],
                    )
                )
            port_recovery = PortRecovery(port)
            self.context.failure_manager.retry_requests[job_name].queue.append(
                port_recovery
            )
        port_recovery.waiting_token += 1
        return port_recovery

    async def sync_running_jobs(
        self, mapper: GraphHomomorphism, workflow: Workflow
    ) -> None:
        map_job_port = {}
        for job_token in [
            token
            for token in mapper.token_instances.values()
            if isinstance(token, JobToken)
        ]:
            retry_request = self.context.failure_manager.retry_requests.setdefault(
                job_token.value.name, RetryRequest()
            )
            if (
                is_available := await self.context.failure_manager.is_running_token(
                    job_token
                )
            ) == TokenAvailability.FutureAvailable:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Synchronize rollbacks: job {job_token.value.name} is running"
                    )
                # todo: create a unit test for this case and check if it works well
                for output_port_name in await mapper.get_output_ports(job_token):
                    port_recovery = self.add_waiter(
                        job_token.value.name,
                        output_port_name,
                        workflow,
                        mapper,
                        map_job_port.get(job_token.value.name, None),
                    )
                    map_job_port.setdefault(job_token.value.name, port_recovery)
                    workflow.ports[port_recovery.port.name] = port_recovery.port
                # Remove tokens recovered in other workflows
                for token_id in await get_output_tokens(
                    mapper.dag_tokens.succ(job_token.persistent_id),
                    self.context,
                ):
                    for t_id_dead_path in mapper.remove_token_prev_links(token_id):
                        mapper.remove_token(t_id_dead_path)
            elif is_available == TokenAvailability.Available:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Synchronize rollbacks: job {job_token.value.name} output available"
                    )
                # todo: create a unit test for this case and check if it works well
                # Search execute token after job token, replace this token with job_req token.
                # Then remove all the prev tokens
                for port_name in await mapper.get_output_ports(job_token):
                    new_token = retry_request.token_output[port_name]
                    port_row = await self.context.database.get_port_from_token(
                        new_token.persistent_id
                    )
                    step_rows = await get_step_rows(port_row["id"], self.context)
                    await mapper.replace_token_and_remove(
                        port_name,
                        ProvenanceToken(
                            new_token,
                            TokenAvailability.Available,
                            port_row,
                            step_rows,
                        ),
                    )
            else:
                async with retry_request.lock:
                    retry_request.is_running = True
                    retry_request.job_token = None
                    retry_request.token_output = {}
                    retry_request.workflow = workflow
                await self.context.failure_manager.update_job_status(
                    job_token.value.name
                )
            # End lock
        # End for job token


async def _inject_tokens(
    new_workflow: Workflow,
    init_ports: MutableSet[str],
    port_tokens: MutableMapping[str, MutableSet[int]],
    token_instances: MutableMapping[int, Token],
    token_available: MutableMapping[int, bool],
) -> None:
    for port_name in init_ports:
        token_list = [
            token_instances[token_id]
            for token_id in port_tokens[port_name]
            if token_id not in (DirectGraph.ROOT, DirectGraph.LEAF)
            and token_available[token_id]
        ]
        token_list.sort(key=lambda x: x.tag, reverse=False)
        if len(
            tags := {(token.persistent_id, token.tag) for token in token_list}
        ) != len(token_list):
            raise FailureHandlingException(
                f"Port {port_name} has multiple tokens with same tag (id, tag): {tags}"
            )
        if any(
            isinstance(token, (TerminationToken, IterationTerminationToken))
            for token in token_list
        ):
            raise FailureHandlingException("Impossible to load a TerminationToken")
        port = new_workflow.ports[port_name]
        for token in token_list:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Injecting token {token.persistent_id}: {token.tag} of port {port.name}"
                )
            port.put(token)
        if len(port.token_list) > 0 and len(port.token_list) == len(
            port_tokens[port_name]
        ):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Injecting termination token on port {port.name}")
            port.put(TerminationToken())


async def _set_step_states(new_workflow: Workflow, mapper: GraphHomomorphism):
    for step in new_workflow.steps.values():
        if isinstance(step, ScatterStep):
            port = step.get_output_port()
            new_workflow.ports[port.name] = FilterTokenPort(
                new_workflow,
                port.name,
                valid_tags=[
                    mapper.token_instances[token_id].tag
                    for token_id in mapper.port_tokens[port.name]
                    if not mapper.token_available[token_id]
                ],
            )
            new_workflow.ports[port.name].token_list = port.token_list
