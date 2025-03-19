from __future__ import annotations

import logging
from collections.abc import MutableMapping, MutableSet

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException
from streamflow.core.recovery import PortRecovery, RetryRequest
from streamflow.core.workflow import Job, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.recovery.rollback_recovery import (
    DirectGraph,
    GraphMapper,
    ProvenanceGraph,
    TokenAvailability,
    create_graph_mapper,
)
from streamflow.recovery.utils import get_output_tokens, populate_workflow
from streamflow.workflow.port import FilterTokenPort, InterWorkflowPort
from streamflow.workflow.step import ScatterStep
from streamflow.workflow.token import (
    IterationTerminationToken,
    JobToken,
    TerminationToken,
)
from streamflow.workflow.utils import get_job_token


def _add_wait(
    retry_request: RetryRequest,
    port_name: str,
    workflow: Workflow,
    port_recovery: PortRecovery | None = None,
) -> PortRecovery:
    if port_recovery is None:
        if (port := workflow.ports.get(port_name)) is None:
            port = InterWorkflowPort(
                FilterTokenPort(
                    workflow,
                    port_name,
                )
            )
            port_recovery = PortRecovery(port)
            retry_request.queue.append(port_recovery)
        elif not isinstance(port, InterWorkflowPort):
            raise FailureHandlingException(f"Port {port} must be a InterWorkflowPort")
        elif (
            port_recovery := next(
                (p.port.workflow == port.workflow for p in retry_request.queue), None
            )
        ) is None:
            raise FailureHandlingException(f"Port {port} is not in the queue")
    port_recovery.waiting_token += 1
    return port_recovery


class RollbackRecoveryPolicy:
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    async def _sync_running_jobs(
        self,
        retry_request: RetryRequest,
        job_token: JobToken,
        mapper: GraphMapper,
        workflow: Workflow,
        job_ports: MutableMapping[str, PortRecovery],
    ) -> None:
        """
        The `retry_request` is the current job running, instead the `job_token`
        is the token to remove in the graph because the workflow will depend
        on the already running job
        """
        job_name = job_token.value.name
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Synchronize rollbacks: job {job_name} is running")
        # todo: create a unit test for this case and check if it works well
        for output_port_name in await mapper.get_output_ports(job_token):
            port_recovery = _add_wait(
                retry_request,
                output_port_name,
                workflow,
                job_ports.get(job_name, None),
            )
            job_ports.setdefault(job_name, port_recovery)
            workflow.ports[port_recovery.port.name] = port_recovery.port
        # Remove tokens recovered in other workflows
        for token_id in await get_output_tokens(
            [
                t
                for t in mapper.dag_tokens.succ(job_token.persistent_id)
                if t not in (DirectGraph.ROOT, DirectGraph.LEAF)
            ],
            self.context,
        ):
            mapper.remove_token(token_id, preserve_token=True)

    async def recover_workflow(self, failed_job: Job, failed_step: Step) -> Workflow:
        workflow = failed_step.workflow
        workflow_builder = WorkflowBuilder(deep_copy=False)
        new_workflow = await workflow_builder.load_workflow(
            workflow.context, workflow.persistent_id
        )
        # Retrieve tokens
        provenance = ProvenanceGraph(workflow.context)
        job_token = get_job_token(
            failed_job.name, failed_step.get_input_port("__job__").token_list
        )
        await provenance.build_graph(
            inputs=(
                job_token,
                *failed_job.inputs.values(),
            )
        )
        mapper = await create_graph_mapper(self.context, provenance)
        # Synchronize across multiple recovery workflows
        await self.sync_workflows(mapper, new_workflow)
        # Populate new workflow
        steps = await mapper.get_port_and_step_ids(failed_step.output_ports.values())
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

    async def sync_workflows(self, mapper: GraphMapper, workflow: Workflow) -> None:
        job_ports = {}
        for job_token in list(
            # todo: visit the jobtoken bottom-up in the graph
            filter(lambda t: isinstance(t, JobToken), mapper.token_instances.values())
        ):
            job_name = job_token.value.name
            retry_request = self.context.failure_manager.get_request(job_name)
            if (
                is_available := await self.context.failure_manager.is_recovered(
                    job_name
                )
            ) == TokenAvailability.FutureAvailable:
                await self._sync_running_jobs(
                    retry_request, job_token, mapper, workflow, job_ports
                )
            elif is_available == TokenAvailability.Available:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Synchronize rollbacks: job {job_token.value.name} output available"
                    )
                # todo: create a unit test with a complex graph where the nodes are removed
                # Search execute token after job token, replace this token with job_req token.
                # Then remove all the prev tokens
                for port_name in await mapper.get_output_ports(job_token):
                    new_token = retry_request.output_tokens[port_name]
                    await mapper.replace_token(
                        port_name,
                        new_token,
                        True,
                    )
                    mapper.remove_token(new_token.persistent_id, preserve_token=True)
            else:
                await self.context.failure_manager.update_request(job_name)
                retry_request.workflow = workflow


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
                logger.debug(f"Injecting token {token.tag} of port {port.name}")
            port.put(token)
        if len(port.token_list) > 0 and len(port.token_list) == len(
            port_tokens[port_name]
        ):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Injecting termination token on port {port.name}")
            port.put(TerminationToken())


async def _set_step_states(new_workflow: Workflow, mapper: GraphMapper) -> None:
    for step in new_workflow.steps.values():
        if isinstance(step, ScatterStep):
            port = step.get_output_port()
            missing_tokens = tuple(
                mapper.token_instances[token_id].tag
                for token_id in mapper.port_tokens[port.name]
                if not mapper.token_available[token_id]
            )
            new_workflow.ports[port.name] = FilterTokenPort(
                new_workflow,
                port.name,
                filter_function=lambda t, tokens=missing_tokens: t.tag in tokens,
            )
            new_workflow.ports[port.name].token_list = port.token_list
