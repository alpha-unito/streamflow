from __future__ import annotations

import asyncio
import logging
import os
import posixpath
from collections.abc import Iterable
from typing import cast

import graphviz

from streamflow.core.exception import FailureHandlingException
from streamflow.core.recovery import RecoveryPolicy
from streamflow.core.utils import get_tag
from streamflow.core.workflow import Job, Status, Step, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.recovery.utils import (
    DirectGraph,
    GraphMapper,
    ProvenanceGraph,
    TokenAvailability,
    create_graph_mapper,
)
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.port import (
    ConnectorPort,
    FilterTokenPort,
    InterWorkflowPort,
    JobPort,
)
from streamflow.workflow.step import ScatterStep
from streamflow.workflow.token import (
    IterationTerminationToken,
    JobToken,
    TerminationToken,
)
from streamflow.workflow.utils import get_job_token


def graph_figure_bipartite(graph, steps, ports, title):
    dot = graphviz.Digraph(title)
    for vertex, neighbors in graph.items():
        shape = "ellipse" if vertex in steps else "box"
        dot.node(str(vertex), shape=shape, color="black" if neighbors else "red")
        for n in neighbors:
            dot.edge(str(vertex), str(n))
    filepath = title + ".gv"
    dot.render(filepath)
    os.system("rm " + filepath)


def dag_workflow(workflow, title="wf"):
    dag = {}
    ports = set()
    steps = set()
    for step in workflow.steps.values():
        steps.add(step.name)
        for port_name in step.output_ports.values():
            dag.setdefault(step.name, set()).add(port_name)
            ports.add(port_name)
        for port_name in step.input_ports.values():
            dag.setdefault(port_name, set()).add(step.name)
            ports.add(port_name)
    graph_figure_bipartite(dag, steps, ports, title + "-recovery-bipartite")


async def _execute_recover_workflow(new_workflow: Workflow, failed_step: Step) -> None:
    if len(new_workflow.steps) == 0:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Workflow {new_workflow.name} is empty. "
                f"Waiting output ports {list(failed_step.output_ports.values())}"
            )
        if set(new_workflow.ports.keys()) != set(failed_step.output_ports.values()):
            raise FailureHandlingException("Recovered workflow construction invalid")
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


async def _inject_tokens(mapper: GraphMapper, new_workflow: Workflow) -> None:
    for port_name in mapper.dcg_port[DirectGraph.ROOT]:
        token_list = sorted(
            [
                mapper.token_instances[token_id]
                for token_id in mapper.port_tokens[port_name]
                if token_id not in (DirectGraph.ROOT, DirectGraph.LEAF)
                and mapper.token_available[token_id]
            ],
            key=lambda x: x.tag,
        )
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
            mapper.port_tokens[port_name]
        ):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Injecting termination token on port {port.name}")
            port.put(TerminationToken(Status.SKIPPED))


async def _populate_workflow(
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
    # Instantiate ports capable of moving tokens across workflows
    for port in new_workflow.ports.values():
        if not isinstance(port, (JobPort, ConnectorPort)):
            new_workflow.create_port(InterWorkflowPort, port.name)
    for port in failed_step.get_output_ports().values():
        cast(InterWorkflowPort, new_workflow.ports[port.name]).add_inter_port(
            port, border_tag=get_tag(failed_job.inputs.values())
        )


async def _set_step_states(mapper: GraphMapper, new_workflow: Workflow) -> None:
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


class RollbackRecoveryPolicy(RecoveryPolicy):
    async def _recover_workflow(self, failed_job: Job, failed_step: Step) -> Workflow:
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
        await self._sync_workflows(mapper, new_workflow)
        # Populate new workflow
        steps = await mapper.get_port_and_step_ids(failed_step.output_ports.values())
        await _populate_workflow(
            steps, failed_step, new_workflow, workflow_builder, failed_job
        )
        await _inject_tokens(mapper, new_workflow)
        await _set_step_states(mapper, new_workflow)
        return new_workflow

    async def _sync_workflows(self, mapper: GraphMapper, workflow: Workflow) -> None:
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
                # The `retry_request` is the current job running, instead
                # the `job_token` is the token to remove in the graph because
                # the workflow will depend on the already running job
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Synchronize rollbacks: job {job_name} is running")
                # todo: create a unit test for this case
                for port_name in await mapper.get_output_ports(job_token):
                    if port_name in retry_request.workflow.ports.keys():
                        cast(
                            InterWorkflowPort, retry_request.workflow.ports[port_name]
                        ).add_inter_port(
                            workflow.create_port(cls=InterWorkflowPort, name=port_name)
                        )
                # Remove tokens recovered in other workflows
                for token_id in await mapper.get_output_tokens(job_token.persistent_id):
                    mapper.remove_token(token_id, preserve_token=True)
            elif is_available == TokenAvailability.Available:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Synchronize rollbacks: job {job_token.value.name} output available"
                    )
                # Search execute token after job token, replace this token with job_req token.
                # Then remove all the prev tokens
                for port_name in await mapper.get_output_ports(job_token):
                    if port_name in retry_request.output_tokens.keys():
                        new_token = retry_request.output_tokens[port_name]
                        await mapper.replace_token(
                            port_name,
                            new_token,
                            True,
                        )
                        mapper.remove_token(
                            new_token.persistent_id, preserve_token=True
                        )
            else:
                await self.context.failure_manager.update_request(job_name)
                retry_request.workflow = workflow

    async def recover(self, failed_job: Job, failed_step: Step) -> None:
        # Create recover workflow
        new_workflow = await self._recover_workflow(failed_job, failed_step)
        dag_workflow(new_workflow)
        # Execute new workflow
        await _execute_recover_workflow(new_workflow, failed_step)
