from __future__ import annotations

import asyncio
import logging
import posixpath
from collections.abc import Iterable
from typing import cast

from streamflow.core.exception import FailureHandlingException
from streamflow.core.recovery import RecoveryPolicy
from streamflow.core.utils import get_job_step_name, get_job_tag, get_tag
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
    InterWorkflowJobPort,
    InterWorkflowPort,
    JobPort,
)
from streamflow.workflow.step import ConditionalStep, LoopCombinatorStep, ScatterStep
from streamflow.workflow.token import (
    IterationTerminationToken,
    JobToken,
    TerminationToken,
)
from streamflow.workflow.utils import get_job_token


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
        # The port is the input port of a loop and there is no input steps before the loop
        # nb. Why three input steps? Input loop step, back propagation step and loop-termination step
        if (
            any(isinstance(s, LoopCombinatorStep) for s in port.get_output_steps())
            and not len(port.get_input_steps()) < 3
        ):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Injecting termination token on port {port.name}")
            port.put(TerminationToken(Status.SKIPPED))
        if len(port.token_list) > 0 and len(port.token_list) == len(
            mapper.port_tokens[port_name]
        ):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Injecting termination token on port {port.name}")
            port.put(TerminationToken(Status.SKIPPED))
    for port_ in new_workflow.ports.values():
        if (
            len(port_.get_input_steps()) == 0
            and not any(isinstance(t, TerminationToken) for t in port_.token_list)
            and len(port_.get_input_steps()) > 0
        ):
            raise FailureHandlingException(
                f"Port {port_.name} is an input but have not a termination token"
            )


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
    # Add the failed step into new_workflow
    await workflow_builder.load_step(
        new_workflow.context,
        failed_step.persistent_id,
    )
    # Instantiate ports capable of moving tokens across workflows
    for port in list(new_workflow.ports.values()):
        if len(port.get_input_steps()) == 0 and len(port.get_output_steps()) == 0:
            attached = False
            for s in new_workflow.steps.values():
                if isinstance(s, ConditionalStep):
                    if attached := port.name in (
                        p.name for p in s.get_skip_ports().values()
                    ):
                        break
            if not attached:
                logger.debug(f"Removing port {port.name}")
                new_workflow.ports.pop(port.name)
                continue
        if not isinstance(port, (InterWorkflowPort, InterWorkflowJobPort)):
            if isinstance(port, JobPort):
                new_workflow.create_port(
                    InterWorkflowJobPort, port.name, interrupt=True
                )
            elif not isinstance(port, ConnectorPort):
                new_workflow.create_port(InterWorkflowPort, port.name, interrupt=True)
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
        elif isinstance(step, LoopCombinatorStep):
            if (
                len(
                    tags := {
                        p.token_list[0].tag for p in step.get_input_ports().values()
                    }
                )
                != 1
            ):
                raise FailureHandlingException(
                    f"LoopCombinatorStep {step.name} has in input different tags: {tags}"
                )
            await cast(LoopCombinatorStep, step).set_iteration(
                tag=get_tag(p.token_list[0] for p in step.get_input_ports().values())
            )
            logger.info(
                f"iteration_termination_checklist: {step.combinator.iteration_map}"
            )


class RollbackRecoveryPolicy(RecoveryPolicy):
    async def _recover_workflow(self, failed_job: Job, failed_step: Step) -> Workflow:
        workflow = failed_step.workflow
        workflow_builder = WorkflowBuilder(deep_copy=False)
        new_workflow = await workflow_builder.load_workflow(
            workflow.context, workflow.persistent_id
        )
        for port in failed_step.get_output_ports().values():
            await workflow_builder.load_port(
                new_workflow.context,
                port.persistent_id,
            )
        # Retrieve tokens
        provenance = ProvenanceGraph(workflow.context)
        inputs = []
        # todo: Add a parameter in the API containing the input token of the failed step (in a specific job)?
        if job_port := failed_step.get_input_port("__job__"):
            inputs.append(get_job_token(failed_job.name, job_port.token_list))
        if conn_tokens := [
            failed_step.get_input_port(name).token_list[0]
            for name in failed_step.input_ports
            if name.startswith("__connector__")
        ]:
            inputs.extend(conn_tokens)
        inputs.extend(failed_job.inputs.values())
        await provenance.build_graph(inputs=inputs)
        mapper = await create_graph_mapper(self.context, provenance)

        # Synchronize across multiple recovery workflows
        await self._sync_workflows(mapper, new_workflow)
        # Populate new workflow
        steps = await mapper.get_port_and_step_ids(failed_step.output_ports.values())
        await _populate_workflow(
            steps, failed_step, new_workflow, workflow_builder, failed_job
        )
        # dag_workflow(new_workflow)
        await _inject_tokens(mapper, new_workflow)
        await _set_step_states(mapper, new_workflow)
        try:
            for step in workflow.steps.values():
                # Check the step has all the ports
                step.get_input_ports()
                step.get_output_ports()
        except KeyError:
            raise FailureHandlingException("The recovery workflow is malformed")
        for port in new_workflow.ports.values():
            if (
                len(in_steps := [(type(s), s.name) for s in port.get_input_steps()])
                == 0
            ):
                if (
                    len(
                        out_steps := [
                            (type(s), s.name) for s in port.get_output_steps()
                        ]
                    )
                    == 0
                ):
                    # A port without steps. todo: delete it from the workflow
                    continue
                elif (len(port.token_list) < 2) and port.name not in mapper.sync_ports:
                    logger.debug(
                        f"Empty input port: {port.name}. "
                        f"in_tokens: {len(port.token_list)}. in_steps: {in_steps}. out_steps: {out_steps}"
                    )
                if port.token_list and not isinstance(
                    port.token_list[-1], TerminationToken
                ):
                    raise FailureHandlingException(
                        f"Missing termination token: {port.name}"
                    )
        return new_workflow

    async def _sync_workflows(self, mapper: GraphMapper, workflow: Workflow) -> None:
        rollback_jobs = []
        for job_token in set(
            # todo: visit the jobtoken bottom-up in the graph
            filter(lambda _t: isinstance(_t, JobToken), mapper.token_instances.values())
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
                out_ports = []
                num_ports_used = 0
                for port_name in await mapper.get_output_ports(job_token):
                    out_ports.append(port_name)
                    if port_name in retry_request.workflow.ports.keys():
                        num_ports_used += 1
                        missing_tag = get_job_tag(job_name)
                        running_port = retry_request.workflow.ports[port_name]
                        if isinstance(running_port, JobPort):
                            new_port = workflow.create_port(
                                InterWorkflowJobPort, port_name, interrupt=True
                            )
                        else:
                            new_port = workflow.create_port(
                                InterWorkflowPort, port_name, interrupt=True
                            )
                        for t in running_port.token_list:
                            if t.tag == missing_tag:
                                new_port.put(t)
                                break
                        else:
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(
                                    f"Job {job_name} is running. Waiting on the port {port_name}"
                                )
                            retry_request.waiting_ports.setdefault(
                                port_name, []
                            ).append((missing_tag, new_port))
                        mapper.sync_ports.append(port_name)
                steps = [
                    (type(s), s.name)
                    for s in workflow.steps.values()
                    if s.name.startswith(get_job_step_name(job_name))
                ]
                logger.debug(f"Job {job_name} is running. Execute steps: {steps}")
                if len(steps) > 0:
                    if any(
                        len(workflow.ports[p].token_list) == 0
                        for p in out_ports
                        if p in workflow.ports
                    ):
                        empty_ports = [
                            p
                            for p in out_ports
                            if p in workflow.ports
                            and len(workflow.ports[p].token_list) == 0
                        ]
                        raise FailureHandlingException(
                            f"Some input ports are empty for the job {job_name}. "
                            f"Input port used: {num_ports_used}, input port empty {len(empty_ports)}: {empty_ports}"
                        )
                    if len(out_ports) == 0:
                        raise FailureHandlingException(
                            f"job {job_name} is running. Recovery workflow does not wait no ports"
                        )

                # Remove tokens recovered in other workflows
                for token_id in await mapper.get_output_tokens(job_token.persistent_id):
                    logger.debug(
                        f"Job {job_name} is running. Removing token {token_id}"
                    )
                    mapper.remove_token(token_id) #, preserve_token=True)
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
                        mapper.replace_token(
                            port_name,
                            new_token,
                            True,
                        )
                        mapper.remove_token(
                            new_token.persistent_id, preserve_token=True
                        )
            else:
                await self.context.failure_manager.update_request(job_name)
                rollback_jobs.append(job_name)
                retry_request.workflow = workflow
        logger.debug(f"Recovery workflow: {rollback_jobs}")

    async def recover(self, failed_job: Job, failed_step: Step) -> None:
        # Create recover workflow
        new_workflow = await self._recover_workflow(failed_job, failed_step)
        self.context.failure_manager._workflows.append(new_workflow)
        # Execute new workflow
        await _execute_recover_workflow(new_workflow, failed_step)
