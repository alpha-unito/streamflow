from __future__ import annotations

import logging
from collections.abc import MutableMapping, MutableSet
from functools import cmp_to_key

from streamflow.core.exception import FailureHandlingException
from streamflow.core.utils import compare_tags
from streamflow.core.workflow import Job, Port, Step, Token, Workflow
from streamflow.cwl.transformer import ForwardTransformer
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.recovery.rollback_recovery import (
    DirectGraph,
    GraphHomomorphism,
    ProvenanceGraph,
    ProvenanceToken,
    TokenAvailability,
    create_graph_homomorphism,
)
from streamflow.recovery.utils import (
    get_execute_step_out_token_ids,
    get_step_instances_from_output_port,
    increase_tag,
)
from streamflow.workflow.port import (
    ConnectorPort,
    FilterTokenPort,
    InterWorkflowPort,
    JobPort,
)
from streamflow.workflow.step import (
    LoopCombinatorStep,
    ScatterStep,
)
from streamflow.workflow.token import (
    IterationTerminationToken,
    JobToken,
    TerminationToken,
)


class PortRecovery:
    def __init__(self, port: Port):
        self.port: Port = port
        self.waiting_token: int = 1


def get_skip_tags(rdwp, port_name, stop_tag):
    if stop_tag and (
        valid_tokens := (
            rdwp.port_tokens[port_name] if port_name in rdwp.port_tokens else []
        )
    ):
        valid_tags = [rdwp.token_instances[t].tag for t in valid_tokens]
        sorted_tags = sorted(valid_tags, key=cmp_to_key(compare_tags))
        curr_tag = ".".join(stop_tag.split(".")[:-1])
        skip_tags = [curr_tag] if curr_tag not in sorted_tags else []
        curr_tag += ".0"
        i = 0
        while True:
            if i == 30:
                raise Exception("Stop tag too long")
            i += 1
            if curr_tag == stop_tag:
                break
            elif (
                compare_tags(curr_tag, sorted_tags[0]) < 0
            ):  # curr_tag not in sorted_tags:
                skip_tags.append(curr_tag)
            curr_tag = increase_tag(curr_tag)
            if curr_tag is None:
                raise Exception("Curr tar is None")
    else:
        skip_tags = []
    return skip_tags


class RollbackRecoveryPolicy:
    def __init__(self, context):
        self.context = context

    async def recover_workflow(self, failed_job: Job, failed_step: Step):
        workflow = failed_step.workflow
        workflow_builder = WorkflowBuilder(deep_copy=False)
        new_workflow = await workflow_builder.load_workflow(
            workflow.context, workflow.persistent_id
        )

        provenance = ProvenanceGraph(workflow.context)
        # TODO: add a data_manager to store the file checked. Before to check directly a file,
        #  search in this data manager if the file was already checked and return its availability.
        #  It is helpful for performance reason but also for consistency. If the first time that the
        #  file is checked exists, and the second time is lost can create invalid state of the graph
        await provenance.build_graph(inputs=failed_job.inputs.values())
        mapper = await create_graph_homomorphism(
            self.context, provenance, failed_step.get_output_ports().values()
        )

        # Update class state (attributes) and jobs synchronization
        logger.debug("Start sync-rollbacks")
        await self.sync_running_jobs(mapper, new_workflow)

        logger.debug("End sync-rollbacks")
        ports, steps = await mapper.get_port_and_step_ids(
            failed_step.output_ports.values()
        )
        await mapper.populate_workflow(
            ports, steps, failed_step, new_workflow, workflow_builder, failed_job
        )
        logger.debug("end populate")

        last_iteration = await _new_put_tokens(
            new_workflow,
            mapper.dcg_port[DirectGraph.ROOT],
            mapper.port_tokens,
            mapper.token_instances,
            mapper.token_available,
        )
        logger.debug("end _put_tokens")

        await _new_set_steps_state(new_workflow, mapper)

        return new_workflow, last_iteration

    def add_waiter(self, job_name, port_name, workflow, rdwp, port_recovery=None):
        if port_recovery:
            port_recovery.waiting_token += 1
        else:
            max_tag = get_max_tag(
                {
                    rdwp.token_instances[t_id]
                    for t_id in rdwp.port_tokens.get(port_name, [])
                    if t_id > 0
                }
            )
            if max_tag is None:
                max_tag = "0"
            stop_tag = increase_tag(max_tag)
            logger.info(
                f"Wf {workflow.name} added PortRecovery on port {port_name} with stop_tag: {stop_tag}. "
                f"Is Port created? {port_name not in workflow.ports.keys()}"
            )
            port = workflow.ports.get(
                port_name,
                InterWorkflowPort(
                    FilterTokenPort(
                        workflow,
                        port_name,
                        [stop_tag],
                        get_skip_tags(rdwp, port_name, stop_tag),
                    )
                ),
            )
            port_recovery = PortRecovery(port)
            self.context.failure_manager.retry_requests[job_name].queue.append(
                port_recovery
            )
        return port_recovery

    async def sync_running_jobs(self, mapper: GraphHomomorphism, workflow: Workflow):
        map_job_port = {}
        logger.debug("Start synchronization")
        for job_token in [
            token
            for token in mapper.token_instances.values()
            if isinstance(token, JobToken)
        ]:
            retry_request = self.context.failure_manager.get_retry_request(
                job_token.value.name
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
                for output_port_name in await mapper.get_execute_output_port_names(
                    job_token
                ):
                    port_recovery = self.add_waiter(
                        job_token.value.name,
                        output_port_name,
                        workflow,
                        mapper,
                        map_job_port.get(job_token.value.name, None),
                    )
                    map_job_port.setdefault(job_token.value.name, port_recovery)
                    workflow.ports[port_recovery.port.name] = port_recovery.port
                execute_step_out_token_ids = await get_execute_step_out_token_ids(
                    mapper.dag_tokens.succ(job_token.persistent_id),
                    self.context,
                )
                for t_id in execute_step_out_token_ids:
                    for t_id_dead_path in mapper.remove_token_prev_links(t_id):
                        mapper.remove_token(t_id_dead_path)
            elif is_available == TokenAvailability.Available:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Synchronize rollbacks: job {job_token.value.name} output available"
                    )
                # todo: create a unit test for this case and check if it works well
                # Search execute token after job token, replace this token with job_req token.
                # Then remove all the prev tokens
                for port_name in await mapper.get_execute_output_port_names(job_token):
                    new_token = retry_request.token_output[port_name]
                    port_row = await self.context.database.get_port_from_token(
                        new_token.persistent_id
                    )
                    step_rows = await get_step_instances_from_output_port(
                        port_row["id"], self.context
                    )
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
        logger.debug("End synchronization")


async def _new_put_tokens(
    new_workflow: Workflow,
    init_ports: MutableSet[str],
    port_tokens: MutableMapping[str, MutableSet[int]],
    token_instances: MutableMapping[int, Token],
    token_available: MutableMapping[int, bool],
):
    for port_name in init_ports:
        token_list = [
            token_instances[t_id]
            for t_id in port_tokens[port_name]
            if isinstance(t_id, int) and token_available[t_id]
        ]
        token_list.sort(key=lambda x: x.tag, reverse=False)
        for i, t in enumerate(token_list):
            for t1 in token_list[i:]:
                if t.persistent_id != t1.persistent_id and t.tag == t1.tag:
                    raise FailureHandlingException(
                        f"Port {port_name} has tag {t.tag} ripetuto - id_a: {t.persistent_id} id_b: {t1.persistent_id}"
                    )
        port = new_workflow.ports[port_name]
        is_back_prop_output_port = any(
            s
            for s in port.get_input_steps()
            if isinstance(s, ForwardTransformer)  # TODO
            # BackPropagationTransformer)
        )
        loop_combinator_input = any(
            s for s in port.get_output_steps() if isinstance(s, LoopCombinatorStep)
        )

        for t in token_list:
            if isinstance(t, (TerminationToken, IterationTerminationToken)):
                raise FailureHandlingException(
                    f"Added {type(t)} into port {port.name} but it is wrong"
                )
            port.put(t)
            if is_back_prop_output_port:
                logger.debug(
                    f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, "
                    f"insert termination token (it is output port of a Back-prop)"
                )
                port.put(TerminationToken())
                break

        len_port_token_list = len(port.token_list)
        len_port_tokens = len(port_tokens[port_name])

        if len_port_token_list > 0 and len_port_token_list == len_port_tokens:
            if loop_combinator_input and not is_back_prop_output_port:
                if port.token_list[-1].tag != "0":
                    increased_tag = increase_tag(port.token_list[-1].tag)
                    # increased_tag = ".".join(
                    #     (
                    #         *port.token_list[-1].tag.split(".")[:-1],
                    #         str(int(port.token_list[-1].tag.split(".")[-1]) + 1),
                    #     )
                    # )
                    port.put(Token(value=None, tag=increased_tag))
                logger.debug(
                    f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, "
                    f"inserts IterationTerminationToken with tag {port.token_list[-1].tag}"
                )
                port.put(IterationTerminationToken(port.token_list[-1].tag))
                logger.debug(
                    f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, "
                    f"inserts IterationTerminationToken with tag {port.token_list[0].tag}"
                )
                port.put(IterationTerminationToken(port.token_list[0].tag))
            if not any(t for t in port.token_list if isinstance(t, TerminationToken)):
                logger.debug(
                    f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, insert termination token"
                )
                port.put(TerminationToken())
        else:
            logger.debug(
                f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, does NOT insert "
                f"manually TerminationToken. Is there Termination? "
                f"{any(isinstance(t, TerminationToken) for t in port.token_list )}"
            )
    return None


async def _new_set_steps_state(new_workflow, rdwp):
    # dt = str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
    # dir_path = f"graphs/set_steps_state/{dt}-{new_workflow.name}"
    # dag_workflow(new_workflow, dir_path + "/new-wf")
    for step in new_workflow.steps.values():
        if isinstance(step, ScatterStep):
            port = step.get_output_port()
            str_t = [
                (t_id, rdwp.token_instances[t_id].tag, rdwp.token_available[t_id])
                for t_id in rdwp.port_tokens[port.name]
            ]
            logger.debug(
                f"_set_scatter_inner_state: wf {new_workflow.name} -> "
                f"port_tokens[{step.get_output_port().name}]: {str_t}"
            )
            for t_id in rdwp.port_tokens[port.name]:
                logger.debug(
                    f"_set_scatter_inner_state: Token {t_id} is necessary to rollback the scatter on port {port.name} "
                    f"(wf {new_workflow.name}). It is {'' if rdwp.token_available[t_id] else 'NOT '}available"
                )
                # a possible control can be if not token_visited[t_id][1]: then add in valid tags
                if step.valid_tags is None:
                    step.valid_tags = {rdwp.token_instances[t_id].tag}
                else:
                    step.valid_tags.add(rdwp.token_instances[t_id].tag)
        elif isinstance(step, LoopCombinatorStep):
            port = list(step.get_input_ports().values()).pop()
            token = port.token_list[0]

            prefix = ".".join(token.tag.split(".")[:-1])
            if prefix != "":
                step.combinator.iteration_map[prefix] = int(token.tag.split(".")[-1])
                logger.debug(
                    f"recover_jobs-last_iteration: Step {step.name} combinator updated "
                    f"map[{prefix}] = {step.combinator.iteration_map[prefix]}"
                )
        # if not isinstance(
        #     step, (BackPropagationTransformer, CombinatorStep, ConditionalStep)
        # ):
        #     max_tag = "0"
        #     for port_name in step.input_ports.values():
        #         curr_tag = get_max_tag(
        #             {
        #                 rdwp.token_instances[t_id]
        #                 for t_id in rdwp.port_tokens.get(port_name, [])
        #                 if t_id > 0
        #             }
        #         )
        #         if curr_tag and compare_tags(curr_tag, max_tag) == 1:
        #             max_tag = curr_tag
        #     if max_tag != "0":
        #         step.token_tag_stop = ".".join(
        #             (*max_tag.split(".")[:-1], str(int(max_tag.split(".")[-1]) + 1))
        #         )
        #         logger.info(
        #             f"wf {new_workflow.name}. Step {step.name}. token tag stop {step.token_tag_stop}"
        #         )
        #     pass
        # max_tag = "0"
        # for port_name in step.input_ports.values():
        #     curr_tag = get_max_tag(
        #         {
        #             rdwp.token_instances[t_id]
        #             for t_id in rdwp.port_tokens.get(port_name, [])
        #             if t_id > 0
        #         }
        #     )
        #     if curr_tag and compare_tags(curr_tag, max_tag) == 1:
        #         max_tag = curr_tag
        for port_name, port in new_workflow.ports.items():
            if not isinstance(
                port, (ConnectorPort, JobPort, FilterTokenPort, InterWorkflowPort)
            ):
                max_tag = get_max_tag(
                    {
                        rdwp.token_instances[t_id]
                        for t_id in rdwp.port_tokens.get(port_name, [])
                        if t_id > 0
                    }
                )
                if max_tag is None:
                    max_tag = "0"
                stop_tag = increase_tag(max_tag)
                new_workflow.ports[port_name] = FilterTokenPort(
                    new_workflow,
                    port_name,
                    [stop_tag] if stop_tag else [],
                    get_skip_tags(rdwp, port.name, stop_tag),
                )
                new_workflow.ports[port_name].token_list = port.token_list
                logger.info(
                    f"wf {new_workflow.name}. "
                    f"Port {port.name}. "
                    f"token tag stop {stop_tag}. "
                    f"token tag skip {new_workflow.ports[port_name].skip_tags}"
                )
    logger.info("end _new_set_steps_state")


def get_max_tag(token_list):
    max_tag = None
    for t in token_list:
        if max_tag is None or compare_tags(t.tag, max_tag) == 1:
            max_tag = t.tag
    return max_tag


class JobVersion:
    __slots__ = ("job", "outputs", "step", "version")

    def __init__(
        self,
        job: Job = None,
        outputs: MutableMapping[str, Token] | None = None,
        step: Step = None,
        version: int = 1,
    ):
        self.job: Job = job
        self.outputs: MutableMapping[str, Token] | None = outputs
        self.step: Step = step
        self.version: int = version
