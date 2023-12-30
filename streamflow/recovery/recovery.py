from __future__ import annotations


from typing import MutableMapping, MutableSet

from streamflow.core import utils
from streamflow.core.utils import (
    random_name,
    compare_tags,
)
from streamflow.core.exception import FailureHandlingException

from streamflow.core.workflow import Job, Step, Token
from streamflow.cwl.transformer import BackPropagationTransformer
from streamflow.log_handler import logger
from streamflow.recovery.rollback_recovery import (
    NewProvenanceGraphNavigation,
    DirectGraph,
    ProvenanceToken,
    TokenAvailability,
)
from streamflow.recovery.utils import (
    extra_data_print,
    _is_token_available,
    get_execute_step_out_token_ids,
    get_steps_from_output_port,
    increase_tag,
)
from streamflow.workflow.port import (
    ConnectorPort,
    JobPort,
    FilterTokenPort,
    InterWorkflowPort,
)
from streamflow.workflow.step import (
    LoopCombinatorStep,
    ScatterStep,
)
from streamflow.workflow.token import (
    TerminationToken,
    IterationTerminationToken,
    JobToken,
)
from streamflow.core.workflow import Workflow
from streamflow.workflow.utils import get_job_token


class PortRecovery:
    def __init__(self, port):
        self.port = port
        self.waiting_token = 1


class RollbackRecoveryPolicy:
    def __init__(self, context):
        self.context = context

    async def recover_workflow(
        self, failed_job: Job, failed_step: Step, loading_context
    ):
        workflow = failed_step.workflow
        new_workflow = Workflow(
            context=workflow.context,
            type=workflow.type,
            name=random_name(),
            config=workflow.config,
        )

        for port in failed_step.get_output_ports().values():
            # stop_tag = increase_tag(utils.get_tag(failed_job.inputs.values()))
            stop_tag = utils.get_tag(failed_job.inputs.values())
            logger.info(
                f"Wf {new_workflow.name} created output port {port.name} of failed job {failed_job.name} (wf {workflow.name}) and stop_tag: {stop_tag}"
            )
            new_port = InterWorkflowPort(
                FilterTokenPort(new_workflow, port.name, [stop_tag])
            )
            new_port.add_inter_port(port, stop_tag)
            new_workflow.add_port(new_port)

        # should be an impossible case
        if failed_step.persistent_id is None:
            raise FailureHandlingException(
                f"Workflow {workflow.name} has the step {failed_step.name} not saved in the database."
            )

        job_token = get_job_token(
            failed_job.name,
            failed_step.get_input_port("__job__").token_list,
        )
        npgn = NewProvenanceGraphNavigation(workflow.context)
        await npgn.build_unfold_graph((*failed_job.inputs.values(), job_token))

        # update class state (attributes) and jobs synchronization
        inner_graph = await npgn.refold_graphs(failed_step.get_output_ports().values())
        logger.debug("Start sync-rollbacks")
        await self.sync_running_jobs(inner_graph, new_workflow)

        logger.debug("End sync-rollbacks")
        ports, steps = await inner_graph.get_port_and_step_ids(
            failed_step.output_ports.values()
        )
        await inner_graph._populate_workflow(
            ports,
            steps,
            failed_step,
            new_workflow,
            loading_context,
        )
        # _replace_loop_condition(new_workflow, inner_graph)
        if "/subworkflow/i1-back-propagation-transformer" in new_workflow.steps.keys():
            raise FailureHandlingException("Caricata i1-back-prop CHE NON SERVE")
        logger.debug("end populate")

        # for port in failed_step.get_input_ports().values():
        #     if port.name not in new_workflow.ports.keys():
        #         raise FailureHandlingException(
        #             f"La input port {port.name} dello step fallito {failed_step.name} non è presente nel new_workflow {new_workflow.name}"
        #         )
        logger.debug("end save_for_retag")

        last_iteration = await _new_put_tokens(
            new_workflow,
            inner_graph.dcg_port[DirectGraph.INIT_GRAPH_FLAG],
            inner_graph.port_tokens,
            inner_graph.token_instances,
            inner_graph.token_available,
            inner_graph.port_name_ids,
        )
        logger.debug("end _put_tokens")

        await _new_set_steps_state(new_workflow, inner_graph)

        extra_data_print(
            workflow,
            new_workflow,
            None,  # job_rollback,
            {
                t_id: (instance, inner_graph.token_available[t_id])
                for t_id, instance in inner_graph.token_instances.items()
            },
            last_iteration,
        )

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
                f"Wf {workflow.name} added PortRecovery on port {port_name} with stop_tag: {stop_tag}. is Port created? {port_name not in workflow.ports.keys()}"
            )
            port = workflow.ports.get(
                port_name,
                InterWorkflowPort(FilterTokenPort(workflow, port_name, [stop_tag])),
            )
            port_recovery = PortRecovery(port)
            self.context.failure_manager.job_requests[job_name].queue.append(
                port_recovery
            )
        return port_recovery

    async def sync_running_jobs(self, rdwp, workflow):
        logger.debug(f"INIZIO sync (wf {workflow.name}) RUNNING JOBS")
        map_job_port = {}
        job_token_names = [
            token.value.name
            for token in rdwp.token_instances.values()
            if isinstance(token, JobToken)
        ]
        logger.debug(f"TOKEN_GRAPH: {rdwp.dag_tokens}")
        tmp = {
            f'"{k}"': [f'{v}' for v in values]
            for k, values in rdwp.port_tokens.items()
        }
        tmp = (
            "{\n"
            + "\n".join([f"{k} : {values}" for k, values in tmp.items()])
            + "\n}\n"
        )
        logger.debug(f"PORT_TAGS: {tmp}")

        for job_token in [
            token
            for token in rdwp.token_instances.values()
            if isinstance(token, JobToken)
        ]:
            job_request = await self.context.failure_manager.setup_job_request(
                job_token.value.name, default_is_running=False
            )
            async with job_request.lock:
                if job_request.is_running:
                    logger.debug(
                        f"Sync Job {job_token.value.name} (wf {workflow.name}): JobRequest is running on wf {job_request.workflow.name}. Other jobs: {job_token_names}"
                    )
                    output_port_names = await rdwp.get_execute_output_port_names(
                        job_token
                    )
                    logger.debug(f"Lista output_port_names: {output_port_names}")
                    for output_port_name in output_port_names:
                        port_recovery = self.add_waiter(
                            job_token.value.name,
                            output_port_name,
                            workflow,
                            rdwp,
                            map_job_port.get(job_token.value.name, None),
                        )
                        logger.debug(
                            f"Created port {port_recovery.port.name} for wf {workflow.name} with waiting token {port_recovery.waiting_token}"
                        )
                        map_job_port.setdefault(job_token.value.name, port_recovery)
                        workflow.add_port(port_recovery.port)
                    if len(job_token_names) == 1:
                        logger.debug(f"New-workflow {workflow.name} will be empty.")
                        pass
                        # raise FailureHandlingException(
                        #     "There is only job job in this rollback, but it is already running in another workflow. Something is wrong."
                        # )
                    execute_step_out_token_ids = await get_execute_step_out_token_ids(
                        rdwp.dag_tokens.succ(job_token.persistent_id),
                        self.context,
                    )
                    logger.debug(
                        f"Lista execute_step_out_token_ids: {execute_step_out_token_ids}"
                    )
                    for t_id in execute_step_out_token_ids:
                        for t_id_dead_path in rdwp.remove_token_prev_links(t_id):
                            rdwp.remove_token(t_id_dead_path)

                elif job_request.token_output and all(
                    [
                        await _is_token_available(t, self.context)
                        for t in job_request.token_output.values()
                    ]
                ):
                    # search execute token after job token, replace this token with job_requ token. then remove all the prev tokens
                    logger.debug(
                        f"Sync Job {job_token.value.name} (wf {workflow.name}): JobRequest has token_output { {k: v.persistent_id for k, v in job_request.token_output.items()} }"
                    )
                    for port_name in await rdwp.get_execute_output_port_names(
                        job_token
                    ):
                        new_token = job_request.token_output[port_name]
                        port_row = await self.context.database.get_port_from_token(
                            new_token.persistent_id
                        )
                        step_rows = await get_steps_from_output_port(
                            port_row["id"], self.context
                        )
                        logger.debug(
                            f"Sync Job {job_token.value.name} (wf {workflow.name}): Replace {rdwp.get_equal_token(port_name, new_token)} with {new_token.persistent_id}"
                        )
                        await rdwp.replace_token_and_remove(
                            port_name,
                            ProvenanceToken(
                                new_token,
                                TokenAvailability.Available,
                                port_row,
                                step_rows,
                            ),
                        )
                else:
                    logger.debug(
                        f"Sync Job {job_token.value.name} (wf {workflow.name}): JobRequest set to running and job_token and token_output to None."
                        f"\n\t- Prev value job_token: {job_request.job_token}\n\t- Prev value token_output: {job_request.token_output}"
                    )
                    job_request.is_running = True
                    job_request.job_token = None
                    job_request.token_output = None
                    job_request.workflow = workflow
                    await self.context.failure_manager.update_job_status(
                        job_token.value.name, job_request.lock
                    )
            # end lock
        # end for job token
        logger.debug(f"FINE sync (wf {workflow.name}) RUNNING JOBS")


async def _new_put_tokens(
    new_workflow: Workflow,
    init_ports: MutableSet[str],
    port_tokens: MutableMapping[str, MutableSet[int]],
    token_instances: MutableMapping[int, Token],
    token_available: MutableMapping[int, bool],
    port_name_ids: MutableSet[str, MutableSet[int]],
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
            if isinstance(s, BackPropagationTransformer)
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
                    f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, insert termination token (it is output port of a Back-prop)"
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
                    f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, inserts IterationTerminationToken with tag {port.token_list[-1].tag}"
                )
                port.put(IterationTerminationToken(port.token_list[-1].tag))
                logger.debug(
                    f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, inserts IterationTerminationToken with tag {port.token_list[0].tag}"
                )
                port.put(IterationTerminationToken(port.token_list[0].tag))
            if not any(t for t in port.token_list if isinstance(t, TerminationToken)):
                logger.debug(
                    f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, insert termination token"
                )
                port.put(TerminationToken())
        else:
            logger.debug(
                f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, does NOT insert manually TerminationToken. Is there Termination? ({any(isinstance(t, TerminationToken) for t in port.token_list )})"
            )
    return None


def get_max_tag_list(tag_list):
    max_tag = "0"
    for curr_tag in tag_list:
        if compare_tags(curr_tag, max_tag) == 1:
            max_tag = curr_tag
    return max_tag


async def _new_set_steps_state(new_workflow, rdwp):
    for step in new_workflow.steps.values():
        if isinstance(step, ScatterStep):
            port = step.get_output_port()
            str_t = [
                (t_id, rdwp.token_instances[t_id].tag, rdwp.token_available[t_id])
                for t_id in rdwp.port_tokens[port.name]
            ]
            logger.debug(
                f"_set_scatter_inner_state: wf {new_workflow.name} -> port_tokens[{step.get_output_port().name}]: {str_t}"
            )
            for t_id in rdwp.port_tokens[port.name]:
                logger.debug(
                    f"_set_scatter_inner_state: Token {t_id} is necessary to rollback the scatter on port {port.name} (wf {new_workflow.name}). It is {'' if rdwp.token_available[t_id] else 'NOT '}available"
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
                    f"recover_jobs-last_iteration: Step {step.name} combinator updated map[{prefix}] = {step.combinator.iteration_map[prefix]}"
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
                    new_workflow, port_name, [stop_tag]
                )
                new_workflow.ports[port_name].token_list = port.token_list
                logger.info(
                    f"wf {new_workflow.name}. Port {port.name}. token tag stop {stop_tag}"
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
