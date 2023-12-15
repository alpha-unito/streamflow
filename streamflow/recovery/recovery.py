from __future__ import annotations


import asyncio
import json
from collections import deque
from typing import MutableMapping, MutableSequence, Tuple, MutableSet, Iterable

from streamflow.core.context import StreamFlowContext
from streamflow.core.utils import (
    get_class_fullname,
    get_class_from_name,
    contains_id,
    get_tag_level,
    random_name,
)
from streamflow.core.exception import FailureHandlingException

from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.core.workflow import Job, Step, Port, Token
from streamflow.cwl.processor import CWLTokenProcessor
from streamflow.cwl.step import CWLLoopConditionalStep, CWLRecoveryLoopConditionalStep
from streamflow.cwl.transformer import (
    BackPropagationTransformer,
    CWLTokenTransformer,
)
from streamflow.log_handler import logger
from streamflow.recovery.rollback_recovery import NewProvenanceGraphNavigation
from streamflow.recovery.utils import (
    is_output_port_forward,
    is_next_of_someone,
    get_steps_from_output_port,
    _is_token_available,
    get_necessary_tokens,
    get_prev_vertices,
    INIT_DAG_FLAG,
    get_key_by_value,
    convert_to_json,
    get_recovery_loop_expression,
    get_output_ports,
    extra_data_print,
    load_and_add_ports,
    load_and_add_steps,
    load_missing_ports,
)
# from streamflow.token_printer import print_dag_ports
from streamflow.workflow.combinator import LoopTerminationCombinator
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import (
    CombinatorStep,
    InputInjectorStep,
    LoopCombinatorStep,
    ExecuteStep,
    ScatterStep,
)
from streamflow.workflow.token import (
    TerminationToken,
    JobToken,
    IterationTerminationToken,
)
from streamflow.core.workflow import Workflow
from streamflow.workflow.utils import get_job_token


class RollbackRecoveryPolicy:
    def __init__(self, context):
        self.context = context

    # async def recover_workflow_mix(
    #     self, failed_job: Job, failed_step: Step, loading_context
    # ):
    #     workflow = failed_step.workflow
    #     new_workflow = Workflow(
    #         context=workflow.context,
    #         type=workflow.type,
    #         name=random_name(),
    #         config=workflow.config,
    #     )
    #
    #     # should be an impossible case
    #     if failed_step.persistent_id is None:
    #         raise FailureHandlingException(
    #             f"Workflow {workflow.name} has the step {failed_step.name} not saved in the database."
    #         )
    #
    #     # dag = {}
    #     # for k, t in failed_job.inputs.items():
    #     #     if t.persistent_id is None:
    #     #         raise FailureHandlingException("Token has not a persistent_id")
    #     #     # se lo step è Transfer, allora non tutti gli input del job saranno nello step
    #     #     if k in failed_step.input_ports.keys():
    #     #         dag[failed_step.get_input_port(k).name] = {failed_step.name}
    #     #     else:
    #     #         logger.debug(f"Step {failed_step.name} has not the input port {k}")
    #     # dag[failed_step.get_input_port("__job__").name] = {failed_step.name}
    #     #
    #     # wr = ProvenanceGraphNavigation(
    #     #     context=workflow.context,
    #     #     output_ports=list(failed_step.input_ports.values()),
    #     #     port_name_ids={
    #     #         port.name: {port.persistent_id}
    #     #         for port in failed_step.get_input_ports().values()
    #     #     },
    #     #     dag_ports=dag,
    #     # )
    #
    #     job_token = get_job_token(
    #         failed_job.name,
    #         failed_step.get_input_port("__job__").token_list,
    #     )
    #     npgn = NewProvenanceGraphNavigation(workflow.context)
    #     await npgn.build_unfold_graph((*failed_job.inputs.values(), job_token))
    #
    #     # tokens = deque(failed_job.inputs.values())
    #     # tokens.append(job_token)
    #     # await wr.build_dag(tokens, workflow, loading_context)
    #     # wr.token_visited = get_necessary_tokens(wr.port_tokens, wr.token_visited)
    #     # logger.debug("build_dag: end build dag")
    #
    #     # update class state (attributes) and jobs synchronization
    #     inner_graph = await npgn.refold_graphs(failed_step.output_ports.values())
    #     logger.debug("Start sync-rollbacks")
    #     map_job_port = await inner_graph.sync_running_jobs(new_workflow)
    #
    #     # job_rollback = await self.context.failure_manager.sync_rollbacks(
    #     #     new_workflow, loading_context, failed_step, wr, enable_sync=False
    #     # )
    #     # wr.token_visited = get_necessary_tokens(wr.port_tokens, wr.token_visited)
    #
    #     # todo tmp soluzione perché con i loop non funziona
    #     for pr in map_job_port.values():
    #         pr.port.workflow = new_workflow
    #         new_workflow.add_port(pr.port)
    #
    #     def div(dict_a, dict_b, title):
    #         if dict_a.keys() != dict_b.keys():
    #             c = set(dict_a.keys()) - set(dict_b.keys())
    #             d = set(dict_b.keys()) - set(dict_a.keys())
    #             pass
    #         for key, values in dict_a.items():
    #             if values != dict_a[key]:
    #                 pass
    #
    #     # div(
    #     #     {k: i for k, (_, i) in wr.token_visited.items()},
    #     #     inner_graph.token_available,
    #     #     "token_visited",
    #     # )
    #     # div(wr.dag_ports, inner_graph.dcg_port, "graph_port")
    #     # div(wr.port_tokens, inner_graph.port_tokens, "port_tokens")
    #     # div(wr.port_name_ids, inner_graph.port_name_ids, "port_name_ids")
    #
    #     await print_dag_ports(
    #         {
    #             npgn.info_tokens[k].port_row["id"]: {
    #                 npgn.info_tokens[v].port_row["id"]
    #                 for v in values
    #                 if isinstance(v, int)
    #             }
    #             for k, values in npgn.dag_tokens.items()
    #             if isinstance(k, int)
    #         },
    #         self.context,
    #         "p-new",
    #     )
    #     await print_dag_ports(
    #         {
    #             min(inner_graph.port_name_ids[k]): {
    #                 min(inner_graph.port_name_ids[v])
    #                 for v in values
    #                 if v in inner_graph.port_name_ids.keys()
    #             }
    #             for k, values in inner_graph.dcg_port.items()
    #             if k in inner_graph.port_name_ids.keys()
    #         },
    #         self.context,
    #         "p-new-2",
    #     )
    #     # await print_dag_ports(
    #     #     {
    #     #         min(wr.port_name_ids[k]): {
    #     #             min(wr.port_name_ids[v])
    #     #             for v in values
    #     #             if v in wr.port_name_ids.keys()
    #     #         }
    #     #         for k, values in wr.dag_ports.items()
    #     #         if k in wr.port_name_ids.keys()
    #     #     },
    #     #     self.context,
    #     #     "p-old",
    #     # )
    #
    #     wr = ProvenanceGraphNavigation(
    #         {k: {v for v in vs} for k, vs in inner_graph.dcg_port.items()},
    #         [],
    #         {k: {v for v in vs} for k, vs in inner_graph.port_name_ids.items()},
    #         self.context,
    #     )
    #     wr.token_visited = {
    #         k: (inner_graph.token_instances[k], i)
    #         for k, i in inner_graph.token_available.items()
    #     }
    #     wr.port_tokens = {
    #         k: {v for v in vs} for k, vs in inner_graph.port_tokens.items()
    #     }
    #
    #     logger.debug("End sync-rollbacks")
    #
    #     # todo wr.inputs_ports non viene aggiornato
    #     # if (set(wr.input_ports) - set(wr.dag_ports[INIT_DAG_FLAG])) or (set(wr.dag_ports[INIT_DAG_FLAG]) - set(wr.input_ports)):
    #     #     pass
    #
    #     ports, steps = await wr.get_port_and_step_ids()
    #
    #     await _populate_workflow(
    #         wr,
    #         ports,
    #         steps,
    #         failed_step,
    #         new_workflow,
    #         loading_context,
    #     )
    #     if "/subworkflow/i1-back-propagation-transformer" in new_workflow.steps.keys():
    #         raise FailureHandlingException("Caricata i1-back-prop CHE NON SERVE")
    #     logger.debug("end populate")
    #
    #     # for port in failed_step.get_input_ports().values():
    #     #     if port.name not in new_workflow.ports.keys():
    #     #         raise FailureHandlingException(
    #     #             f"La input port {port.name} dello step fallito {failed_step.name} non è presente nel new_workflow {new_workflow.name}"
    #     #         )
    #
    #     # _set_scatter_inner_state(
    #     #     new_workflow, wr.dag_ports, wr.port_tokens, wr.token_visited
    #     # )
    #     logger.debug("end save_for_retag")
    #
    #     last_iteration = await _put_tokens(
    #         new_workflow,
    #         wr.dag_ports[INIT_DAG_FLAG],
    #         wr.port_tokens,
    #         wr.token_visited,
    #         wr,
    #     )
    #     logger.debug("end _put_tokens")
    #     # await set_combinator_status(new_workflow, workflow, wr, loading_context)
    #     await _set_steps_state(new_workflow, wr)
    #     extra_data_print(
    #         workflow,
    #         new_workflow,
    #         None,  # job_rollback,
    #         wr,
    #         last_iteration,
    #     )
    #     return new_workflow, last_iteration

    async def recover_workflow(
        self, failed_job: Job, failed_step: Step, loading_context
    ):
        new_workflow, lw = await self.recover_workflow_new(
            failed_job, failed_step, loading_context
        )
        # new_workflow_old_style, _ = await self.recover_workflow_old(
        #     failed_job, failed_step, loading_context
        # )
        # a = set(new_workflow.steps.keys()) - set(new_workflow_old_style.steps.keys())
        # b = set(new_workflow_old_style.steps.keys()) - set(new_workflow.steps.keys())
        #
        # c = set(new_workflow.ports.keys()) - set(new_workflow_old_style.ports.keys())
        # d = set(new_workflow_old_style.ports.keys()) - set(new_workflow.ports.keys())

        pass
        return new_workflow, lw

    async def recover_workflow_new(
        self, failed_job: Job, failed_step: Step, loading_context
    ):
        workflow = failed_step.workflow
        new_workflow = Workflow(
            context=workflow.context,
            type=workflow.type,
            name=random_name(),
            config=workflow.config,
        )

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
        map_job_port = await inner_graph.sync_running_jobs(new_workflow)

        # todo tmp soluzione perché con i loop non funziona
        for pr in map_job_port.values():
            pr.port.workflow = new_workflow
            new_workflow.add_port(pr.port)

        logger.debug("End sync-rollbacks")
        ports, steps = await inner_graph.get_port_and_step_ids()
        await inner_graph._populate_workflow(
            ports,
            steps,
            failed_step,
            new_workflow,
            loading_context,
        )
        # await _populate_workflow(
        #     wr,
        #     ports,
        #     steps,
        #     failed_step,
        #     new_workflow,
        #     loading_context,
        # )
        if "/subworkflow/i1-back-propagation-transformer" in new_workflow.steps.keys():
            raise FailureHandlingException("Caricata i1-back-prop CHE NON SERVE")
        logger.debug("end populate")

        # for port in failed_step.get_input_ports().values():
        #     if port.name not in new_workflow.ports.keys():
        #         raise FailureHandlingException(
        #             f"La input port {port.name} dello step fallito {failed_step.name} non è presente nel new_workflow {new_workflow.name}"
        #         )
        logger.debug("end save_for_retag")

        wr = ProvenanceGraphNavigation(
            {k: {v for v in vs} for k, vs in inner_graph.dcg_port.items()},
            [],
            {k: {v for v in vs} for k, vs in inner_graph.port_name_ids.items()},
            self.context,
        )
        wr.token_visited = {
            k: (inner_graph.token_instances[k], i)
            for k, i in inner_graph.token_available.items()
        }
        wr.port_tokens = {
            k: {v for v in vs} for k, vs in inner_graph.port_tokens.items()
        }

        last_iteration = await _put_tokens(
            new_workflow,
            wr.dag_ports[INIT_DAG_FLAG],
            wr.port_tokens,
            wr.token_visited,
            wr,
        )
        logger.debug("end _put_tokens")
        await _set_steps_state(new_workflow, wr)
        extra_data_print(
            workflow,
            new_workflow,
            None,  # job_rollback,
            wr,
            last_iteration,
        )
        return new_workflow, last_iteration

    # async def recover_workflow_old(
    #     self, failed_job: Job, failed_step: Step, loading_context
    # ):
    #     workflow = failed_step.workflow
    #     new_workflow = Workflow(
    #         context=workflow.context,
    #         type=workflow.type,
    #         name=random_name(),
    #         config=workflow.config,
    #     )
    #
    #     # should be an impossible case
    #     if failed_step.persistent_id is None:
    #         raise FailureHandlingException(
    #             f"Workflow {workflow.name} has the step {failed_step.name} not saved in the database."
    #         )
    #
    #     dag = {}
    #     for k, t in failed_job.inputs.items():
    #         if t.persistent_id is None:
    #             raise FailureHandlingException("Token has not a persistent_id")
    #         # se lo step è Transfer, allora non tutti gli input del job saranno nello step
    #         if k in failed_step.input_ports.keys():
    #             dag[failed_step.get_input_port(k).name] = {failed_step.name}
    #         else:
    #             logger.debug(f"Step {failed_step.name} has not the input port {k}")
    #     dag[failed_step.get_input_port("__job__").name] = {failed_step.name}
    #
    #     wr = ProvenanceGraphNavigation(
    #         context=workflow.context,
    #         output_ports=list(failed_step.input_ports.values()),
    #         port_name_ids={
    #             port.name: {port.persistent_id}
    #             for port in failed_step.get_input_ports().values()
    #         },
    #         dag_ports=dag,
    #     )
    #
    #     job_token = get_job_token(
    #         failed_job.name,
    #         failed_step.get_input_port("__job__").token_list,
    #     )
    #
    #     tokens = deque(failed_job.inputs.values())
    #     tokens.append(job_token)
    #     await wr.build_dag(tokens, workflow, loading_context)
    #     wr.token_visited = get_necessary_tokens(wr.port_tokens, wr.token_visited)
    #     logger.debug("build_dag: end build dag")
    #
    #     # update class state (attributes) and jobs synchronization
    #     logger.debug("Start sync-rollbacks")
    #     # job_rollback = await self.context.failure_manager.sync_rollbacks(
    #     #     new_workflow, loading_context, failed_step, wr, enable_sync=False
    #     # )
    #     wr.token_visited = get_necessary_tokens(wr.port_tokens, wr.token_visited)
    #
    #     logger.debug("End sync-rollbacks")
    #     ports, steps = await wr.get_port_and_step_ids()
    #
    #     await _populate_workflow(
    #         wr,
    #         ports,
    #         steps,
    #         failed_step,
    #         new_workflow,
    #         loading_context,
    #     )
    #     if "/subworkflow/i1-back-propagation-transformer" in new_workflow.steps.keys():
    #         raise FailureHandlingException("Caricata i1-back-prop CHE NON SERVE")
    #     logger.debug("end populate")
    #
    #     # for port in failed_step.get_input_ports().values():
    #     #     if port.name not in new_workflow.ports.keys():
    #     #         raise FailureHandlingException(
    #     #             f"La input port {port.name} dello step fallito {failed_step.name} non è presente nel new_workflow {new_workflow.name}"
    #     #         )
    #
    #     logger.debug("end save_for_retag")
    #
    #     last_iteration = await _put_tokens(
    #         new_workflow,
    #         wr.dag_ports[INIT_DAG_FLAG],
    #         wr.port_tokens,
    #         wr.token_visited,
    #         wr,
    #     )
    #     logger.debug("end _put_tokens")
    #     await _set_steps_state(new_workflow, wr)
    #     extra_data_print(
    #         workflow,
    #         new_workflow,
    #         None,  # job_rollback,
    #         wr,
    #         last_iteration,
    #     )
    #     return new_workflow, last_iteration


class ProvenanceGraphNavigation:
    def __init__(
        self,
        dag_ports: MutableMapping[str, MutableSet[str]],
        output_ports: MutableSequence[str],
        port_name_ids: MutableMapping[str, MutableSet[int]],
        context: StreamFlowContext,
    ):
        self.context = context
        self.input_ports = set()

        # { port_names }
        self.output_ports = output_ports

        # { name : [ ids ] }
        self.port_name_ids = port_name_ids

        # { port_name : [ next_port_names ] }
        self.dag_ports = dag_ports

        # { port_name : [ token_ids ] }
        self.port_tokens = {}

        # { id : (Token, is_available) }
        self.token_visited = {}

        # LoopCombinatorStep name if the failed step is inside a loop
        self.external_loop_step_name = None
        self.external_loop_step = None

    def add_into_vertex(
        self,
        port_name_to_add: str,
        token_to_add: Token,
        output_port_forward: bool,
    ):
        if isinstance(token_to_add, JobToken):
            logger.debug(
                f"add_into_vertex: Token to add is JobToken {token_to_add.value.name} (id {token_to_add.persistent_id}) on port {port_name_to_add}"
            )
        if port_name_to_add in self.port_tokens.keys():
            for sibling_token_id in self.port_tokens[port_name_to_add]:
                sibling_token = self.token_visited[sibling_token_id][0]
                # If there are two of the same token in the same port, keep the newer token or that available
                # In the case of JobTokens, they are the same when they have the same job.name
                # In other token types, they are equal when they have the same tag
                if isinstance(token_to_add, JobToken):
                    if not isinstance(sibling_token, JobToken):
                        raise FailureHandlingException(
                            f"Non è un jobtoken {sibling_token}"
                        )
                    if token_to_add.value.name == sibling_token.value.name:
                        if (
                            self.token_visited[token_to_add.persistent_id][1]
                            or token_to_add.persistent_id > sibling_token_id
                        ):
                            self.port_tokens[port_name_to_add].remove(sibling_token_id)
                            break
                        else:
                            return
                else:
                    if token_to_add.tag == sibling_token.tag:
                        if (
                            self.token_visited[token_to_add.persistent_id][1]
                            or token_to_add.persistent_id > sibling_token_id
                        ):
                            self.port_tokens[port_name_to_add].remove(sibling_token_id)
                            break
                        else:
                            return
        logger.debug(
            f"add_into_vertex: Aggiungo token {token_to_add} id {token_to_add.persistent_id} tag {token_to_add.tag} nella port_tokens[{port_name_to_add}]"
        )
        self.port_tokens.setdefault(port_name_to_add, set()).add(
            token_to_add.persistent_id
        )

    async def add_into_graph(
        self,
        port_name_key: str,
        port_name_to_add: str,
        token_to_add: Token = None,
    ):
        output_port_forward = await is_output_port_forward(
            min(self.port_name_ids[port_name_to_add]), self.context
        )
        if token_to_add:
            self.add_into_vertex(
                port_name_to_add,
                token_to_add,
                output_port_forward,
            )

        # todo: fare i controlli prima di aggiungere.
        #   in particolare se la port è presente in init e si sta aggiungendo in un'altra port
        #       - o non si aggiunge
        #       - o si toglie da init e si aggiunge nella nuova port
        #   stessa cosa vice-versa, se è in un'altra port e si sta aggiungendo in init
        # edit. attuale implementazione: si toglie da init e si aggiunge alla port nuova
        if (
            INIT_DAG_FLAG in self.dag_ports.keys()
            and port_name_to_add in self.dag_ports[INIT_DAG_FLAG]
            and port_name_key != INIT_DAG_FLAG
        ):
            logger.debug(
                f"add_into_graph: Inserisco la port {port_name_to_add} dopo la port {port_name_key}. però è già in INIT"
            )
        if port_name_key == INIT_DAG_FLAG and port_name_to_add in [
            pp
            for p, plist in self.dag_ports.items()
            for pp in plist
            if p != INIT_DAG_FLAG
        ]:
            logger.debug(
                f"add_into_graph: Inserisco la port {port_name_to_add} in INIT. però è già in {port_name_key}"
            )
        self.dag_ports.setdefault(port_name_key, set()).add(port_name_to_add)

        # It is possible find some token available and other unavailable in a scatter
        # In this case the port is added in init and in another port.
        # If the port is next in another port, it is necessary to detach the port from the init next list
        if (
            INIT_DAG_FLAG in self.dag_ports.keys()
            and port_name_to_add in self.dag_ports[INIT_DAG_FLAG]
            and is_next_of_someone(port_name_to_add, self.dag_ports)
        ):
            if not output_port_forward:
                logger.debug(
                    f"add_into_graph: port {port_name_to_add} is removed from init "
                )
                self.dag_ports[INIT_DAG_FLAG].remove(port_name_to_add)
            else:
                logger.debug(f"add_into_graph: port {port_name_to_add} is kept in init")

    async def build_dag(
        self,
        token_frontier: deque,
        workflow: Workflow,
        loading_context: DefaultDatabaseLoadingContext,
        graph_cut: MutableSequence[str] = None,
    ):
        if not graph_cut:
            graph_cut = []
        # todo: cambiarlo in {token_id: is_available} e quando è necessaria l'istanza del token recuperarla dal loading_context.load_token(id)
        # { token_id: (token, is_available)}
        all_token_visited = self.token_visited

        # {old_job_token_id : job_request_running}
        running_new_job_tokens = {}

        # {old_job_token_id : (new_job_token_id, new_output_token_id)}
        available_new_job_tokens = {}

        # counter of LoopCombinatorStep and LoopTerminator visited
        # if it is odd, it means that the failed step is inside a loop
        counter_loop = 0

        # Visit the (token provenance) graph with a Breadth First Search approach using token_frontier as a Queue
        while token_frontier:
            token = token_frontier.popleft()
            if len(
                graph_cut
            ) > 0 or not await self.context.failure_manager.has_token_already_been_recovered(
                token,
                all_token_visited,
                available_new_job_tokens,
                running_new_job_tokens,
                workflow.context,
            ):
                # impossible case because when added in tokens, the elem is checked
                if token.persistent_id in all_token_visited.keys():
                    raise FailureHandlingException(
                        f"Token {token.persistent_id} already visited"
                    )

                port_row = await workflow.context.database.get_port_from_token(
                    token.persistent_id
                )
                self.port_name_ids.setdefault(port_row["name"], set()).add(
                    port_row["id"]
                )
                step_rows = await get_steps_from_output_port(
                    port_row["id"], workflow.context
                )
                is_available = await _is_token_available(token, workflow.context)

                invalidate = True
                for step_row in step_rows:
                    logger.debug(
                        f"build_dag: Trovato {step_row['name']} di tipo {get_class_from_name(step_row['type'])} dalla port {port_row['name']}"
                    )
                    if issubclass(
                        get_class_from_name(step_row["type"]),
                        (ExecuteStep, InputInjectorStep, BackPropagationTransformer),
                    ):
                        invalidate = False
                    elif issubclass(
                        get_class_from_name(step_row["type"]), CombinatorStep
                    ) and issubclass(
                        get_class_from_name(
                            json.loads(step_row["params"])["combinator"]["type"]
                        ),
                        LoopTerminationCombinator,
                    ):
                        counter_loop += 1
                        if (
                            not self.external_loop_step_name
                            and counter_loop % 2 == 1
                            and issubclass(
                                get_class_from_name(
                                    json.loads(step_row["params"])["combinator"]["type"]
                                ),
                                LoopTerminationCombinator,
                            )
                        ):
                            raise FailureHandlingException(
                                "Visited a LoopTerminationCombinator but not a CWLLoopConditionalStep before"
                            )
                    elif issubclass(
                        get_class_from_name(step_row["type"]), CWLLoopConditionalStep
                    ):
                        counter_loop += 1
                        if not self.external_loop_step_name and counter_loop % 2 == 1:
                            self.external_loop_step_name = step_row["name"]
                if invalidate:
                    is_available = False
                if port_row["name"] in graph_cut:
                    logger.debug(
                        f"build_dag: port {port_row['name']} in graph_cut and token {token.persistent_id} turned available True (original val {is_available}). Graph_cut len {len(graph_cut)} -> {graph_cut}"
                    )
                    is_available = True
                all_token_visited[token.persistent_id] = (token, is_available)
                if not is_available:
                    if prev_tokens := await loading_context.load_prev_tokens(
                        workflow.context, token.persistent_id
                    ):
                        prev_port_rows = await asyncio.gather(
                            *(
                                asyncio.create_task(
                                    workflow.context.database.get_port_from_token(
                                        pt.persistent_id
                                    )
                                )
                                for pt in prev_tokens
                            )
                        )
                        logger.debug(
                            f"build_dag: Dal token id {token.persistent_id} ho recuperato prev-tokens {[pt.persistent_id for pt in prev_tokens]}"
                        )
                        for pt, prev_port_row in zip(prev_tokens, prev_port_rows):
                            self.port_name_ids.setdefault(
                                prev_port_row["name"], set()
                            ).add(prev_port_row["id"])
                            await self.add_into_graph(
                                prev_port_row["name"],
                                port_row["name"],
                                token,
                            )
                            if (
                                pt.persistent_id not in all_token_visited.keys()
                                and not contains_id(pt.persistent_id, token_frontier)
                            ):
                                token_frontier.append(pt)
                    else:
                        logger.debug(
                            f"build_dag: token id {token.persistent_id} non ha token precedenti"
                        )
                        await self.add_into_graph(
                            INIT_DAG_FLAG,
                            port_row["name"],
                            token,
                        )
                else:
                    logger.debug(
                        f"build_dag: Il token {token.persistent_id} è disponibile, quindi aggiungo la sua port {port_row['name']} tra gli init"
                    )
                    await self.add_into_graph(
                        INIT_DAG_FLAG,
                        port_row["name"],
                        token,
                    )
            else:
                logger.debug(
                    f"build_dag: Il token {token.persistent_id} è disponibile su has_token_already_been_recovered (new tokens {available_new_job_tokens[token.persistent_id]})"
                )
                port_row = await workflow.context.database.get_port_from_token(
                    token.persistent_id
                )
                await self.add_into_graph(
                    INIT_DAG_FLAG,
                    port_row["name"],
                    token,
                )
        logger.debug(
            f"build_dag: len_token_visited_original: {len(self.token_visited)} - len_after_checking: {len(get_necessary_tokens(self.port_tokens, all_token_visited))}"
        )
        logger.debug(
            f"build_dag: JobTokens: {set([t.value.name for t, _ in get_necessary_tokens(self.port_tokens, all_token_visited).values() if isinstance(t, JobToken)])}",
        )

        all_token_visited = dict(sorted(all_token_visited.items()))
        for t, a in all_token_visited.values():
            logger.debug(
                f"build_dag: Token id: {t.persistent_id} tag: {t.tag} val: {t} is available? {a}"
            )
        logger.debug(f"PORT_TOKENS: {convert_to_json(self.port_tokens)}")
        logger.debug(f"DAG_PORTS: {convert_to_json(self.dag_ports)}")

        logger.debug(
            f"build_dag: available_new_job_tokens n.elems: {len(available_new_job_tokens)} {json.dumps(available_new_job_tokens, indent=2)}"
        )
        # json_running = json.dumps(
        #     {
        #         k: {
        #             "job_token": v.job_token.value.name if v.job_token else None,
        #             "is_running": v.is_running,
        #             "queue": [(x.port.name, x.waiting_token) for x in v.queue],
        #         }
        #         for k, v in running_new_job_tokens.items()
        #     },
        #     indent=2,
        # )
        logger.debug(
            f"build_dag: running_new_job_tokens n.elems: {len(available_new_job_tokens)}"  # f"{json_running}"
        )

        logger.debug("build_dag: pre riduzione")
        await self.reduce_graph(
            available_new_job_tokens,
            loading_context,
        )
        logger.debug(
            f"build_dag: grafo ridotto - JobTokens: {set([t.value.name for t, _ in get_necessary_tokens(self.port_tokens, all_token_visited).values() if isinstance(t, JobToken)])}"
        )

    async def reduce_graph(
        self,
        available_new_job_tokens,
        loading_context,
    ):
        for v in available_new_job_tokens.values():
            if v["out-token-port-name"] not in self.port_tokens.keys():
                raise FailureHandlingException("Non c'è la porta. NON VA BENE")

        # todo: aggiustare nel caso in cui uno step abbia più port di output.
        #   Nella replace_token, se i token della port sono tutti disponibili
        #   allora rimuove tutte le port precedenti.
        #   ma se i token della seconda port di output dello step non sono disponibili
        #   è necessario eseguire tutte le port precedenti.
        for v in available_new_job_tokens.values():
            if v["out-token-port-name"] in self.port_tokens.keys():
                await self.replace_token(
                    v["out-token-port-name"],
                    v["old-out-token"],
                    self.token_visited[v["new-out-token"]][0],
                )
                await self.remove_prev(
                    v["old-out-token"],
                    loading_context,
                )
                logger.debug(
                    f"reduce_graph: Port_tokens {v['out-token-port-name']} - token {v['old-out-token']} sostituito con token {v['new-out-token']}. Quindi la port ha {self.port_tokens[v['out-token-port-name']]} tokens"
                )
            else:
                logger.debug(
                    f"reduce_graph: Port_tokens {v['out-token-port-name']} - port non più presente. Non serve più eseguire lo step annesso"
                )

    async def replace_token(
        self,
        port_name,
        token_id_to_replace: int,
        token_to_add: Token,
    ):
        if token_id_to_replace in self.port_tokens[port_name]:
            self.port_tokens[port_name].remove(token_id_to_replace)
        # token_visited.pop(token_id_to_replace, None)
        self.port_tokens[port_name].add(token_to_add.persistent_id)
        self.token_visited[token_to_add.persistent_id] = (token_to_add, True)
        logger.debug(
            f"replace_token: replace token {token_id_to_replace} con {token_to_add.persistent_id} - I token della port {port_name} sono tutti disp? {all((self.token_visited[t_id][1] for t_id in self.port_tokens[port_name]))}"
        )
        if all((self.token_visited[t_id][1] for t_id in self.port_tokens[port_name])):
            for prev_port_name in get_prev_vertices(port_name, self.dag_ports):
                await self.remove_from_graph(prev_port_name)
            await self.add_into_graph(INIT_DAG_FLAG, port_name, None)

    async def remove_from_graph(self, port_name_to_remove: str):
        other_ports_to_remove = set()
        # Removed all occurrences as next port
        for port_name, next_port_names in self.dag_ports.items():
            if port_name_to_remove in next_port_names:
                next_port_names.remove(port_name_to_remove)
            if len(next_port_names) == 0:
                other_ports_to_remove.add(port_name)

        # removed all tokens generated by port
        for t_id in self.port_tokens.pop(port_name_to_remove, ()):
            # token_visited.pop(t_id)
            logger.debug(
                f"remove_from_graph: pop token id {t_id} from port_tokens[{port_name_to_remove}]"
            )

        # removed in the graph and moved its next port in INIT
        for port_name in self.dag_ports.pop(port_name_to_remove, ()):
            if not is_next_of_someone(port_name, self.dag_ports):
                await self.add_into_graph(INIT_DAG_FLAG, port_name, None)
        logger.debug(
            f"remove_from_graph: pop {port_name_to_remove} from dag_ports and port_tokens"
        )
        # remove vertex detached from the graph
        for port_name in other_ports_to_remove:
            await self.remove_from_graph(port_name)

    async def remove_token_by_id(self, token_id_to_remove: int):
        # token_visited.pop(token_id_to_remove, None)
        logger.debug(
            f"remove_token_by_id: remove token {token_id_to_remove} from visited"
        )
        ports_to_remove = set()
        for port_name, token_ids in self.port_tokens.items():
            if token_id_to_remove in token_ids:
                token_ids.remove(token_id_to_remove)
                logger.debug(
                    f"remove_token_by_id: remove token {token_id_to_remove} from {port_name}"
                )
            if len(token_ids) == 0:
                ports_to_remove.add(port_name)
        for port_name in ports_to_remove:
            await self.remove_from_graph(port_name)

    async def remove_token(self, token, port_name):
        msg = (
            "job {token.value.name}"
            if isinstance(token, JobToken)
            else f"tag {token.tag}"
        )
        if port_name in self.port_tokens.keys():
            for t_id in self.port_tokens[port_name]:
                if (
                    not isinstance(token, JobToken)
                    and self.token_visited[t_id][0].tag == token.tag
                ) or (
                    isinstance(token, JobToken)
                    and self.token_visited[t_id][0].value.name == token.value.name
                ):
                    logger.debug(
                        f"remove_token: Rimuovo token {t_id} con {msg} dal port_tokens[{port_name}]"
                    )
                    await self.remove_token_by_id(t_id)
                    return
            logger.debug(
                f"remove_token: Volevo rimuovere token con {msg} dal port_tokens[{port_name}] ma non ce n'è"
            )
        else:
            logger.debug(
                f"remove_token: Volevo rimuovere token con {msg} ma non c'è port {port_name} in port_tokens"
            )

    # richiamare ricorsivamente finche i token.tag hanno sempre lo stesso livello o se si trova un JobToken
    async def remove_prev(
        self,
        token_id_to_remove: int,
        loading_context,
    ):
        curr_tag_level = get_tag_level(self.token_visited[token_id_to_remove][0].tag)
        for token_row in await self.context.database.get_dependees(token_id_to_remove):
            port_row = await self.context.database.get_port_from_token(
                token_row["dependee"]
            )
            if token_row["dependee"] not in self.token_visited.keys():
                logger.debug(
                    f"remove_prev: token id {token_row['dependee']} non presente nei token_visited"
                )
                continue
            elif port_row["type"] != get_class_fullname(ConnectorPort):
                if token_row["dependee"] not in self.token_visited.keys():
                    self.token_visited[token_row["dependee"]] = (
                        await loading_context.load_token(
                            self.context, token_row["dependee"]
                        ),
                        False,
                    )
                    logger.debug(
                        f"remove_prev: prev token id {token_row['dependee']} tag {self.token_visited[token_row['dependee']][0].tag} non è presente tra i token visitati, lo aggiungo"
                    )
                logger.debug(
                    f"remove_prev: token {token_id_to_remove} rm prev token id: {token_row['dependee']} val: {self.token_visited[token_row['dependee']][0]} tag: {self.token_visited[token_row['dependee']][0].tag}"
                )

                port_dependee_row = await self.context.database.get_port_from_token(
                    token_row["dependee"]
                )
                dependee_token = self.token_visited[token_row["dependee"]][0]
                a = port_dependee_row["type"] == get_class_fullname(JobPort)
                b = isinstance(dependee_token, JobToken)
                if a != b:
                    raise FailureHandlingException("Diverse port")
                if isinstance(dependee_token, JobToken) or (
                    not isinstance(self.token_visited[token_id_to_remove][0], JobToken)
                    and curr_tag_level == get_tag_level(dependee_token.tag)
                ):
                    await self.remove_token(dependee_token, port_dependee_row["name"])
                    msg = (
                        f"job {dependee_token.value.name} "
                        if isinstance(dependee_token, JobToken)
                        else ""
                    )
                    logger.debug(
                        f"remove_prev: token {token_id_to_remove} tag {self.token_visited[token_id_to_remove][0].tag} (lvl {curr_tag_level}) ha prev {msg}id {token_row['dependee']} tag {dependee_token.tag} (lvl {get_tag_level(dependee_token.tag)}) -> {curr_tag_level == get_tag_level(dependee_token.tag)}"
                    )
                    await self.remove_prev(token_row["dependee"], loading_context)
                else:
                    logger.debug(
                        f"remove_prev: pulizia fermata al token {token_id_to_remove} con tag {self.token_visited[token_id_to_remove][0].tag}"
                    )
            else:
                logger.debug(
                    f"remove_prev: token {token_id_to_remove} volevo rm prev token id {token_row['dependee']} tag {self.token_visited[token_row['dependee']][0].tag} ma è un token del ConnectorPort"
                )

    async def explore_top_down(self):
        ports_frontier = {port_name for port_name in self.dag_ports[INIT_DAG_FLAG]}
        ports_visited = set()

        while ports_frontier:
            port_name = ports_frontier.pop()
            ports_visited.add(port_name)
            port_id = min(self.port_name_ids[port_name])
            dep_steps_port_rows = await self.context.database.get_steps_from_input_port(
                port_id
            )
            step_rows = await asyncio.gather(
                *(
                    asyncio.create_task(
                        self.context.database.get_step(dependency_row["step"])
                    )
                    for dependency_row in dep_steps_port_rows
                )
            )
            for step_row in step_rows:
                for port_row in await get_output_ports(step_row["id"], self.context):
                    self.port_name_ids.setdefault(port_row["name"], set()).add(
                        port_row["id"]
                    )
                    if port_row["name"] not in ports_visited:
                        ports_frontier.add(port_row["name"])
                    await self.add_into_graph(port_name, port_row["name"])
        pass

    async def get_port_and_step_ids(self):
        steps = set()
        ports = {
            min(self.port_name_ids[port_name]) for port_name in self.port_tokens.keys()
        }
        for row_dependencies in await asyncio.gather(
            *(
                asyncio.create_task(
                    self.context.database.get_steps_from_output_port(port_id)
                )
                for port_id in ports
            )
        ):
            for row_dependency in row_dependencies:
                logger.debug(
                    f"get_port_and_step_ids: Step {(await self.context.database.get_step(row_dependency['step']))['name']} (id {row_dependency['step']}) recuperato dalla port {get_key_by_value(row_dependency['port'], self.port_name_ids)} (id {row_dependency['port']})"
                )
                steps.add(row_dependency["step"])
        rows_dependencies = await asyncio.gather(
            *(
                asyncio.create_task(self.context.database.get_input_ports(step_id))
                for step_id in steps
            )
        )
        step_to_remove = set()
        for step_id, row_dependencies in zip(steps, rows_dependencies):
            for row_port in await asyncio.gather(
                *(
                    asyncio.create_task(
                        self.context.database.get_port(row_dependency["port"])
                    )
                    for row_dependency in row_dependencies
                )
            ):
                if row_port["name"] not in self.port_tokens.keys():
                    logger.debug(
                        f"get_port_and_step_ids: Step {(await self.context.database.get_step(row_dependency['step']))['name']} (id {step_id}) rimosso perché port {row_port['name']} non presente nei port_tokens. è presente nelle ports? {row_port['id'] in ports}"
                    )
                    step_to_remove.add(step_id)
        for s_id in step_to_remove:
            steps.remove(s_id)
        return ports, steps


async def _put_tokens(
    new_workflow: Workflow,
    init_ports: MutableSet[str],
    port_tokens: MutableMapping[str, MutableSet[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
    wr,
):
    last_iteration = {}
    for port_name in init_ports:
        token_list = [
            token_visited[t_id][0]
            for t_id in port_tokens[port_name]
            if isinstance(t_id, int) and token_visited[t_id][1]
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
        if loop_combinator_input and len(port_tokens[port_name]) > 1:
            last_iteration.setdefault(port.name, set())
            for p_id in wr.port_name_ids[port.name]:
                last_iteration[port.name].add(p_id)

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
                if last_iteration:
                    if isinstance(port.token_list[-1], TerminationToken):
                        token = port.token_list[-1]
                    else:
                        token = port.token_list[-1]
                        logger.debug(
                            f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, inserts EMPTY token with tag {token.tag}"
                        )
                        # port.put(token)
                    logger.debug(
                        f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, inserts IterationTerminationToken with tag {token.tag}"
                    )
                    port.put(IterationTerminationToken(token.tag))
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
                f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, does NOT insert TerminationToken"
            )
    return last_iteration


async def _set_steps_state(new_workflow, wr):
    for step in new_workflow.steps.values():
        if isinstance(step, ScatterStep):
            port = step.get_output_port()
            str_t = [
                (t_id, wr.token_visited[t_id][0].tag, wr.token_visited[t_id][1])
                for t_id in wr.port_tokens[port.name]
            ]
            logger.debug(
                f"_set_scatter_inner_state: wf {new_workflow.name} -> port_tokens[{step.get_output_port().name}]: {str_t}"
            )
            for t_id in wr.port_tokens[port.name]:
                logger.debug(
                    f"_set_scatter_inner_state: Token {t_id} is necessary to rollback the scatter on port {port.name} (wf {new_workflow.name}). It is {'' if wr.token_visited[t_id][1] else 'NOT '}available"
                )
                # a possible control can be if not token_visited[t_id][1]: then add in valid tags
                if step.valid_tags is None:
                    step.valid_tags = {wr.token_visited[t_id][0].tag}
                else:
                    step.valid_tags.add(wr.token_visited[t_id][0].tag)
        elif isinstance(step, LoopCombinatorStep):
            port = list(step.get_input_ports().values()).pop()
            token = port.token_list[0]

            prefix = ".".join(token.tag.split(".")[:-1])
            if prefix != "":
                step.combinator.iteration_map[prefix] = int(token.tag.split(".")[-1])
                logger.debug(
                    f"recover_jobs-last_iteration: Step {step.name} combinator updated map[{prefix}] = {step.combinator.iteration_map[prefix]}"
                )


def _set_scatter_inner_state(new_workflow, dag_ports, port_tokens, token_visited):
    for step in new_workflow.steps.values():
        if isinstance(step, ScatterStep):
            port = step.get_output_port()
            str_t = [
                (t_id, token_visited[t_id][0].tag, token_visited[t_id][1])
                for t_id in port_tokens[port.name]
            ]
            logger.debug(
                f"_set_scatter_inner_state: wf {new_workflow.name} -> port_tokens[{step.get_output_port().name}]: {str_t}"
            )
            for t_id in port_tokens[port.name]:
                logger.debug(
                    f"_set_scatter_inner_state: Token {t_id} is necessary to rollback the scatter on port {port.name} (wf {new_workflow.name}). It is {'' if token_visited[t_id][1] else 'NOT '}available"
                )
                # a possible control can be if not token_visited[t_id][1]: then add in valid tags
                if step.valid_tags is None:
                    step.valid_tags = {token_visited[t_id][0].tag}
                else:
                    step.valid_tags.add(token_visited[t_id][0].tag)


async def set_combinator_status(new_workflow, workflow, wr, loading_context):
    for step in new_workflow.steps.values():
        if isinstance(step, LoopCombinatorStep):
            port = list(step.get_input_ports().values()).pop()
            token = port.token_list[0]

            # tags = {
            #     wr.token_visited[t_id][0].tag
            #     for port in step.get_input_ports().values()
            #     for t_id in wr.port_tokens[port.name]
            # }
            # len_tags = {len(tag.split(".")) for tag in tags}
            # db_tokens = await asyncio.gather(
            #     *(
            #         asyncio.create_task(
            #             Token.load(new_workflow.context, t_id, loading_context)
            #         )
            #         for t_id in await new_workflow.context.database.get_port_tokens(
            #             min(wr.port_name_ids[port.name])
            #         )
            #     )
            # )
            # db_tags = list({t.tag for t in db_tokens})
            # db_len_tags = {len(tag.split(".")) for tag in db_tags}

            # for k in workflow.steps[step.name].combinator.iteration_map.keys():
            #     step.combinator.iteration_map[k] = int(token.tag.split(".")[-1])
            #     pass

            # logger.debug(
            #     f"recover_jobs-last_iteration: Port {port.name} db toks {db_tags} and ref-token {token.tag}\n\tlen_t {len(token.tag.split('.'))} vs len_min_ts {min(sorted(db_len_tags))} => {len(token.tag.split('.')) != min(sorted(db_len_tags))}"
            # )

            # if has_same_depth_of_succ(db_tags, token, port):
            #     prefix = ".".join(token.tag.split(".")[:-1])
            #     step.combinator.iteration_map[prefix] = int(token.tag.split(".")[-1])
            #     logger.debug(
            #         f"recover_jobs-last_iteration: Step {step.name} combinator updated map[{prefix}] = {step.combinator.iteration_map[prefix]}"
            #     )

            prefix = ".".join(token.tag.split(".")[:-1])
            if prefix != "":
                step.combinator.iteration_map[prefix] = int(token.tag.split(".")[-1])
                logger.debug(
                    f"recover_jobs-last_iteration: Step {step.name} combinator updated map[{prefix}] = {step.combinator.iteration_map[prefix]}"
                )


# def has_same_depth_of_succ(db_tags, token, port):
#     # ordina la lista per tag,
#     # vedi la posizione di token.tag nella lista,
#     # vedi il tag successivo. (idea iniziale era precedente, ma con rollback annidati si perdono info sui token prodotti nella port iniziale)
#     # se ha un livello meno, allora return true
#     from functools import cmp_to_key
#
#     db_tags.sort(key=cmp_to_key(compare_tags))
#     if token.tag in db_tags:
#         index = db_tags.index(token.tag)
#     else:
#         raise Exception("Tag non trovato")
#     logger.debug(
#         f"recover_jobs-last_iteration: Port {port.name} db toks {db_tags} and ref-token {token.tag}. Index: {index}"
#     )
#     # if index > 0:
#     #     res = len(db_tags[index - 1].split(".")) == len(db_tags[index].split("."))
#     #     return res
#     if index < len(db_tags) - 1:
#         res = len(db_tags[index + 1].split(".")) == len(db_tags[index].split("."))
#         return res
#     return False


def get_failed_loop_conditional_step(new_workflow, wr):
    if not wr.external_loop_step_name:
        return None

    return wr.external_loop_step


async def _populate_workflow(
    wr,
    port_ids: Iterable[int],
    step_ids: Iterable[int],
    failed_step,
    new_workflow,
    loading_context,
):
    logger.debug(
        f"populate_workflow: wf {new_workflow.name} dag[INIT] {wr.dag_ports[INIT_DAG_FLAG]}"
    )
    logger.debug(
        f"populate_workflow: wf {new_workflow.name} port {new_workflow.ports.keys()}"
    )
    await load_and_add_ports(port_ids, new_workflow, loading_context)

    step_name_id = await load_and_add_steps(step_ids, new_workflow, wr, loading_context)

    await load_missing_ports(new_workflow, step_name_id, loading_context)

    # add failed step into new_workflow
    logger.debug(
        f"populate_workflow: wf {new_workflow.name} add_3.0 step {failed_step.name}"
    )
    new_workflow.add_step(
        await Step.load(
            new_workflow.context,
            failed_step.persistent_id,
            loading_context,
            new_workflow,
        )
    )
    for port in await asyncio.gather(
        *(
            asyncio.create_task(
                Port.load(
                    new_workflow.context, p.persistent_id, loading_context, new_workflow
                )
            )
            for p in failed_step.get_output_ports().values()
        )
    ):
        if port.name not in new_workflow.ports.keys():
            logger.debug(
                f"populate_workflow: wf {new_workflow.name} add_3 port {port.name}"
            )
            new_workflow.add_port(port)

    if replace_step := get_failed_loop_conditional_step(new_workflow, wr):
        port_name = list(replace_step.input_ports.values()).pop()
        ll_cond_step = CWLRecoveryLoopConditionalStep(
            replace_step.name
            if isinstance(replace_step, CWLRecoveryLoopConditionalStep)
            else replace_step.name + "-recovery",
            new_workflow,
            get_recovery_loop_expression(
                # len(wr.port_tokens[port_name])
                1
                if len(wr.port_tokens[port_name]) == 1
                else len(wr.port_tokens[port_name]) - 1
            ),
            full_js=True,
        )
        logger.debug(
            f"Step {ll_cond_step.name} (wf {new_workflow.name}) set with expression: {ll_cond_step.expression}"
        )
        for dep_name, port in replace_step.get_input_ports().items():
            logger.debug(
                f"Step {ll_cond_step.name} (wf {new_workflow.name}) add input port {dep_name} {port.name}"
            )
            ll_cond_step.add_input_port(dep_name, port)
        for dep_name, port in replace_step.get_output_ports().items():
            logger.debug(
                f"Step {ll_cond_step.name} (wf {new_workflow.name}) add output port {dep_name} {port.name}"
            )
            ll_cond_step.add_output_port(dep_name, port)
        for dep_name, port in replace_step.get_skip_ports().items():
            logger.debug(
                f"Step {ll_cond_step.name} (wf {new_workflow.name}) add skip port {dep_name} {port.name}"
            )
            ll_cond_step.add_skip_port(dep_name, port)
        new_workflow.steps.pop(replace_step.name)
        new_workflow.add_step(ll_cond_step)
        logger.debug(
            f"populate_workflow: (3) Step {ll_cond_step.name} caricato nel wf {new_workflow.name}"
        )
        logger.debug(
            f"populate_workflow: Rimuovo lo step {replace_step.name} dal wf {new_workflow.name} perché lo rimpiazzo con il nuovo step {ll_cond_step.name}"
        )

    # fixing skip ports in loop-terminator
    for step in new_workflow.steps.values():
        if isinstance(step, CombinatorStep) and isinstance(
            step.combinator, LoopTerminationCombinator
        ):
            dependency_names = set()
            for dep_name, port_name in step.input_ports.items():
                # Some data are available so added directly in the LoopCombinatorStep inputs.
                # In this case, LoopTerminationCombinator must not wait on ports where these data are created.
                if port_name not in new_workflow.ports.keys():
                    dependency_names.add(dep_name)
            for name in dependency_names:
                step.input_ports.pop(name)
                step.combinator.items.remove(name)

    # remove steps which have not input ports loaded in new workflow
    steps_to_remove = set()
    for step in new_workflow.steps.values():
        if isinstance(step, InputInjectorStep):
            continue
        for p_name in step.input_ports.values():
            if p_name not in new_workflow.ports.keys():
                # problema nato dai loop. Vengono caricati nel new_workflow tutti gli step che hanno come output le port
                # nel grafo. Però nei loop, più step hanno stessa porta di output (forward, backprop, loop-term).
                # per capire se lo step sia necessario controlliamo che anche le sue port di input siano state caricate
                logger.debug(
                    f"populate_workflow: Rimuovo step {step.name} dal wf {new_workflow.name} perché manca la input port {p_name}"
                )
                steps_to_remove.add(step.name)
        if step.name not in steps_to_remove and isinstance(
            step, BackPropagationTransformer
        ):
            loop_terminator = False
            for port in step.get_output_ports().values():
                for prev_step in port.get_input_steps():
                    if isinstance(prev_step, CombinatorStep) and isinstance(
                        prev_step.combinator, LoopTerminationCombinator
                    ):
                        loop_terminator = True
                        break
                if loop_terminator:
                    break
            if not loop_terminator:
                logger.debug(
                    f"populate_workflow: Rimuovo step {step.name} dal wf {new_workflow.name} perché manca come prev step un LoopTerminationCombinator"
                )
                steps_to_remove.add(step.name)
    for step_name in steps_to_remove:
        logger.debug(
            f"populate_workflow: Rimozione (2) definitiva step {step_name} dal new_workflow {new_workflow.name}"
        )
        new_workflow.steps.pop(step_name)

    graph = None
    # todo tmp soluzione per format_graph ripetuti. Risolvere a monte, nel momento in cui si fa la load
    for s in new_workflow.steps.values():
        if isinstance(s, CWLTokenTransformer) and isinstance(
            s.processor, CWLTokenProcessor
        ):
            if not graph:
                graph = s.processor.format_graph
            else:
                s.processor.format_graph = graph
    logger.debug("populate_workflow: Finish")


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
