from __future__ import annotations

import posixpath
import re
import asyncio
import logging
import os
import itertools
import tempfile
import token
from asyncio import Lock
from typing import MutableMapping, MutableSequence, cast, MutableSet

import pkg_resources

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataLocation, DataType
from streamflow.core.utils import random_name, get_class_fullname
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import (
    FailureHandlingException,
    UnrecoverableTokenException,
)
from streamflow.core.recovery import FailureManager, ReplayRequest, ReplayResponse
from streamflow.core.workflow import (
    CommandOutput,
    Job,
    Status,
    Step,
    Port,
    Token,
    TokenProcessor,
)
from streamflow.cwl.processor import CWLCommandOutput
from streamflow.cwl.step import CWLTransferStep
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.transformer import CWLTokenTransformer
from streamflow.data import remotepath
from streamflow.data.data_manager import RemotePathMapper
from streamflow.log_handler import logger
from streamflow.recovery.recovery import JobVersion
from streamflow.workflow.step import ExecuteStep, ScatterStep
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import (
    ExecuteStep,
    DeployStep,
    ScheduleStep,
    CombinatorStep,
)
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.token import TerminationToken, JobToken, ListToken, ObjectToken

# from streamflow.workflow.utils import get_token_value, get_files_from_token

# from streamflow.main import build_context
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow

# import networkx as nx
# import matplotlib.pyplot as plt
# from dyngraphplot import DynGraphPlot
import graphviz


def add_step(step, steps):
    found = False
    for s in steps:
        found = found or s.name == step.name
    if not found:
        steps.append(step)


def add_pair(step_name, label, step_labels, tokens):
    for curr_step_name, curr_label in step_labels:
        if curr_step_name == step_name and curr_label == label:
            return
    step_labels.append((step_name, label))


async def _load_steps_from_token(token, context, loading_context, new_workflow):
    # TODO: quando interrogo sulla tabella dependency (tra step e port) meglio recuperare anche il nome della dipendenza
    # così posso associare subito token -> step e a quale porta appartiene
    # edit. Impossibile. Recuperiamo lo step dalla porta di output. A noi serve la port di input
    row_token = await context.database.get_token(token.persistent_id)
    steps = []
    if row_token:
        row_steps = await context.database.get_step_from_output_port(row_token["port"])
        for r in row_steps:
            st = await Step.load(
                context,
                r["step"],
                loading_context,
                new_workflow,
            )
            steps.append(
                st,
            )
            # due modi alternativi per ottenre il nome della output_port che genera il token in questione
            #    [ op.name for op in workflow.steps[st.name].output_ports if op.persistent_id == int(row_token['port'])][0]
            # (await context.database.get_port(row_token['port']))['name']

    return steps


def print_graph_figure(graph, title):
    dot = graphviz.Digraph(title)
    for vertex, neighbors in graph.items():
        dot.node(str(vertex))
        for n in neighbors:
            dot.edge(str(vertex), str(n))
    # print(dot.source)
    dot.view("dev/" + title + ".gv")  # tempfile.mktemp('.gv')


def print_graph_figure_label(graph, title):
    dot = graphviz.Digraph(title)
    for vertex, neighbors in graph.items():
        dot.node(str(vertex))
        for n, l in neighbors:
            dot.edge(str(vertex), str(n), label=str(l))
    # print(dot.source)
    dot.view("dev/" + title + ".gv")  # tempfile.mktemp('.gv')


async def print_graph(job_version, loading_context):
    """
    FUNCTION FOR DEBUGGING
    """
    rows = await job_version.step.workflow.context.database.get_all_provenance()
    tokens = {}
    graph = {}
    for row in rows:
        dependee = (
            await loading_context.load_token(
                job_version.step.workflow.context, row["dependee"]
            )
            if row["dependee"]
            else -1
        )
        depender = (
            await loading_context.load_token(
                job_version.step.workflow.context, row["depender"]
            )
            if row["depender"]
            else -1
        )
        curr_key = dependee.persistent_id if dependee != -1 else -1
        if curr_key not in graph.keys():
            graph[curr_key] = set()
        graph[curr_key].add(depender.persistent_id)
        tokens[depender.persistent_id] = depender
        tokens[curr_key] = dependee

    steps_token = {}
    graph_steps = {}
    for k, values in graph.items():
        if k != -1:
            k_step = (
                await _load_steps_from_token(
                    tokens[k],
                    job_version.step.workflow.context,
                    loading_context,
                    job_version.step.workflow,
                )
            ).pop()
        step_name = k_step.name if k != -1 else INIT_DAG_FLAG
        if step_name not in graph_steps.keys():
            graph_steps[step_name] = set()
        if step_name not in steps_token.keys():
            steps_token[step_name] = set()
        steps_token[step_name].add(k)

        for v in values:
            s = (
                await _load_steps_from_token(
                    tokens[v],
                    job_version.step.workflow.context,
                    loading_context,
                    job_version.step.workflow,
                )
            ).pop()
            graph_steps[step_name].add(s.name)
            if s.name not in steps_token.keys():
                steps_token[s.name] = set()
            steps_token[s.name].add(v)

    valid_steps_graph = {}
    for step_name_1, steps_name in graph_steps.items():
        valid_steps_graph[step_name_1] = []
        for step_name_2 in steps_name:
            for label in steps_token[step_name_1]:
                add_pair(
                    step_name_2,
                    str_token_value(tokens[label]) + f"({label})",
                    valid_steps_graph[step_name_1],
                    tokens,
                )

    print_graph_figure_label(valid_steps_graph, "get_all_provenance_steps")
    wf_steps = sorted(job_version.step.workflow.steps.keys())
    pass


def str_token_value(token):
    if isinstance(token, CWLFileToken):
        return token.value["class"]  # token.value['path']
    if isinstance(token, ListToken):
        return str([str_token_value(t) for t in token.value])
    if isinstance(token, JobToken):
        return token.value.name
    if isinstance(token, TerminationToken):
        return "T"
    if isinstance(token, Token):
        return str(token.value)
    return "None"


def search_step_name_into_graph(graph_tokens):
    for tokens in graph_tokens.values():
        for s in tokens:
            if isinstance(s, str) and s != INIT_DAG_FLAG:
                return s
    raise Exception("Step name non trovato")


async def printa_token(token_visited, workflow, graph_tokens, loading_context):
    token_values = {}
    for token_id, (token, _) in token_visited.items():
        token_values[token_id] = str_token_value(token)
    token_values[INIT_DAG_FLAG] = INIT_DAG_FLAG
    step_name = search_step_name_into_graph(graph_tokens)
    token_values[step_name] = step_name

    graph_steps = {}
    for token_id, tokens_id in graph_tokens.items():
        step_1 = (
            (
                await _load_steps_from_token(
                    token_visited[token_id][0],
                    workflow.context,
                    loading_context,
                    workflow,
                )
            )
            .pop()
            .name
            if isinstance(token_id, int)
            else token_values[token_id]
        )
        steps_2 = set()
        label = (
            str_token_value(token_visited[token_id][0]) + f"({token_id})"
            if isinstance(token_id, int)
            else token_values[token_id]
        )
        for token_id_2 in tokens_id:
            step_2 = (
                (
                    await _load_steps_from_token(
                        token_visited[token_id_2][0],
                        workflow.context,
                        loading_context,
                        workflow,
                    )
                )
                .pop()
                .name
                if isinstance(token_id_2, int)
                else token_values[token_id_2]
            )
            steps_2.add(step_2)
        if step_1 != INIT_DAG_FLAG:
            graph_steps[step_1] = [(s, label) for s in steps_2]
    print_graph_figure_label(graph_steps, "graph_steps recovery")


#################################################
#################################################
#################################################
#################################################
#################################################
#################################################


async def _cleanup_dir(
    connector: Connector, location: Location, directory: str
) -> None:
    await remotepath.rm(
        connector, location, await remotepath.listdir(connector, location, directory)
    )


async def _load_prev_tokens(token_id, loading_context, context):
    rows = await context.database.get_dependee(token_id)

    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["dependee"]))
            for row in rows
        )
    )


async def data_location_exists(data_locations, context, token):
    for data_loc in data_locations:
        if data_loc.path == token.value["path"]:
            connector = context.deployment_manager.get_connector(data_loc.deployment)
            # location_allocation = job_version.step.workflow.context.scheduler.location_allocations[data_loc.deployment][data_loc.name]
            # available_locations = job_version.step.workflow.context.scheduler.get_locations(location_allocation.jobs[0])
            if exists := await remotepath.exists(
                connector, data_loc, token.value["path"]
            ):
                print(
                    f"token.id: {token.persistent_id} in loc {data_loc} -> exists {exists} "
                )
                return True
            print(
                f"token.id: {token.persistent_id} in loc {data_loc} -> exists {exists} "
            )
    return False


def _get_data_location(path, context):
    data_locs = context.data_manager.get_data_locations(path)
    for data_loc in data_locs:
        if data_loc.path == path:
            return data_loc
    return None


async def is_token_available(token, context):
    if isinstance(token, CWLFileToken):
        # TODO: è giusto cercare una loc dal suo path? se va bene aggiustare data_location_exists method
        data_loc = _get_data_location(token.value["path"], context)
        print(f"token.id: {token.persistent_id} ({token.value['path']})")
        if not data_loc:  # if the data are in invalid locations, data_loc is None
            return False
        if not await data_location_exists([data_loc], context, token):
            context.data_manager.invalidate_location(data_loc, token.value["path"])
            # context.data_manager.invalidate_location(data_loc, "/")
            return False
        return True
    if isinstance(token, ListToken):
        # if at least one file does not exist, returns false
        a = await asyncio.gather(
            *(
                asyncio.create_task(is_token_available(inner_token, context))
                for inner_token in token.value
            )
        )
        b = all(a)
        return b
    if isinstance(token, ObjectToken):
        # TODO: sistemare
        return True
    if isinstance(token, JobToken):
        return False
    return True


async def _put_tokens(
    new_workflow: Workflow,
    token_port: MutableMapping[int, str],
    port_tokens_counter: MutableMapping[str, int],
    dag_tokens: MutableMapping[int, MutableSet[int]],
    token_visited: MutableMapping[int, bool],
):
    # add tokens into the ports
    for token_id in dag_tokens[INIT_DAG_FLAG]:
        port = new_workflow.ports[token_port[token_id]]
        port.put(token_visited[token_id][0])
        if len(port.token_list) == port_tokens_counter[port.name]:
            port.put(TerminationToken())


async def _populate_workflow(
    failed_step,
    token_visited,
    new_workflow,
    loading_context,
):
    steps = set()
    ports = {}

    token_port = {}  # { token.id : port.name }
    port_tokens_counter = {}  # {port.name : n.of tokens}
    for token_id, (_, is_available) in token_visited.items():
        row = await failed_step.workflow.context.database.get_port_from_token(token_id)
        if row["id"] in ports.keys():
            ports[row["id"]] = ports[row["id"]] and is_available
        else:
            ports[row["id"]] = is_available

        # save the token and its port name
        token_port[token_id] = row["name"]

        # save the port name and tokens number in the DAG it produces
        if row["name"] not in port_tokens_counter.keys():
            port_tokens_counter[row["name"]] = 0
        port_tokens_counter[row["name"]] += 1

    for port in await asyncio.gather(
        *(
            Port.load(
                failed_step.workflow.context,
                port_id,
                loading_context,
                new_workflow,
            )
            for port_id in ports.keys()
        )
    ):
        new_workflow.add_port(port)

    for rows_dependency in await asyncio.gather(
        *(
            failed_step.workflow.context.database.get_step_from_output_port(port_id)
            for port_id, is_available in ports.items()
            if not is_available
        )
    ):
        for row_dependency in rows_dependency:
            steps.add(row_dependency["step"])
    for step in await asyncio.gather(
        *(
            Step.load(
                failed_step.workflow.context,
                step_id,
                loading_context,
                new_workflow,
            )
            for step_id in steps
        )
    ):
        new_workflow.add_step(step)
    return token_port, port_tokens_counter


INIT_DAG_FLAG = "init"


def add_elem_dictionary(key, elem, dictionary):
    if key not in dictionary.keys():
        dictionary[key] = set()
    dictionary[key].add(elem)


def get_token_by_tag(token_tag, token_list):
    for token in token_list:
        if token_tag == token.tag:
            return token
    return None


def get_job_token_from_token_list(job_name, token_list):
    for token in token_list:
        if isinstance(token, JobToken):  # discard TerminationToken
            if token.value.name == job_name:
                return token
    return None


def contains_token_id(token_id, token_list):
    for token in token_list:
        if token_id == token.persistent_id:
            return True
    return False


class DefaultFailureManager(FailureManager):
    def __init__(
        self,
        context: StreamFlowContext,
        max_retries: int | None = None,
        retry_delay: int | None = None,
    ):
        super().__init__(context)
        self.jobs: MutableMapping[str, JobVersion] = {}
        self.max_retries: int = max_retries
        self.replay_cache: MutableMapping[str, ReplayResponse] = {}
        self.retry_delay: int | None = retry_delay
        self.wait_queues: MutableMapping[str, asyncio.Condition] = {}

        # {workflow.name : { port.id: [ token.id ] } }
        self.retags: MutableMapping[str, MutableMapping[int, MutableSequence[int]]] = {}
        self.job_tokens = {}

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        if job.name not in self.jobs:
            self.jobs[job.name] = JobVersion(
                job=Job(
                    name=job.name,
                    workflow_id=step.workflow.persistent_id,
                    inputs=dict(job.inputs),
                    input_directory=job.input_directory,
                    output_directory=job.output_directory,
                    tmp_directory=job.tmp_directory,
                ),
                outputs=None,
                step=step,
                version=1,
            )

        # todo: sistemarlo meglio
        self.jobs[job.name].job = job
        self.jobs[job.name].step = step
        command_output = await self._replay_job(self.jobs[job.name], job, step)
        return command_output

    # TODO: aggiungere nella classe dummy.
    def is_valid_tag(self, workflow_name, tag, output_port):
        if workflow_name not in self.retags.keys():
            return True
        if output_port.name not in self.retags[workflow_name].keys():
            return True

        token_list = self.retags[workflow_name][output_port.name]
        for t in token_list:
            if t.tag == tag:
                return False
        return True

    def _save_for_retag(self, new_workflow, dag_token, token_port):
        if new_workflow.name not in self.retags.keys():
            self.retags[new_workflow.name] = {}

        port_token = {}
        for t_id, p_name in token_port.items():
            if p_name not in port_token:
                port_token[p_name] = set()
            port_token[p_name].add(t_id)

        for step in new_workflow.steps.values():
            if isinstance(step, ScatterStep):
                port = step.get_output_port()
                for t_id in port_token[port.name]:
                    if t_id not in dag_token.keys():
                        add_elem_dictionary(
                            port.name, t_id, self.retags[new_workflow.name]
                        )

    async def _recover_jobs(self, failed_job, failed_step, loading_context):
        # await print_graph(job_version, loading_context)

        workflow = failed_step.workflow
        new_workflow = Workflow(
            context=workflow.context,
            type="cwl",
            name=random_name(),
            config=workflow.config,
        )

        if failed_step.persistent_id is None:
            raise Exception("step id NONEEEEEEEEEEE")

        job_token = get_job_token_from_token_list(
            failed_job.name,
            failed_step.get_input_port("__job__").token_list,
        )

        tokens = list(failed_job.inputs.values())  # tokens to check
        tokens.append(job_token)

        dag_tokens = {}  # { token.id : set of next tokens' id }
        for t in failed_job.inputs.values():
            dag_tokens[t.persistent_id] = set((failed_step.name,))

        token_visited = {}  # { token_id: (token, is_available)}

        while tokens:
            token = tokens.pop()
            is_available = await is_token_available(token, workflow.context)

            # impossible case because when added in tokens, the elem is checked
            if token.persistent_id in token_visited.keys():
                raise Exception("Token already visited")
            token_visited[token.persistent_id] = (token, is_available)

            if not is_available:
                prev_tokens = await _load_prev_tokens(
                    token.persistent_id,
                    loading_context,
                    workflow.context,
                )
                if prev_tokens:
                    for pt in prev_tokens:
                        add_elem_dictionary(
                            pt.persistent_id, token.persistent_id, dag_tokens
                        )
                        if (
                            pt.persistent_id not in token_visited.keys()
                            and not contains_token_id(pt.persistent_id, tokens)
                        ):
                            tokens.append(pt)
                else:
                    add_elem_dictionary(INIT_DAG_FLAG, token.persistent_id, dag_tokens)
            else:
                add_elem_dictionary(INIT_DAG_FLAG, token.persistent_id, dag_tokens)

        token_visited = dict(sorted(token_visited.items()))
        # await printa_token(token_visited, workflow, dag_tokens, loading_context)
        pass

        token_port, port_tokens_counter = await _populate_workflow(
            failed_step,
            token_visited,
            new_workflow,
            loading_context,
        )

        pass
        await _put_tokens(
            new_workflow,
            token_port,
            port_tokens_counter,
            dag_tokens,
            token_visited,
        )
        pass

        self._save_for_retag(new_workflow, dag_tokens, token_port)

        # free resources scheduler
        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                await workflow.context.scheduler.notify_status(
                    token.value.name, Status.WAITING
                )
        pass

        print("VIAAAAAAAAAAAAAA")
        for port in await asyncio.gather(
            *(
                Port.load(
                    new_workflow.context, p.persistent_id, loading_context, new_workflow
                )
                for p in failed_step.get_output_ports().values()
            )
        ):
            new_workflow.add_port(port)

        await new_workflow.save(workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        try:
            await executor.run()
        except Exception as err:
            print("ERROR", err)
            raise Exception("EXCEPTION ERR")

        # get new job created by ScheduleStep
        new_job_token = get_job_token_from_token_list(
            failed_job.name,
            new_workflow.ports[failed_step.get_input_port("__job__").name].token_list,
        )

        # get new job inputs
        new_inputs = {}
        for step_port_name, token in failed_job.inputs.items():
            original_port = failed_step.get_input_port(step_port_name)
            if original_port.name in new_workflow.ports.keys():
                new_inputs[step_port_name] = get_token_by_tag(
                    token.tag, new_workflow.ports[original_port.name].token_list
                )
            else:
                new_inputs[step_port_name] = get_token_by_tag(
                    token.tag, original_port.token_list
                )
        new_job_token.value.inputs = new_inputs

        self.job_tokens[failed_job.name] = new_job_token
        new_job = new_job_token.value
        new_step = await Step.load(
            new_workflow.context,
            failed_step.persistent_id,
            loading_context,
            new_workflow,
        )
        new_workflow.add_step(new_step)
        await new_step.save(new_workflow.context)

        print("Finito")
        cmd_out = await cast(ExecuteStep, new_step).command.execute(new_job)
        if cmd_out.status == Status.FAILED:
            logger.error(f"FAILED Job {new_job.name} with error:\n\t{cmd_out.value}")
            cmd_out = await self.handle_failure(new_job, new_step, cmd_out)

        return cmd_out

    # todo: aggiungere metodo in dummy. ritorna direttamente job_token
    async def get_valid_job_token(self, job_token):
        if job_token.value.name in self.job_tokens.keys():
            return self.job_tokens[job_token.value.name]
        return job_token

    async def _replay_job(
        self, job_version: JobVersion, failed_job, failed_step
    ) -> CommandOutput:
        job = job_version.job
        if self.max_retries is None or self.jobs[job.name].version < self.max_retries:
            # Update version
            self.jobs[job.name].version += 1
            try:
                loading_context = DefaultDatabaseLoadingContext()

                return await self._recover_jobs(
                    failed_job, failed_step, loading_context
                )
            # When receiving a FailureHandlingException, simply fail
            except FailureHandlingException as e:
                logger.exception(e)
                raise
            # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.exception(e)
                return await self.handle_exception(job, job_version.step, e)
        else:
            logger.error(
                f"FAILED Job {job.name} {self.jobs[job.name].version} times. Execution aborted"
            )
            raise FailureHandlingException()

    async def close(self):
        pass

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "default_failure_manager.json")
        )

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {type(exception).__name__} failure for job {job.name}"
            )
        return await self._do_handle_failure(job, step)

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Handling command failure for job {job.name}")
        return await self._do_handle_failure(job, step)


class DummyFailureManager(FailureManager):
    async def close(self):
        ...

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "dummy_failure_manager.json")
        )

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        raise exception

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        return command_output
