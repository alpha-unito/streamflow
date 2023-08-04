from __future__ import annotations

import asyncio
import json
import logging
import os
import datetime
from typing import MutableMapping, MutableSequence, cast, MutableSet, Tuple, Iterable

import pkg_resources

from streamflow.core.utils import random_name, get_class_fullname
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import (
    FailureHandlingException,
    WorkflowTransferException,
    WorkflowExecutionException,
)
from streamflow.core.recovery import FailureManager
from streamflow.core.workflow import CommandOutput, Job, Status, Step, Port, Token
from streamflow.cwl.token import CWLFileToken
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.recovery.recovery import JobVersion
from streamflow.token_printer import (
    printa_token,
    print_graph_figure,
    print_step_from_ports,
    str_token_value,
)
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import ScatterStep, CombinatorStep
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import ExecuteStep
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.token import (
    TerminationToken,
    JobToken,
    ListToken,
    IterationTerminationToken,
)

from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow

from streamflow.workflow.utils import get_job_token


async def _cleanup_dir(
    connector: Connector, location: Location, directory: str
) -> None:
    await remotepath.rm(
        connector, location, await remotepath.listdir(connector, location, directory)
    )


def local_str_token_value(token):
    if isinstance(token, CWLFileToken):
        return f"{token.value['class']} {token.value['basename']}"
    if isinstance(token, ListToken):
        return str([local_str_token_value(t) for t in token.value])
    if isinstance(token, JobToken):
        return token.value.name
    if isinstance(token, TerminationToken):
        return "T"
    if isinstance(token, IterationTerminationToken):
        return "IT"
    if isinstance(token, Token):
        if isinstance(token.value, Token):
            return "t(" + local_str_token_value(token.value)
        else:
            return f"{token.value}"
    return "None"


async def _load_prev_tokens(token_id, loading_context, context):
    rows = await context.database.get_dependees(token_id)

    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["dependee"]))
            for row in rows
        )
    )


async def _load_prev_tokens_newest(token_id, loading_context, context):
    rows2 = await context.database.get_dependees_newest_workflow(token_id)

    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["dependee"]))
            for row in rows2
        )
    )


def _get_data_location(path, context):
    data_locs = context.data_manager.get_data_locations(path)
    for data_loc in data_locs:
        if data_loc.path == path:
            return data_loc
    return None


def get_prev_ports(searched_port_name, dag_ports):
    start_port_names = set()
    for port_name, next_port_names in dag_ports.items():
        if searched_port_name in next_port_names and port_name != INIT_DAG_FLAG:
            start_port_names.add(port_name)
    return start_port_names


def get_port_tags(
    new_workflow, dag_ports, port_tokens, token_visited, failed_step_name
):
    # port_tags = {}
    # for port_name, next_port_names in dag_ports.items():
    #     if port_name == INIT_DAG_FLAG or port_name == failed_step_name:
    #         continue
    #     for t_id in port_tokens[port_name]:
    #         if isinstance(new_workflow.ports[port_name], JobPort):
    #             job = token_visited[t_id][0].value
    #             rand_input_token = {t.tag for t in job.inputs.values()}
    #             if len(rand_input_token) != 1:
    #                 raise FailureHandlingException(
    #                     f"Job {job.name} has not inputs or inputs with different tags {rand_input_token}"
    #                 )
    #             port_tags.setdefault(port_name, set()).add(rand_input_token.pop())
    #         else:
    #             port_tags.setdefault(port_name, set()).add(token_visited[t_id][0].tag)
    # for port_name, next_port_names in dag_ports.items():
    #     if port_name != INIT_DAG_FLAG and not isinstance(
    #         new_workflow.ports[port_name], (ConnectorPort, JobPort)
    #     ):
    #         # search next ports
    #         for next_port_name in next_port_names:
    #             if next_port_name != failed_step_name:
    #                 # search brother of port_name
    #                 for curr_port_name, curr_next_port_names in dag_ports.items():
    #                     if (
    #                         curr_port_name != port_name
    #                         and curr_port_name != INIT_DAG_FLAG
    #                         and next_port_name in curr_next_port_names
    #                         and not isinstance(
    #                             new_workflow.ports[curr_port_name],
    #                             ConnectorPort,
    #                         )
    #                     ):
    #                         tmp = port_tags[port_name].intersection(
    #                             port_tags[curr_port_name]
    #                         )
    #
    #                         if (
    #                             port_tags[port_name] - tmp
    #                             or port_tags[curr_port_name] - tmp
    #                         ):
    #                             print(
    #                                 f"wf {new_workflow.name} - Difference ports result: {tmp}"
    #                                 f"\n\tFrom port {port_name} ({port_tags[port_name]}) discards tags {port_tags[port_name] - tmp}"
    #                                 f"\n\t\tport_tokens[{port_name}]: {[ (t_id, token_visited[t_id][0].tag, local_str_token_value(token_visited[t_id][0])) for t_id in port_tokens[port_name]]}"
    #                                 f"\n\tFrom port {curr_port_name} ({port_tags[curr_port_name]}) discards tags {port_tags[curr_port_name] - tmp}"
    #                                 f"\n\t\tport_tokens[{curr_port_name}]: {[(t_id, token_visited[t_id][0].tag, local_str_token_value(token_visited[t_id][0])) for t_id in port_tokens[curr_port_name]]}"
    #                             )
    #                         # combinator input ports (it can be possible find inputs with different tags)
    #                         # todo: mettere un controllo attivo che siano effettivamente le input port di un combinator? Altrimenti se tmp è vuoto lanciare errore
    #                         if len(tmp) == 0:
    #                             # raise FailureHandlingException(
    #                             #     f"No token nell'intersezione di {port_name} e {curr_port_name}\n\t-> {port_tags[port_name]} e {port_tags[curr_port_name]}\n\t -> { [str_token_value(token_visited[t_id][0]) for t_id in port_tokens[port_name]] } e {[str_token_value(token_visited[t_id][0]) for t_id in port_tokens[curr_port_name]]}"
    #                             # )
    #                             continue
    #                         port_tags[port_name] = tmp
    #                         port_tags[curr_port_name] = tmp

    port_tags = {}
    for port_name in dag_ports[INIT_DAG_FLAG]:
        intersection_tags = {
            token_visited[t_id][0].tag for t_id in port_tokens[port_name]
        }
        if not isinstance(new_workflow.ports[port_name], (ConnectorPort, JobPort)):
            for next_port_name in dag_ports[port_name]:
                for same_level_port_name in get_prev_ports(next_port_name, dag_ports):
                    if same_level_port_name != port_name and not isinstance(
                        new_workflow.ports[port_name], (ConnectorPort, JobPort)
                    ):
                        intersection_tags.intersection(
                            {
                                token_visited[t_id][0].tag
                                for t_id in port_tokens[same_level_port_name]
                            }
                        )
        # print(f"Port {port_name} valid tags ", intersection_tags)
        if port_name in port_tags.keys():
            raise FailureHandlingException(
                f"Port {port_name} già presente nei tags",
                port_tags[port_name],
                "Altri tags trovati",
                intersection_tags,
            )
        port_tags[port_name] = intersection_tags

    return port_tags


async def _put_tokens(
    workflow: Workflow,
    new_workflow: Workflow,
    init_ports: MutableSequence[str],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
    port_tokens_counter: MutableMapping[str, int],
    dag_ports: MutableMapping[str, MutableSequence[str]],
    failed_step_name: str,
):
    port_tags = get_port_tags(
        new_workflow, dag_ports, port_tokens, token_visited, failed_step_name
    )
    for port_name in init_ports:
        port = new_workflow.ports[port_name]
        # token_list = [
        #     token_visited[t_id][0]
        #     for t_id in port_tokens[port_name]
        #     if token_visited[t_id][1]
        #     and (
        #         new_workflow.name not in tags.keys()
        #         or port_name not in tags[new_workflow.name].keys()
        #         or token_visited[t_id][0].tag in tags[new_workflow.name][port_name]
        #     )
        # ]
        token_list = [
            token_visited[t_id][0]
            for t_id in port_tokens[port_name]
            if token_visited[t_id][1]
            and token_visited[t_id][0].tag in port_tags[port_name]
        ]
        token_list.sort(key=lambda x: x.tag, reverse=False)
        tmpa = [
            s.name
            for s in port.get_input_steps()
            if s.name == "/tosort/out_file-gather"
        ]
        pass
        for i, t in enumerate(token_list):
            for t1 in token_list[i:]:
                if t.persistent_id != t1.persistent_id and t.tag == t1.tag:
                    raise FailureHandlingException(
                        f"Tag ripetuto id1: {t.persistent_id} id2: {t1.persistent_id}"
                    )

        for t in token_list:
            if isinstance(t, TerminationToken):
                raise FailureHandlingException(
                    f"Aggiungo un termination token nell port {port.name} ma non dovrei"
                )
            port.put(t)
        print(
            f"port {port.name}\n"
            f"\tport_tokens_counter[port_name]\t\t= {port_tokens_counter[port.name]}\n"
            # f"\ttoken_list_in_workflow_port - 1\t\t= {len(workflow.ports[port.name].token_list) - 1}\n"
            f"\t=> len(port.token_list)\t\t\t= {len(port.token_list)}\n"
            f"\t=> len(port_tokens[port_name])\t\t= {len(port_tokens[port.name])}\n"
            f"\tlen(port_tokens[port_name] avai)\t= {len([ t_id for t_id in port_tokens[port.name] if token_visited[t_id][1]])}"
        )
        if len(port.token_list) > 0 and len(port.token_list) == len(
            port_tokens[port_name]
        ):
            print(
                f"Port {port.name} with {len(port.token_list)} tokens. It will be added TerminationToken\n"
            )
            port.put(TerminationToken())
        else:
            print(
                f"Port {port.name} with {len(port.token_list)} tokens. NO TerminationToken\n"
            )
        pass


async def _populate_workflow(
    failed_step,
    token_visited,
    new_workflow,
    loading_context,
    port_tokens,
):
    steps = set()

    # { id : all_tokens_are_available }
    ports = {}
    port_names = set()

    # {port.name : n.of tokens}
    port_tokens_counter = {}

    for token_id, (_, is_available) in token_visited.items():
        row = await failed_step.workflow.context.database.get_port_from_token(token_id)

        # in teoria questa porta non verrà mai utilizzata
        # if row['name'] in failed_step.output_ports.values():
        #     continue
        if row["id"] in ports.keys():
            ports[row["id"]] = ports[row["id"]] and is_available
        else:
            ports[row["id"]] = is_available

        port_names.add(row["name"])
        # save the port name and tokens number in the DAG it produces
        if row["name"] not in port_tokens_counter.keys():
            port_tokens_counter[row["name"]] = 0
        port_tokens_counter[row["name"]] += 1

    # add port into new_workflow
    for port in await asyncio.gather(
        *(
            Port.load(
                new_workflow.context,
                port_id,
                loading_context,
                new_workflow,
            )
            for port_id in ports.keys()
        )
    ):
        new_workflow.add_port(port)

    # add step into new_workflow
    for rows_dependency in await asyncio.gather(
        *(
            new_workflow.context.database.get_step_from_output_port(port_id)
            for port_id, is_available in ports.items()
            if not is_available
        )
    ):
        for row_dependency in rows_dependency:
            steps.add(row_dependency["step"])
    for step in await asyncio.gather(
        *(
            Step.load(
                new_workflow.context,
                step_id,
                loading_context,
                new_workflow,
            )
            for step_id in steps
        )
    ):
        new_workflow.add_step(step)

    # add output port of failed step into new_workflow
    for port in await asyncio.gather(
        *(
            Port.load(
                new_workflow.context, p.persistent_id, loading_context, new_workflow
            )
            for p in failed_step.get_output_ports().values()
        )
    ):
        new_workflow.add_port(port)
    return port_tokens_counter


INIT_DAG_FLAG = "init"


# todo: move it in utils
def get_token_by_tag(token_tag, token_list):
    for token in token_list:
        if token_tag == token.tag:
            return token
    return None


async def _is_token_available(token, context):
    return not isinstance(token, JobToken) and await token.is_available(context)
    # return (
    #     await token.is_available(context) if not isinstance(token, JobToken) else False
    # )


# todo: move it in utils
def contains_token_id(token_id, token_list):
    return token_id in (t.persistent_id for t in token_list)


# todo: cambiare nome funzione
# controlla se la port ha generato due token con lo stesso tag
# se è vero significa che c'è stato un rollback...quali dei due token devo prendere?
def is_token_aging(port_tokens, port_row, all_token_visited):
    for token_id in port_tokens[port_row["name"]]:
        for token_id_2 in port_tokens[port_row["name"]]:
            if (
                token_id != token_id_2
                and all_token_visited[token_id][0].tag
                == all_token_visited[token_id_2][0].tag
            ):
                # todo: caso in cui i jobtoken sono diversi avviene quando c'è un transferException. i jobtoken generati sono diversi, ma è giusto così. in quel caso come procedo? continuo la ricerca su quel branch o no?
                aa = all_token_visited[token_id][0]
                ab = all_token_visited[token_id_2][0]
                print(f"Interrotto branch di ricerca token {token_id_2} ({type(ab)})")
                return True
    return False


class JobRequest:
    def __init__(self):
        self.job_token: JobToken = None
        self.token_output: MutableMapping[str, Token] = {}
        self.lock = asyncio.Condition()
        self.is_running = True


def str_value(value):
    if isinstance(value, dict):
        return value["basename"]
    return value


def _clean_port_tokens(
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_to_remove: MutableSequence[int],
    dag_ports: MutableMapping[str, MutableSequence[str]],
    available_ports: MutableSequence[str],
):
    for port_name, token_id_list in port_tokens.items():
        for t_id in token_to_remove:
            if t_id in token_id_list:
                token_id_list.remove(t_id)
                print(f"Port_tokens - Port {port_name} removes token {t_id}")

    empty_ports = set()
    for port_name, next_port_names in dag_ports.items():
        for p_name in available_ports:
            if p_name in next_port_names:
                next_port_names.remove(p_name)
        if len(next_port_names) == 0:
            empty_ports.add(port_name)
    for port_name in empty_ports:
        dag_ports.pop(port_name)
        port_tokens.pop(port_name)
        print(f"Pop {port_name} for dag_ports and port_tokens")
    return empty_ports


def _check_port_is_attached(dag_ports, searching_port_name):
    for port_name in dag_ports.keys():
        if port_name == searching_port_name:
            return True
    return False


def _update_dag_ports(
    dag_ports: MutableMapping[str, MutableSequence[str]],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    empty_ports: MutableSequence[str],
    new_workflow_name: str,
):
    new_empty_ports = set()
    for port_name, next_port_names in dag_ports.items():
        for empty_port in empty_ports:
            if empty_port in next_port_names:
                next_port_names.remove(empty_port)
        # There is no next ports, so port_name is not even more connected to the main graph
        if len(next_port_names) == 0:
            new_empty_ports.add(port_name)
    for port_name in new_empty_ports:
        dag_ports.pop(port_name)
        port_tokens.pop(port_name)
        print(f"wf {new_workflow_name} - Pop {port_name} for dag_ports and port_tokens")
    return new_empty_ports


def temp_print_retag(workflow_name, output_port, tag, retags, final_msg):
    print(
        f"problema scatter:out3 wf {workflow_name} - port {output_port.name} - tag {tag}",
        "\nretags",
        json.dumps(
            {
                k: {
                    p: [
                        {
                            "id": t.persistent_id,
                            "tag": t.tag,
                            "value": str_value(t.value),
                            "class": get_class_fullname(type(t)),
                        }
                        for t in t_list
                    ]
                    for p, t_list in v.items()
                }
                for k, v in retags.items()
            },
            indent=2,
        ),
        final_msg,
    )


def is_tag_present(token, port_name, port_tokens, token_visited):
    return (
        get_token_by_tag(
            token.tag, (token_visited[t_id] for t_id in port_tokens[port_name])
        )
        is not None
    )


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
        self.retry_delay: int | None = retry_delay

        print(
            "DefaultFailureManager.max_retries:",
            max_retries,
            ".retry_delay:",
            retry_delay,
        )

        # { workflow.name : { port.id: [ token ] } }
        self.retags: MutableMapping[
            str, MutableMapping[str, MutableSequence[Token]]
        ] = {}

        # { job.name : RequestJob }
        self.job_requests: MutableMapping[str, JobRequest] = {}

    async def _has_token_already_been_recovered(
        self,
        token,
        token_visited,
        available_new_job_tokens,
        running_new_job_tokens,
        context,
        loading_context,
        port_tokens,
        token_frontier,
    ):
        if isinstance(token, JobToken) and token.value.name in self.job_requests.keys():
            async with self.job_requests[token.value.name].lock:
                job_request = self.job_requests[token.value.name]

                # todo: togliere parametro is_running, usare lo status dello step
                # edit: lasciare is_running perché mi segnala che lo step è in rollback. Lo status mi segna se è in esecuzione. (forse?)
                if job_request.is_running:
                    print(
                        f"Il job {token.value.name} {token.persistent_id} - è running, dovrei aspettare il suo output"
                    )

                    running_new_job_tokens[token.persistent_id] = {
                        "jobtoken": job_request
                    }
                    # raise FailureHandlingException(f"Job {token.value.name} is already running. This case is still a working progress")
                elif (
                    job_request.job_token
                    and job_request.job_token.persistent_id
                    and token.persistent_id != job_request.job_token.persistent_id
                    and job_request.token_output
                ):
                    # todo: Fare lo load del token direttamente quando recupero
                    #   così se più avanti serve, non si deve riaccedere al db ....
                    #   update: FORCE NON SERVE FARLO
                    out_tokens_json = (
                        await context.database.get_out_tokens_from_job_token(
                            token.persistent_id
                        )
                    )
                    # out_tokens_json dovrebbe essere una lista...caso in cui un job produce più token
                    print(
                        f"Il job {token.value.name} ({token.persistent_id})"
                        " ha il out_token_json",
                        dict(out_tokens_json) if out_tokens_json else "null",
                    )
                    if out_tokens_json:
                        port_json = await context.database.get_port_by_token(
                            out_tokens_json["id"]
                        )
                        print(
                            f"Il job {token.value.name} ({token.persistent_id})"
                            " ha il port_json",
                            dict(port_json),
                        )
                        disp = await _is_token_available(
                            job_request.token_output[port_json["name"]], context
                        )
                        print(
                            f"Il job {token.value.name} {token.persistent_id} - il token di output {job_request.token_output[port_json['name']]} è già disponibile? {disp}"
                        )
                        if disp:
                            available_new_job_tokens[token.persistent_id] = {
                                "out-token-port-name": port_json["name"],
                                "new-job-token": job_request.job_token.persistent_id,
                                "new-out-token": job_request.token_output[
                                    port_json["name"]
                                ].persistent_id,
                                "old-job-token": token.persistent_id,
                                "old-out-token": out_tokens_json["id"],
                            }
                            token_visited[token.persistent_id] = (
                                token,
                                True,
                            )
                            token_visited[job_request.job_token.persistent_id] = (
                                job_request.job_token,
                                True,
                            )
                            token_visited[
                                job_request.token_output[
                                    port_json["name"]
                                ].persistent_id
                            ] = (job_request.token_output[port_json["name"]], True)
                            a = port_json["name"]
                            b = job_request.token_output[port_json["name"]]
                            print(
                                f"Updated port_tokens. Added in port_name_key {a} token {b.persistent_id}"
                            )
                            # port_tokens.setdefault(port_json["name"], set()).add(
                            #     job_request.token_output[
                            #         port_json["name"]
                            #     ].persistent_id
                            # )
                            # return False
                            return True  # todo: temporaneo. Ripristinare a true appena si implementa il metodo nuovo di sostituzione con le port
                    # else:
                    #     print(
                    #         f"Il job {token.value.name} {token.persistent_id} - impossibile recuperare il token perché non so quale porta è: {job_request.token_output} "
                    #     )
                    pass
        return False

    def clean_dag(self, dag_tokens, token_id_list):
        next_round = set()
        for key in dag_tokens.keys():
            dag_tokens[key].difference_update(token_id_list)
            if not dag_tokens[key]:  # key has empty set in value
                next_round.add(key)
        return next_round

    # todo: togliere async . serve solo per stampare il dag dei token
    async def _update_dag(
        self,
        dag_tokens,
        old_out_token,
        new_out_token,
        token_visited,
        new_workflow,
        loading_context,
    ):
        try:
            values = dag_tokens.pop(old_out_token)
        except Exception as e:
            await printa_token(
                token_visited,
                new_workflow,
                dag_tokens,
                loading_context,
                "caduto",
            )
            print(
                "error dumps",
                json.dumps({k: list(v) for k, v in dag_tokens.items()}, indent=2),
            )
            raise e
        dag_tokens[new_out_token] = values
        dag_tokens[INIT_DAG_FLAG].add(new_out_token)

    async def _build_dag(
        self,
        tokens,
        failed_job,
        failed_step,
        workflow,
        loading_context,
        new_workflow_name,
    ):
        # port -> next ports
        # { port_name : set(port_names) }
        dag_ports = {}

        dag_tokens = {}

        # { port_name : set(token_ids) }
        port_tokens = {}

        # { token_id: (token, is_available)}
        all_token_visited = {}

        # {old token id : job token running}
        running_new_job_tokens = {}

        # {old job token id : (new job token id, new output token id)}
        available_new_job_tokens = {}

        for k, t in failed_job.inputs.items():
            if t.persistent_id is None:
                raise FailureHandlingException("Token has not a persistent_id")
            # se lo step è Transfer, allora non tutti gli input del job saranno nello step
            if k in failed_step.input_ports.keys():
                dag_ports[failed_step.get_input_port(k).name] = set((failed_step.name,))
                dag_tokens[t.persistent_id] = set((failed_step.name,))
            else:
                print(f"Step {failed_step.name} has not the input port {k}")
        dag_ports[failed_step.get_input_port("__job__").name] = set((failed_step.name,))
        dag_tokens[
            get_job_token(
                failed_job.name, failed_step.get_input_port("__job__").token_list
            ).persistent_id
        ] = set((failed_step.name,))

        lost_ports = set()

        port_name_id = {}

        while tokens:
            token = tokens.pop()

            if not await self._has_token_already_been_recovered(
                token,
                all_token_visited,
                available_new_job_tokens,
                running_new_job_tokens,
                workflow.context,
                loading_context,
                port_tokens,
                tokens,
            ):
                is_available = await _is_token_available(token, workflow.context)
                if isinstance(token, CWLFileToken):
                    print(
                        f"CWLFileToken ({token.persistent_id})",
                        token,
                        "is available",
                        is_available,
                    )
                if isinstance(token, JobToken):
                    print(f"JobToken ({token.persistent_id})", token.value.name)

                # token added by _has_token_already_been_recovered method
                if (
                    token.persistent_id in all_token_visited.keys()
                    and token.persistent_id in available_new_job_tokens.keys()
                ):
                    print(
                        f"Token {token} già aggiunto dalla funzione _has_token_already_been_recovered"
                    )
                    continue
                # impossible case because when added in tokens, the elem is checked
                if token.persistent_id in all_token_visited.keys():
                    raise FailureHandlingException(
                        f"Token {token.persistent_id} already visited"
                    )
                all_token_visited[token.persistent_id] = (token, is_available)

                port_row = await workflow.context.database.get_port_from_token(
                    token.persistent_id
                )

                port_name_id[port_row["name"]] = port_row["id"]

                # se ci sono più token con stesso tag, taglia un branch di ricerca
                if port_row["name"] in port_tokens.keys():
                    # if the token is not present then normal execution, otherwise check if there is available tokens
                    if token_present := get_token_by_tag(
                        token.tag,
                        (
                            all_token_visited[t_id][0]
                            for t_id in port_tokens[port_row["name"]]
                        ),
                    ):
                        aa = {
                            "uguaglianza": token_present.tag == token.tag,
                            "present": token_present,
                            "found": token,
                        }
                        pass
                        # if the token in port_tokens[port.name] is already available, skip current token
                        if all_token_visited[token_present.persistent_id][1]:
                            continue

                        # if both are not available ... cosa fare? in teoria sarebbe meglio seguire entrambi i path e tenere quello che arriva prima ad un token disponibile. Ma quanto è dispendioso? E sopratutto quanto difficile da scrivere?
                        # per il momento seguo il percorso solo del primo trovato (quindi token_present)
                        if (
                            not is_available
                            and not all_token_visited[token_present.persistent_id][1]
                        ):
                            continue

                        # if the token is available and token_present is not available
                        # than remove token_present. In following, token will be added
                        if is_available:
                            port_tokens[port_row["name"]].remove(
                                token_present.persistent_id
                            )
                            pass

                port_tokens.setdefault(port_row["name"], set()).add(token.persistent_id)
                if not is_available:
                    prev_tokens = await _load_prev_tokens(
                        token.persistent_id,
                        loading_context,
                        workflow.context,
                    )
                    if prev_tokens:
                        # todo: fare controllo port:tag già quindi? Per vedere se ci sono token duplicati di rollback precedenti
                        for pt in prev_tokens:
                            prev_port_row = (
                                await workflow.context.database.get_port_from_token(
                                    pt.persistent_id
                                )
                            )
                            port_name_id[prev_port_row["name"]] = prev_port_row["id"]
                            dag_ports.setdefault(prev_port_row["name"], set()).add(
                                port_row["name"]
                            )
                            dag_tokens.setdefault(pt.persistent_id, set()).add(
                                token.persistent_id
                            )
                            if (
                                pt.persistent_id not in all_token_visited.keys()
                                and not contains_token_id(pt.persistent_id, tokens)
                            ):
                                tokens.append(pt)
                    else:
                        dag_ports.setdefault(INIT_DAG_FLAG, set()).add(port_row["name"])
                        dag_tokens.setdefault(INIT_DAG_FLAG, set()).add(
                            token.persistent_id
                        )
                else:
                    dag_ports.setdefault(INIT_DAG_FLAG, set()).add(port_row["name"])
                    dag_tokens.setdefault(INIT_DAG_FLAG, set()).add(token.persistent_id)
                # alternativa ai due else ... però è più difficile la lettura del codice (ancora da provare)
                # if is_available or not prev_tokens:
                #     add_elem_dictionary(INIT_DAG_FLAG, token.persistent_id, dag_tokens)
            else:
                port_row = await workflow.context.database.get_port_from_token(
                    token.persistent_id
                )
                port_tokens.setdefault(port_row["name"], set()).add(token.persistent_id)

        jobs_execs = set()
        for token_id_list in port_tokens.values():
            for t_id in token_id_list:
                if isinstance(all_token_visited[t_id][0], JobToken):
                    jobs_execs.add(all_token_visited[t_id][0].value)
        print(
            "wf",
            new_workflow_name,
            " - Subito dopo la build, ho trovato jobs da rieseguire",
            [j.name for j in jobs_execs],
        )

        all_token_visited = dict(sorted(all_token_visited.items()))
        print(
            "available_new_job_tokens:",
            len(available_new_job_tokens) > 0,
            json.dumps(available_new_job_tokens, indent=2),
        )

        dt = str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
        dir_path = f"graphs/{dt}"
        os.makedirs(dir_path)

        label_t_av = lambda x: "A" if x else "NA"
        # DEBUG: create ports graph

        print_graph_figure(
            {
                (k, all_token_visited[k][0].tag, label_t_av(all_token_visited[k][1])): [
                    (
                        vv,
                        all_token_visited[vv][0].tag,
                        label_t_av(all_token_visited[vv][1]),
                    )
                    if isinstance(vv, int)
                    else (vv, "None", "None")
                    for vv in v
                ]
                for k, v in dag_tokens.items()
                if k != INIT_DAG_FLAG
            },
            dir_path + "/tokens-" + new_workflow_name,
        )
        print_graph_figure(
            {k: v for k, v in dag_ports.items() if k != INIT_DAG_FLAG},
            dir_path + "/ports-" + new_workflow_name,
        )
        await print_step_from_ports(
            dag_ports,
            port_name_id,
            list(port_tokens.keys()),
            workflow.context,
            failed_step.name,
            dir_path + "/steps-" + new_workflow_name,
        )

        test = {
            k for k in dag_ports.keys() if k != INIT_DAG_FLAG and k != failed_step.name
        }
        for values in dag_ports.values():
            for v in values:
                if v != INIT_DAG_FLAG and v != failed_step.name:
                    test.add(v)
        b1 = set(port_tokens.keys()) - set(test)
        b2 = set(test) - set(port_tokens.keys())
        if len(port_tokens.keys()) != len(test):
            raise FailureHandlingException(
                f"port_tokens.keys() != len(test) => {len(port_tokens.keys())} != {len(test)} ==> lost_ports: {len(lost_ports)}\nb1: {b1}\nb2: {b2}"
            )
        pass
        print("DEBUG: grafici creati")

        # DEBUG: controllo se ci sono branch che divergono (token di diverse esecuzioni che per via dei tag vengono ripetuti)
        token_mapping = {
            t_id: all_token_visited[t_id][0]
            for token_list in port_tokens.values()
            for t_id in token_list
        }
        for port_name, token_id_list in port_tokens.items():
            for token_id in token_id_list:
                for token_id_2 in token_id_list:
                    if (
                        token_id in token_mapping.keys()
                        and token_id_2 in token_mapping.keys()
                        and token_id != token_id_2
                        and all_token_visited[token_id][0].tag
                        == all_token_visited[token_id_2][0].tag
                    ):
                        if isinstance(
                            all_token_visited[token_id][0], JobToken
                        ) and isinstance(all_token_visited[token_id_2][0], JobToken):
                            print(
                                "DIVERGENZAAA ma sono due job token, quindi tutto regolare.",
                                all_token_visited[token_id][0].value.name,
                                "and",
                                all_token_visited[token_id_2][0].value.name,
                            )
                        else:
                            aa = all_token_visited[token_id][0]
                            ab = all_token_visited[token_id_2][0]
                            print(
                                "DIVERGENZAAA port",
                                port_name,
                                "type:",
                                type(all_token_visited[token_id][0]),
                                ", id:",
                                token_id,
                                ", tag:",
                                all_token_visited[token_id][0].tag,
                                ", value:",
                                all_token_visited[token_id][0].value.name
                                if isinstance(all_token_visited[token_id][0], JobToken)
                                else all_token_visited[token_id][0].value,
                            )
                            pass
        print("DEBUG: divergenza controllata")

        for v in available_new_job_tokens.values():
            if v["out-token-port-name"] not in port_tokens.keys():
                raise FailureHandlingException("Non c'è la porta. NON VA BENE")

        # todo: aggiustare nel caso in cui una port abbia più token di output
        empty_ports = _clean_port_tokens(
            port_tokens,
            [v["old-out-token"] for v in available_new_job_tokens.values()],
            dag_ports,
            [v["out-token-port-name"] for v in available_new_job_tokens.values()],
        )
        while empty_ports:
            empty_ports = _update_dag_ports(
                dag_ports, port_tokens, empty_ports, new_workflow_name
            )
        for v in available_new_job_tokens.values():
            a = v["out-token-port-name"]
            b = v["new-out-token"]
            # if not _check_port_is_attached(dag_ports, v["out-token-port-name"]):
            #     print_graph_figure(
            #         {k: v for k, v in dag_ports.items() if k != INIT_DAG_FLAG},
            #         dir_path + "/ports-post-remove-failure-exception",
            #     )
            #     print("dag", json.dumps(dag_ports, indent=2))
            #     raise FailureHandlingException(
            #         f"Port {v['out-token-port-name']} is no longer connected to anyone"
            #     )
            # print(
            #     f"Port_tokens {v['out-token-port-name']} ha {port_tokens[v['out-token-port-name']]} tokens"
            # )
            # port_tokens.setdefault(v["out-token-port-name"], set()).add(
            #     v["new-out-token"]
            # )
            # dag_ports[INIT_DAG_FLAG].add(v["out-token-port-name"])
            # print(
            #     f"Port_tokens {v['out-token-port-name']} aggiunto un nuovo elemento quindi ha {port_tokens[v['out-token-port-name']]} tokens"
            # )
            if v["out-token-port-name"] in port_tokens.keys():
                port_tokens[v["out-token-port-name"]].add(v["new-out-token"])
                dag_ports[INIT_DAG_FLAG].add(v["out-token-port-name"])
                print(
                    f"Port_tokens {v['out-token-port-name']} aggiunto un nuovo elemento quindi ha {port_tokens[v['out-token-port-name']]} tokens"
                )
            else:
                print(f"Port_tokens non ha più la port {v['out-token-port-name']}")

        print_graph_figure(
            {k: v for k, v in dag_ports.items() if k != INIT_DAG_FLAG},
            dir_path + "/ports-post-remove-" + new_workflow_name,
        )
        await print_step_from_ports(
            dag_ports,
            port_name_id,
            list(port_tokens.keys()),
            workflow.context,
            failed_step.name,
            dir_path + "/step-post-remove-" + new_workflow_name,
        )

        # rimozione di token che sono disponibili in modi alternativi (generati da altri rollback)
        # to_remove = [v["old-out-token"] for v in available_new_job_tokens.values()]
        # while to_remove:
        #     to_remove = self.clean_dag(
        #         dag_tokens,
        #         to_remove,
        #     )
        #     for k in to_remove:
        #         dag_tokens.pop(k)
        # for values in available_new_job_tokens.values():
        #     if values["old-out-token"] in dag_tokens:
        #         await self._update_dag(
        #             dag_tokens,
        #             values["old-out-token"],
        #             values["new-out-token"],
        #             all_token_visited,
        #             workflow,
        #             loading_context,
        #         )
        #     else:
        #         print("ECCO CHI FACEVA CADERE TUTTO", values["old-out-token"])

        # DEBUG: altra stampa debug per vedere grafo dopo la rimozione dei token superflui
        # await printa_token(
        #     all_token_visited,
        #     workflow,
        #     dag_tokens,
        #     loading_context,
        #     dir_path + "/steps-post-remove",
        # )

        token_mapping = {
            t_id: all_token_visited[t_id][0]
            for token_list in port_tokens.values()
            for t_id in token_list
        }
        return (
            dag_ports,
            port_tokens,
            {k: all_token_visited[k] for k in token_mapping.keys()},
        )

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow, loading_context = await self._recover_jobs(job, step)

            # get new job created by ScheduleStep
            command_output = await self._execute_failed_job(
                job, step, new_workflow, loading_context
            )
            # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except WorkflowTransferException as e:
            logger.exception(e)
            print("WorkflowTransferException ma stavo gestendo execute job")
            raise
        except Exception as e:
            logger.exception(e)
            return await self.handle_exception(job, step, e)
        return command_output

    # todo: situazione problematica
    #  A -> B
    #  A -> C
    #  B -> C
    # A ha successo, B fallisce (cade ambiente), viene rieseguito A, in C che input di A arriva?
    # quello vecchio? quello vecchio e quello nuovo? In teoria solo quello vecchio, da gestire comunque?
    # oppure lasciamo che fallisce e poi il failure manager prendere l'output nuovo di A?
    async def _recover_jobs(
        self, failed_job: Job, failed_step: Step, add_failed_step: bool = False
    ) -> CommandOutput:
        loading_context = DefaultDatabaseLoadingContext()

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

        tokens = list(failed_job.inputs.values())  # tokens to check
        tokens.append(job_token)
        dag_ports, port_tokens, token_visited = await self._build_dag(
            tokens,
            failed_job,
            failed_step,
            workflow,
            loading_context,
            new_workflow.name,
        )

        job_che_eseguo = set()  # debug variable
        # update class state (attributes)
        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                # update job request
                if token.value.name not in self.job_requests.keys():
                    self.job_requests[token.value.name] = JobRequest()
                else:
                    async with self.job_requests[token.value.name].lock:
                        # if self.job_requests[token.value.name].is_running:
                        #     raise Exception(
                        #         f"Job {token.value.name} già in esecuzione -> Ricostrire dag"
                        #     )
                        # else:
                        #     self.job_requests[token.value.name].is_running = True
                        pass
                async with self.job_requests[token.value.name].lock:
                    # save jobs recovered
                    if token.value.name not in self.jobs.keys():
                        self.jobs[token.value.name] = JobVersion(
                            version=1,
                        )

                    if (
                        self.max_retries is None
                        or self.jobs[token.value.name].version < self.max_retries
                    ):
                        job_che_eseguo.add(token.value.name)
                        self.jobs[token.value.name].version += 1
                        logger.debug(
                            f"Updated Job {token.value.name} at {self.jobs[token.value.name].version} times"
                        )
                    else:
                        logger.error(
                            f"FAILED Job {token.value.name} {self.jobs[token.value.name].version} times. Execution aborted"
                        )
                        raise FailureHandlingException()

                    # discard old job tokens
                    if self.job_requests[token.value.name].job_token:
                        print(
                            f"job {token.value.name}. Il mio job_token",
                            "esiste adesso però lo pongo a None"
                            if self.job_requests[token.value.name].job_token
                            else "NON esite. Lo lascio a None",
                        )
                    self.job_requests[token.value.name].job_token = None

        port_tokens_counter = await _populate_workflow(
            failed_step, token_visited, new_workflow, loading_context, port_tokens
        )

        if add_failed_step:
            if failed_step.name not in new_workflow.steps.keys():
                print(
                    f"Lo step fallito {failed_step.name} non è presente nel new_workflow {new_workflow.name} ... Lo aggiungo"
                )
                new_workflow.add_step(
                    await Step.load(
                        new_workflow.context,
                        failed_step.persistent_id,
                        loading_context,
                        new_workflow,
                    )
                )
            for port in failed_step.get_input_ports().values():
                if port.name not in new_workflow.ports.keys():
                    print(
                        f"La input port {port.name} dello step fallito {failed_step.name} non è presente nel new_workflow {new_workflow.name} ... La aggiungo"
                    )
                    new_workflow.add_port(
                        await Port.load(
                            new_workflow.context,
                            port.persistent_id,
                            loading_context,
                            new_workflow,
                        )
                    )

        pass
        self._save_for_retag(
            new_workflow, dag_ports, port_tokens, token_visited, failed_step.name
        )
        pass

        await _put_tokens(
            workflow,
            new_workflow,
            dag_ports[INIT_DAG_FLAG],
            port_tokens,
            token_visited,
            port_tokens_counter,
            dag_ports,
            failed_step.name,
        )

        # pass
        # self._save_for_retag(new_workflow, dag_ports, port_tokens, token_visited)
        # pass

        print("New workflow", new_workflow.name, "popolato così:")
        print("\tJobs da rieseguire:", job_che_eseguo)
        print(
            "\tJobs1",
            [
                token_visited[t_id][0].value.name
                for token_list in port_tokens.values()
                for t_id in token_list
                if isinstance(token_visited[t_id][0], JobToken)
            ],
        )
        print(
            "\tJobs2",
            {
                all_token_visited[t_id][0].value.name
                for token_list in dag_tokens.values()
                for t_id in token_list
                if isinstance(t_id, int)
                and isinstance(all_token_visited[t_id][0], JobToken)
            },
        )
        if "/tosort/0" in job_che_eseguo and "/tosort/1" in job_che_eseguo:
            print("\tRieseguo la scatter per intero")
            pass
        for step in new_workflow.steps.values():
            print(
                f"Step {step.name}\n\tinput ports",
                {
                    k_p: [(t.persistent_id, t.tag) for t in port.token_list]
                    for k_p, port in step.get_input_ports().items()
                },
                "\n\tkey-port_name",
                {k: v.name for k, v in step.get_input_ports().items()},
            )

        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                # free resources scheduler
                await workflow.context.scheduler.notify_status(
                    token.value.name, Status.WAITING
                )
        print("VIAAAAAAAAAAAAAA " + new_workflow.name)

        await new_workflow.save(workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        await executor.run()
        for p in new_workflow.ports.values():
            termtok = [
                "term:" + t.tag for t in p.token_list if isinstance(t, TerminationToken)
            ]
            s_list = [
                s.name
                for s in workflow.steps.values()
                if p.name in s.output_ports.values()
            ]
            if len(termtok) != 1 and "/tosort-scatter-combinator" not in s_list:
                mystr = ""
                for t in p.token_list:
                    mystr += f"id: {t.persistent_id}, tag: {t.tag}, value: {t}"
                print(
                    f"Workflow {new_workflow.name} - port {p.name} di cui è outport degli steps {s_list}\n\tTokens {mystr}\n\tCi sono {len(termtok)} TerminationTokens, invece di solo 1"
                )
                # raise FailureHandlingException(
                #     f"Workflow {new_workflow.name} port {p.name} - Ci sono troppi terminationtoken {len(termtok)} -> steps con outport {s_list}"
                # )
        return new_workflow, loading_context

    def is_valid_tag(self, workflow_name, tag, output_port):
        if workflow_name not in self.retags.keys():
            return True
        if output_port.name not in self.retags[workflow_name].keys():
            return True

        token_list = self.retags[workflow_name][output_port.name]
        for t in token_list:
            if t.tag == tag:
                temp_print_retag(
                    workflow_name, output_port, tag, self.retags, "return true"
                )
                return True
        temp_print_retag(workflow_name, output_port, tag, self.retags, "return false")
        return False

    def _save_for_retag(
        self, new_workflow, dag_ports, port_tokens, token_visited, failed_step_name
    ):
        # todo: aggiungere la possibilità di eserguire comunque tutti i job delle scatter aggiungendo un parametro nel StreamFlow file
        if new_workflow.name not in self.retags.keys():
            self.retags[new_workflow.name] = {}

        port_tags = get_port_tags(
            new_workflow, dag_ports, port_tokens, token_visited, failed_step_name
        )

        for step in new_workflow.steps.values():
            if isinstance(step, ScatterStep):
                port = step.get_output_port()

                print(
                    f"wf {new_workflow.name} -> port_tokens[{port.name}]:",
                    [
                        (t_id, token_visited[t_id][0].tag, token_visited[t_id][1])
                        for t_id in port_tokens[port.name]
                    ],
                )
                for t_id in port_tokens[port.name]:
                    if (
                        t_id not in dag_ports[INIT_DAG_FLAG]
                        and not token_visited[t_id][1]
                        and token_visited[t_id][0].tag in port_tags[port.name]
                    ):
                        self.retags[new_workflow.name].setdefault(port.name, set()).add(
                            token_visited[t_id][0]
                        )

                # for t_id in port_tokens[port.name]:
                # todo: valutare se salvare in retags solo i tag (e non tutto l'oggetto token)

                # # todo: si potrebbe cambiare: scorrere port_tokens e vedere le port che hanno più di un token (se hanno più token significa che hanno tag diversi)
                # if t_id not in dag_ports[INIT_DAG_FLAG]:
                #     if not token_visited[t_id][1]:
                #         self.retags[new_workflow.name].setdefault(port.name, set()).add(
                #             token_visited[t_id][0]
                #         )
                # else:
                #     print(
                #         f"problema scatter:out2 Non aggiungo il token {t_id} dentro retags perché è un token init del dag"
                #     )

    async def _execute_failed_job(
        self, failed_job, failed_step, new_workflow, loading_context
    ):
        try:
            new_job_token = get_job_token(
                failed_job.name,
                new_workflow.ports[
                    failed_step.get_input_port("__job__").name
                ].token_list,
            )
        except WorkflowExecutionException as err:
            raise FailureHandlingException(err)

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

        self.job_requests[failed_job.name].job_token = new_job_token
        new_job = new_job_token.value
        new_step = await Step.load(
            new_workflow.context,
            failed_step.persistent_id,
            loading_context,
            new_workflow,
        )
        new_workflow.add_step(new_step)
        await new_step.save(new_workflow.context)
        # new_step = new_workflow.steps[failed_step.name]

        cmd_out = await cast(ExecuteStep, new_step).command.execute(new_job)
        if cmd_out.status == Status.FAILED:
            jt = new_step.get_input_port("__job__").token_list
            logger.error(
                f"FAILED Job {new_job.name} with jobtoken.id {get_job_token(new_job.name, jt if isinstance(jt, Iterable) else [jt]).persistent_id} with error:\n\t{cmd_out.value}"
            )
            cmd_out = await self.handle_failure(new_job, new_step, cmd_out)
        print("Finito " + new_workflow.name)
        return cmd_out

    async def get_valid_job_token(self, job_token):
        request = (
            self.job_requests[job_token.value.name]
            if job_token.value.name in self.job_requests.keys()
            else None
        )
        if request:
            if request.job_token:
                return request.job_token
            else:
                request.job_token = job_token
        return job_token

    async def close(self):
        pass

    async def notify_jobs(self, job_name, out_port_name, token):
        print("Notify end job", job_name)
        if job_name in self.job_requests.keys():
            async with self.job_requests[job_name].lock:
                self.job_requests[job_name].token_output[out_port_name] = token
                self.job_requests[job_name].is_running = False
                self.job_requests[job_name].lock.notify_all()
                print("Notify end job", job_name, "- done")

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
        if job.name in self.job_requests.keys():
            self.job_requests[job.name].is_running = False
        return await self._do_handle_failure(job, step)

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Handling command failure for job {job.name}")

        if job.name in self.job_requests.keys():
            self.job_requests[job.name].is_running = False
        return await self._do_handle_failure(job, step)

    async def handle_failure_transfer(self, job: Job, step: Step):
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {WorkflowTransferException.__name__} failure for job {job.name}"
            )

        if job.name in self.job_requests.keys():
            self.job_requests[job.name].is_running = False

        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow, loading_context = await self._recover_jobs(
                job, step, add_failed_step=True
            )
            print(
                "transf-pre. new_workflow.new_step.token_list",
                len(
                    new_workflow.ports[
                        list(step.get_input_ports().values())[0].name
                    ].token_list
                ),
                "failed_Step.token_list",
                len(list(step.get_output_ports().values())[0].token_list),
            )
            # new_workflow.add_step(
            #         await Step.load(
            #             new_workflow.context,
            #             step.persistent_id,
            #             loading_context,
            #             new_workflow,
            #         )
            #     )
            status = await self._execute_transfer_step(step, new_workflow)
            print(
                "transf-post. new_workflow.new_step.token_list",
                len(
                    new_workflow.ports[
                        list(step.get_input_ports().values())[0].name
                    ].token_list
                ),
                "failed_Step.token_list",
                len(list(step.get_output_ports().values())[0].token_list),
            )
        # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except WorkflowTransferException as e:
            logger.exception(e)
            return await self._handle_failure_transfer(job, step)
        except Exception as e:
            logger.exception(e)
            return await self.handle_exception(job, step, e)
        return status

    async def _execute_transfer_step(self, failed_step, new_workflow):
        for out_port_tokens in [
            t.token_list
            for t in new_workflow.steps[failed_step.name].get_output_ports().values()
        ]:
            print(failed_step.name, "number of token generated", len(out_port_tokens))
            for out_token in out_port_tokens:
                if isinstance(out_token, TerminationToken):
                    print(failed_step.name, "out_token transfer Termination token")
                elif isinstance(out_token, CWLFileToken):
                    print(failed_step.name, "out_token transfer file", out_token.value)
                    data_loc = _get_data_location(
                        out_token.value["path"], new_workflow.context
                    )
                    print(
                        failed_step.name,
                        "out_token transfer esiste? ",
                        await remotepath.exists(
                            connector=new_workflow.context.deployment_manager.get_connector(
                                data_loc.deployment
                            ),
                            location=data_loc,
                            path=out_token.value["path"],
                        ),
                    )
                else:
                    print("out_token transfer type", type(out_token))
        for port in new_workflow.steps[failed_step.name].get_output_ports().values():
            for t in port.token_list:
                if isinstance(t, TerminationToken):
                    print(
                        failed_step.name,
                        "Ha già un termination token........Questo approccio non va bene",
                    )

        for k, port in new_workflow.steps[failed_step.name].get_output_ports().items():
            print(
                f"Port {port.name} svuoto token_list {len(failed_step.get_output_port(k).token_list)}"
            )
            # failed_step.get_output_port(k).token_list = [] # todo: fix temporaneo
            for t in port.token_list:
                print(
                    failed_step.name,
                    f"Inserisco token {t if not isinstance(t, TerminationToken) else 'TerminationToken'} in port {k}({port.name}), failed step -> len(port.token_list) {len(failed_step.get_output_port(k).token_list)}",
                )
                if not isinstance(t, TerminationToken):
                    failed_step.get_output_port(k).put(t)
        print(
            failed_step.name,
            "fs.old pt2",
            {k: t.token_list for k, t in failed_step.get_output_ports().items()},
        )
        return Status.COMPLETED


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

    async def get_valid_job_token(self, job_token):
        return job_token

    def is_valid_tag(self, workflow_name, tag, output_port):
        return True

    async def notify_jobs(self, job_name, out_port_name, token):
        pass

    async def handle_failure_transfer(self, job: Job, step: Step):
        return Status.FAILED
