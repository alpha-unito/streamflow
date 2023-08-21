from __future__ import annotations

import asyncio
import json
import logging
import os
import datetime
import time
from typing import MutableMapping, MutableSequence, cast, Tuple, Iterable

import pkg_resources

from streamflow.core.utils import random_name, contains_id, get_class_fullname
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
    temp_print_retag,
    print_debug_divergenza,
    print_grafici_parte_uno,
    print_grafici_post_remove,
    print_graph_figure,
    print_step_from_ports,
)
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import ScatterStep
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import ExecuteStep
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.token import TerminationToken, JobToken

from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow

from streamflow.workflow.utils import get_job_token


async def get_execute_step_out_token_ids(next_token_ids, context):
    execute_step_out_token_ids = set()
    for t_id in next_token_ids:
        port_row = await context.database.get_port_from_token(t_id)
        step_id_row = await context.database.get_step_from_output_port(port_row["id"])
        step_row = await context.database.get_step(step_id_row["step"])
        if step_row["type"] == get_class_fullname(ExecuteStep):
            execute_step_out_token_ids.add(t_id)
    return execute_step_out_token_ids


async def _cleanup_dir(
    connector: Connector, location: Location, directory: str
) -> None:
    await remotepath.rm(
        connector, location, await remotepath.listdir(connector, location, directory)
    )


# todo: spostarlo in utils o loading_context. Utilizzare questo metodo in test_provenance invece di ri-definirlo uguale
async def _load_prev_tokens(token_id, loading_context, context):
    rows = await context.database.get_dependees(token_id)

    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["dependee"]))
            for row in rows
        )
    )


# todo: se lascio sia get_prev_tokens che get_prev_ports ... fare un metodo con un nome generico e usarlo per entrambi
def get_prev_tokens(searched_t_id, dag_tokens):
    token_ids = set()
    for t_id, next_t_ids in dag_tokens.items():
        if searched_t_id in next_t_ids and t_id != INIT_DAG_FLAG:
            token_ids.add(t_id)
    return token_ids


def get_prev_ports(searched_port_name, dag_ports):
    start_port_names = set()
    for port_name, next_port_names in dag_ports.items():
        if searched_port_name in next_port_names and port_name != INIT_DAG_FLAG:
            start_port_names.add(port_name)
    return start_port_names


def get_port_tags(new_workflow, dag_ports, port_tokens, token_visited):
    port_tags = {}
    for port_name in dag_ports[INIT_DAG_FLAG]:
        intersection_tags = {
            token_visited[t_id][0].tag for t_id in port_tokens[port_name]
        }
        if not isinstance(new_workflow.ports[port_name], (ConnectorPort, JobPort)):
            for next_port_name in dag_ports[port_name]:
                #  port_name    same_level_port_name(1)  same_level_port_name(2) ...
                #       \                   |               /
                #                   next_port_name
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
    new_workflow: Workflow,
    init_ports: MutableSequence[str],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
    dag_ports: MutableMapping[str, MutableSequence[str]],
):
    # port_tags = get_port_tags(new_workflow, dag_ports, port_tokens, token_visited)
    for port_name in init_ports:
        try:
            port = new_workflow.ports[port_name]
        except:
            print(
                f"Stavo costruendo new_workflow {new_workflow.name}. Ho nelle init_port {port_name} ma non è caricato nel workflow.",
                f"\n\tSteps: {new_workflow.steps.keys()} \n\tPorts: {new_workflow.ports.keys()}",
            )
            raise
        token_list = [
            token_visited[t_id][0]
            for t_id in port_tokens[port_name]
            if isinstance(t_id, int) and token_visited[t_id][1]
            # and token_visited[t_id][0].tag in port_tags[port_name]
        ]
        token_list.sort(key=lambda x: x.tag, reverse=False)
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
        if len(port.token_list) > 0 and len(port.token_list) == len(
            port_tokens[port_name]
        ):
            print(
                f"Port {port.name} with {len(port.token_list)} tokens. It will be added TerminationToken"
            )
            port.put(TerminationToken())
        else:
            print(
                f"Port {port.name} with {len(port.token_list)} tokens. NOOOOOOOOOOOOOOOOOO TerminationToken"
            )
        pass


def cut_off_graph(token, is_available, port_row, port_tokens, all_token_visited):
    if port_row["name"] in port_tokens.keys():
        if isinstance(token, JobToken):
            for t_id in port_tokens[port_row["name"]]:
                if all_token_visited[t_id][0].value.name == token.value.name:
                    # tengo il jobtoken con id più grande. è nuovo quindi forse dati più nuovi e sperabilmente disponibili
                    if all_token_visited[t_id][0].persistent_id < token.persistent_id:
                        port_tokens[port_row["name"]].remove(t_id)
                        print(
                            f"Trovati due JobToken {token.value.name}. Vecchio jobtoken: {t_id}, nuovo: {token.persistent_id}. Taglio branch di {t_id} e continuo su {token.persistent_id}"
                        )
                        return False
                    else:
                        print(
                            f"Trovati due JobToken {token.value.name}. Vecchio jobtoken: {t_id}, nuovo: {token.persistent_id}. Butto {token.persistent_id} lascio branch di {t_id}"
                        )
                        return True
        else:
            # if the token is not present then normal execution, otherwise check if there is available tokens
            if token_present := get_token_by_tag(
                token.tag,
                (all_token_visited[t_id][0] for t_id in port_tokens[port_row["name"]]),
            ):
                # todo: possibile ottimizzazione. vedere l'id e prendere sempre quello più grande.
                # if the token in port_tokens[port.name] is already available, skip current token
                # if all_token_visited[token_present.persistent_id][1]:
                #     return True
                #
                # # if both are not available ... cosa fare? in teoria sarebbe meglio seguire entrambi i path e tenere quello che arriva prima ad un token disponibile. Ma quanto è dispendioso? E sopratutto quanto difficile da scrivere?
                # # per il momento seguo il percorso solo del primo trovato (quindi token_present)
                # if (
                #     not is_available
                #     and not all_token_visited[token_present.persistent_id][1]
                # ):
                #     return True
                #
                # # if the token is available and token_present is not available
                # # than remove token_present. In following, token will be added
                # if is_available:
                #     port_tokens[port_row["name"]].remove(token_present.persistent_id)

                # token incontrato è disponibile, scarto quello attuale
                if all_token_visited[token_present.persistent_id][1]:
                    return True

                # entrambi non disponibili, tengo quello più nuovo
                if not is_available:
                    if token_present.persistent_id > token.persistent_id:
                        return True

                # eseguo questo se:
                #   - o il token_present non è disponibile e token è disponibile
                #   - oppure nessuno dei due è disponibile ma token è più nuovo, quindi forse ha token precedenti disponibili
                port_tokens[port_row["name"]].remove(token_present.persistent_id)
    return False


async def _populate_workflow(
    failed_step, token_visited, new_workflow, loading_context, port_tokens
):
    # { id : all_tokens_are_available }
    ports = {}

    # {port.name : n.of tokens}
    # todo: questa variabile non serve più
    port_tokens_counter = {}

    for token_id, (_, is_available) in token_visited.items():
        row = await failed_step.workflow.context.database.get_port_from_token(token_id)
        if TOKEN_WAITER not in port_tokens[row["name"]]:
            if row["id"] in ports.keys():
                ports[row["id"]] = ports[row["id"]] and is_available
            else:
                ports[row["id"]] = is_available

        # save the port name and tokens number in the DAG it produces
        if row["name"] not in port_tokens_counter.keys():
            port_tokens_counter[row["name"]] = 0
        port_tokens_counter[row["name"]] += 1

    # add port into new_workflow
    print(
        f"new workflow {new_workflow.name} ports init situation {new_workflow.ports.keys()}"
    )
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
    steps = set()
    for row_dependency in await asyncio.gather(
        *(
            new_workflow.context.database.get_step_from_output_port(port_id)
            for port_id, is_available in ports.items()
            if not is_available
        )
    ):
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


# todo: move it in utils
def get_token_by_tag(token_tag, token_list):
    for token in token_list:
        if token_tag == token.tag:
            return token
    return None


async def _is_token_available(token, context):
    return not isinstance(token, JobToken) and await token.is_available(context)


# todo: move it in utils. Generalizzarlo in persistable_entity_list e usarlo in test_provenance
# def contains_token_id(token_id, token_list):
#     return token_id in (t.persistent_id for t in token_list)


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


def get_necessary_tokens(
    port_tokens, all_token_visited
) -> MutableMapping[int, Tuple[Token, bool]]:
    return {
        t_id: all_token_visited[t_id]
        for token_list in port_tokens.values()
        for t_id in token_list
        if t_id != TOKEN_WAITER
    }


def reduce_graph(dag_ports, port_tokens, available_new_job_tokens, new_workflow_name):
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

    # update available token in the port_tokens and adding port as init port in dag_ports
    for v in available_new_job_tokens.values():
        if v["out-token-port-name"] in port_tokens.keys():
            port_tokens[v["out-token-port-name"]].add(v["new-out-token"])
            dag_ports[INIT_DAG_FLAG].add(v["out-token-port-name"])
            print(
                f"Port_tokens {v['out-token-port-name']} aggiunto un nuovo elemento quindi ha {port_tokens[v['out-token-port-name']]} tokens"
            )
        else:
            print(
                f"Port_tokens non ha più la port {v['out-token-port-name']} ... significa che il new token non serve più per questo rollback"
            )


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


INIT_DAG_FLAG = "init"
TOKEN_WAITER = "twaiter"


class PortRecovery:
    def __init__(self, port):
        self.port = port
        self.waiting_token = 1


class JobRequest:
    def __init__(self):
        self.job_token: JobToken = None
        self.token_output: MutableMapping[str, Token] = {}
        self.lock = asyncio.Condition()
        self.is_running = True
        self.queue: MutableSequence[PortRecovery] = []
        self.workflow = None
        # todo: togliere direttamente le istanze delle port e usare una lista di port.id.
        #  Quando necessario le port si recuperano dal DB. Invece usare direttamente le port (ovvero l'attuale
        #  implementazione) grava in memoria perché ci sono tanti oggetti nella classe DefaultFailureManager
        #  Stessa cosa con i token_output. togliere l'istanza del token e mettere l'id


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
    ):
        if isinstance(token, JobToken) and token.value.name in self.job_requests.keys():
            async with self.job_requests[token.value.name].lock:
                job_request = self.job_requests[token.value.name]

                # todo: togliere parametro is_running, usare lo status dello step
                # todo: questo controllo forse meglio toglierlo da qui. Dal momento tra cui li individeo is_running e il momento in cui li attacco al wofklow (sync_rollbacks) i job potrebbero terminare e non avrei i token in input. Quindi due soluzioni
                #   quella facile costruire tutto il grafo ignorando quelli running in questo momento. Poi sync_rollbacks aggiusta tutto
                #   fare un meccanismo (probabilmente in sync_rollback) che si ricorda di controllare se i job nel frattempo hanno finito
                if job_request.is_running:
                    print(
                        f"Il job {token.value.name} {token.persistent_id} - è running, dovrei aspettare il suo output"
                    )

                    running_new_job_tokens[token.persistent_id] = {
                        "job_request": job_request
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
                    #   update: FORSE NON SERVE FARLO
                    out_tokens_json = (
                        await context.database.get_out_tokens_from_job_token(
                            token.persistent_id
                        )
                    )
                    # out_tokens_json dovrebbe essere una lista...caso in cui un job produce più token
                    print(
                        f"Il job {token.value.name} ({token.persistent_id}) ha il out_token_json",
                        dict(out_tokens_json) if out_tokens_json else "null",
                    )
                    if out_tokens_json:
                        print(
                            "out_tokens_json['id']:",
                            out_tokens_json["id"],
                            "- job_request.token_output:",
                            job_request.token_output,
                        )
                        port_json = await context.database.get_port_by_token(
                            out_tokens_json["id"]
                        )
                        print(
                            f"Il job {token.value.name} ({token.persistent_id}) ha il port_json",
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
                        else:
                            self.job_requests[token.value.name].job_token = None
                            # todo: come gestire i vari kport? Se un token non è valido mettere a None solo la kport di quel token o tutta?
                            #  la cosa migliore sarebbe solo di quella persa
                            self.job_requests[token.value.name].token_output = None
                            self.job_requests[token.value.name].workflow = None

        return False

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

        # DEBUG
        dag_tokens = {}  # {t.id : set(next_t.id)
        port_name_id = {}  # {port_name: port_id}

        # { port_name : set(token_ids) }
        port_tokens = {}

        # { token_id: (token, is_available)}
        all_token_visited = {}

        # {old_job_token_id : job_request_running}
        running_new_job_tokens = {}

        # {old_job_token_id : (new_job_token_id, new_output_token_id)}
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

        while tokens:
            token = tokens.pop()

            if not await self._has_token_already_been_recovered(
                token,
                all_token_visited,
                available_new_job_tokens,
                running_new_job_tokens,
                workflow.context,
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
                    # todo: controllare se ancora necessario
                    print(
                        f"Token {token} già aggiunto dalla funzione _has_token_already_been_recovered"
                    )
                    raise FailureHandlingException("OOOOOOOOOOOOOOOOOOOOO")
                    # continue # commentato il continue e sostituito con l'errore per capire se questa situazione accede ancora o meno
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
                if cut_off_graph(
                    token, is_available, port_row, port_tokens, all_token_visited
                ):
                    print(
                        f"CUTOFF token {token.persistent_id} della port {port_row['name']}"
                    )
                    continue

                port_tokens.setdefault(port_row["name"], set()).add(token.persistent_id)
                if not is_available:
                    prev_tokens = await _load_prev_tokens(
                        token.persistent_id,
                        loading_context,
                        workflow.context,
                    )
                    if prev_tokens:
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
                                and not contains_id(pt.persistent_id, tokens)
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

        all_token_visited = dict(sorted(all_token_visited.items()))
        print(
            "available_new_job_tokens:",
            len(available_new_job_tokens) > 0,
            json.dumps(available_new_job_tokens, indent=2),
        )

        dt = str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
        dir_path = f"graphs/{dt}"
        os.makedirs(dir_path)

        # DEBUG: create port-step-token graphs
        await print_grafici_parte_uno(
            all_token_visited,
            dag_tokens,
            dag_ports,
            dir_path,
            new_workflow_name,
            port_tokens,
            port_name_id,
            workflow,
            failed_step,
        )
        # DEBUG: controllo se ci sono branch che divergono (token di diverse esecuzioni che per via dei tag vengono ripetuti)
        print_debug_divergenza(all_token_visited, port_tokens)

        reduce_graph(
            dag_ports, port_tokens, available_new_job_tokens, new_workflow_name
        )

        # DEBUG: create port-step-token graphs post remove
        await print_grafici_post_remove(
            dag_ports,
            dir_path,
            new_workflow_name,
            port_tokens,
            port_name_id,
            workflow,
            failed_step,
        )

        return (
            dag_ports,
            dag_tokens,
            port_tokens,
            get_necessary_tokens(port_tokens, all_token_visited),
            dir_path,
            port_name_id,
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

    async def sync_rollbacks(
        self,
        dag_ports,
        dag_tokens,
        port_tokens,
        token_visited,
        new_workflow,
        loading_context,
        dir_path,
        port_name_id,
        workflow,
        failed_step,
    ):
        # per sincronizzare usare la var is_running
        # dal grafo ricavo quali job devono essere eseguiti:
        # - Se non esiste la request o non sono running, pongo la var a true
        #   - se request esiste, pone job_token e out_token a None
        # - altrimenti (è running quindi) applico politica descritta sotto
        # Quando il job termina la sua esecuzione:
        # - salva nel modulo fault il suo job_token (che ha terminato con successo)
        #   - chiamando il modulo fault controlliamo se il job usato sia quello originale oppure uno eseguito da rollback.
        #     Se ci troviamo nel seconod caso, per il recupero dei dati viene usato il job del rollback
        # - Recupera i dati da remoto (usando il job dentro job_token)
        # Dopo aver recuperato i dati e generato l'out_token, chiama il metodo notify jobs che:
        # - salva out_token nel modulo fault
        # - pone is_running del job a False

        # Se job è già running
        # aggiustare il grafo togliendo gli step non più necessari
        # COME ATTACCARE I DUE WORKFLOW?
        # invece della queue: Aspettare che i token siano pronti, aggiustare il grafo e infine eseguire il workflow
        # fare un publisher-subscribers:
        #   - job in esecuzione dentro wf1
        #   - a wf2 serve l'output (t1) di job
        #   - wf2 costruisce il suo grafo, della parte interessata di job mette solo la port dove viene prodotto t1
        #   - la port è vuota, ma viene iscritta ad una coda presente nella request del job
        #   - quando job in wf1 avrà terminato, salva t1 in request.out_token
        #   - il modulo di fault si occuperà di prendere t1 e metterlo dentro le porte iscritte alla coda
        #   - in questo modo wf2 potrà eseguire nel frattempo gli altri step e si fermarà solo quando sarà davvero necessario t1 prodotto da job
        #   - dopo che il modulo di fault manda t1, disiscrive le port dalla coda

        job_executed_in_new_workflow = set()  # debug variable
        job_token_list = []
        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                # update job request
                if token.value.name not in self.job_requests.keys():
                    self.job_requests[token.value.name] = JobRequest()
                    self.job_requests[token.value.name].workflow = new_workflow.name
                    job_token_list.append(token)
                else:
                    async with self.job_requests[token.value.name].lock:
                        if self.job_requests[token.value.name].is_running:
                            print(
                                f"Job {token.value.name} già in esecuzione nel workflow {self.job_requests[token.value.name].workflow} -> Ricostruire dag"
                            )

                            # Idea: Voglio lasciare in dag_ports solo la port che dà in output il token generato dal job
                            # e rimuovere tutte le port precedenti che non sono connesse ad altre port del grafo
                            # In realtà non è detto che io voglio davvero rimuovere la port, ma solo i token che passano da questa port
                            # coinvolti a generare il token mancante del job (e.g. se sono port di una scatter di alcuni job).
                            # Se rimossi questi token, le port sono vuote (port_tokens) oppure hanno solo token available, allora è possibile rimuovere le porte da dag_ports

                            # alternativa: dal dag_ports prendo tutte le port successive al job_token,
                            # con il database ricavo quali di queste porte sono output di uno ExecuteStep
                            job_token_port_row = (
                                await new_workflow.context.database.get_port_from_token(
                                    token.persistent_id
                                )
                            )  # outport ScheduleStep
                            job_token_port_name = job_token_port_row["name"]
                            next_port_names = dag_ports[
                                job_token_port_name
                            ]  # outport ExecuteStep and TransferStep

                            rows = await new_workflow.context.database.get_executestep_outports_from_jobtoken(
                                token.persistent_id
                            )
                            execute_step_outports = await asyncio.gather(
                                *(
                                    Port.load(
                                        new_workflow.context,
                                        row["id"],
                                        loading_context,
                                        new_workflow,
                                    )
                                    for row in rows
                                    if row["name"] not in new_workflow.ports.keys()
                                )
                            )
                            # controllo per debug
                            for p in execute_step_outports:
                                if p.name not in next_port_names:
                                    raise FailureHandlingException(
                                        f"wf {new_workflow.name} Port {p.name} non presente tra quelle presente nel dag_ports {next_port_names}"
                                    )
                                print(
                                    f"wf {new_workflow.name} Porta {p.name} trovata tra le next con il secondo metodo {next_port_names}"
                                )

                            # if the port can have more tokens
                            for row in rows:
                                if row["name"] in new_workflow.ports.keys():
                                    for pr in self.job_requests[token.value.name].queue:
                                        if pr.port.name == row["name"]:
                                            pr.waiting_token += 1
                                            print(
                                                f"new_workflow {new_workflow.name} already has port {port.name}. Increased waiting_tokens: {pr.waiting_token}"
                                            )

                            # add port in the workflow and create port recovery (request)
                            for port in execute_step_outports:
                                new_workflow.add_port(port)
                                print(
                                    f"new_workflow {new_workflow.name} added port {port.name} -> {new_workflow.ports.keys()}"
                                )
                                self.job_requests[token.value.name].queue.append(
                                    PortRecovery(port)
                                )

                            # # rimuovo i token generati da questo job token
                            # # (non posso levare tutti i token della port perché non so quali
                            # # job li genera. Solo con dag_tokens posso saperlo)
                            # # todo: invece di dag_tokens interrogare il db
                            # new_init_ports = set()
                            # # prendo token successivi al job_token
                            # for t_id in dag_tokens[token.persistent_id]:
                            #     found = False
                            #     # prendo le porte successive al job_token e rimuovo i token
                            #     for p in execute_step_outports:
                            #         if p.name not in port_tokens.keys():
                            #             print(f"port {p.name} non presente tra le port_tokens")
                            #         elif (
                            #             p.name in port_tokens.keys()
                            #             and t_id in port_tokens[p.name]
                            #         ):
                            #             port_tokens[p.name].remove(t_id)
                            #             # port_tokens[p.name].add(TOKEN_WAITER)
                            #             print("Added token waiter in the port", p.name)
                            #             new_init_ports.add(p.name)
                            #             found = True
                            #     if not found:
                            #         print(
                            #             f"Token {token.persistent_id} trovato next token {t_id} ma non corrisponde a nessuna port.",
                            #             "Token presente in token_visited?",
                            #             t_id in token_visited.keys(),
                            #             ""
                            #             if t_id not in token_visited.keys()
                            #             else f"Il token {token_visited[t_id][0]} è available? {token_visited[t_id][1]}",
                            #         )
                            # empty_ports = set()
                            # for nip in new_init_ports:
                            #     for prev in get_prev_ports(nip, dag_ports):
                            #         for p in dag_ports.pop(prev):
                            #             empty_ports.add(p)

                            # new_init_ports = set()
                            # empty_ports = set()
                            # for p in execute_step_outports:
                            #     # todo: ancora da considerare i token, ovvero, non devo togliere la port se questa serve ad altri token.
                            #     #  rimuovo i token generati da questo job token
                            #     #  (non posso levare tutti i token della port perché non so quali
                            #     #  job li genera. Solo con dag_tokens posso saperlo). Invece di dag_tokens usare db
                            #     for prev_p in get_prev_ports(p.name, dag_ports):
                            #         empty_ports.add(prev_p)
                            #         dag_ports.pop(prev_p)
                            #     new_init_ports.add(p.name)

                            token_to_remove = set()
                            execute_step_out_token_ids = (
                                await get_execute_step_out_token_ids(
                                    dag_tokens[token.persistent_id],
                                    new_workflow.context,
                                )
                            )
                            pass
                            for t_id in execute_step_out_token_ids:
                                prev_token_ids = get_prev_tokens(t_id, dag_tokens)
                                pass
                                for prev_t_id in prev_token_ids:
                                    token_to_remove.add(prev_t_id)

                            empty_ports = _clean_port_tokens(
                                port_tokens,
                                token_to_remove,
                                dag_ports,
                                {p.name for p in execute_step_outports},
                            )

                            while empty_ports:
                                print(
                                    f"{token.value.name} poooop ports {empty_ports}",
                                    f"del jobtoken {token.persistent_id} {token.value.name} wf {new_workflow.name}",
                                )
                                empty_ports = _update_dag_ports(
                                    dag_ports,
                                    port_tokens,
                                    empty_ports,
                                    new_workflow.name,
                                )

                            for p in execute_step_outports:
                                dag_ports[INIT_DAG_FLAG].add(p.name)
                                port_tokens.setdefault(p.name, set()).add(TOKEN_WAITER)

                            # for nip in new_init_ports:
                            #     dag_ports[INIT_DAG_FLAG].add(nip)
                            #     port_tokens.setdefault(nip, set()).add(TOKEN_WAITER)
                            print(
                                f"SYNCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC job {token.value.name}",
                                f"in esecuzione in wf {self.job_requests[token.value.name].workflow}",
                                f"servirà al wf {new_workflow.name}",
                            )
                            print_graph_figure(
                                {
                                    k: v
                                    for k, v in dag_ports.items()
                                    if k != INIT_DAG_FLAG
                                },
                                dir_path + "/ports-sync-" + new_workflow.name,
                            )
                            await print_step_from_ports(
                                dag_ports,
                                port_name_id,
                                list(port_tokens.keys()),
                                workflow.context,
                                failed_step.name,
                                dir_path + "/steps-sync-" + new_workflow.name,
                            )
                            pass
                        else:
                            print(
                                f"Job {token.value.name} posto running, mentre job_token e token_output posti a None. (Valore corrente jt: {self.job_requests[token.value.name].job_token} - t: {self.job_requests[token.value.name].token_output})"
                            )
                            job_token_list.append(token)
                            self.job_requests[token.value.name].is_running = True
                            self.job_requests[token.value.name].job_token = None
                            self.job_requests[token.value.name].token_output = None
                            self.job_requests[
                                token.value.name
                            ].workflow = new_workflow.name

        for token in job_token_list:
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
                    job_executed_in_new_workflow.add(token.value.name)
                    self.jobs[token.value.name].version += 1
                    logger.debug(
                        f"Updated Job {token.value.name} at {self.jobs[token.value.name].version} times"
                    )
                else:
                    logger.error(
                        f"FAILED Job {token.value.name} {self.jobs[token.value.name].version} times. Execution aborted"
                    )
                    raise FailureHandlingException()
        aa = get_necessary_tokens(port_tokens, token_visited)
        # for t in set(token_visited.keys()) - set(aa.keys()):
        #     print(
        #         "poooop token",
        #         t,
        #         f"del jobtoken {token.persistent_id} {token.value.name} wf {new_workflow.name}",
        #     )
        #     token_visited.pop(t)
        # pass
        nie = get_necessary_tokens(port_tokens, token_visited)
        kies = list(token_visited.keys())
        for k in kies:
            token_visited.pop(k)
        for k, v in nie.items():
            token_visited[k] = v

        return job_executed_in_new_workflow

    # todo: situazione problematica
    #  A -> B
    #  A -> C
    #  B -> C
    # A ha successo, B fallisce (cade ambiente), viene rieseguito A, in C che input di A arriva?
    # quello vecchio? quello vecchio e quello nuovo? In teoria solo quello vecchio, da gestire comunque?
    # oppure lasciamo che fallisce e poi il failure manager prende l'output nuovo di A?
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
        (
            dag_ports,
            dag_tokens,
            port_tokens,
            token_visited,
            dir_path,
            port_name_id,
        ) = await self._build_dag(
            tokens,
            failed_job,
            failed_step,
            workflow,
            loading_context,
            new_workflow.name,
        )

        # update class state (attributes) and jobs syncronization
        job_executed_in_new_workflow = await self.sync_rollbacks(
            dag_ports,
            dag_tokens,
            port_tokens,
            token_visited,
            new_workflow,
            loading_context,
            dir_path,
            port_name_id,
            workflow,
            failed_step,
        )

        # port_tokens_counter non serve più. solo per debug
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

        self._save_for_retag(
            new_workflow, dag_ports, port_tokens, token_visited, failed_step.name
        )

        await _put_tokens(
            new_workflow,
            dag_ports[INIT_DAG_FLAG],
            port_tokens,
            token_visited,
            dag_ports,
        )

        # pass
        # self._save_for_retag(new_workflow, dag_ports, port_tokens, token_visited)
        # pass

        print("New workflow", new_workflow.name, "popolato così:")
        print("\tJobs da rieseguire:", job_executed_in_new_workflow)

        for step in new_workflow.steps.values():
            try:
                print(
                    f"Step {step.name}\n\tinput ports",
                    {
                        k_p: [(t.persistent_id, t.tag) for t in port.token_list]
                        for k_p, port in step.get_input_ports().items()
                    },
                    "\n\tkey-port_name",
                    {k: v.name for k, v in step.get_input_ports().items()},
                )
            except Exception as e:
                # debug: a volte non trova la porta. Credo capiti perché ancora manca la sincronizzazione dei rollback
                # e quando nella scatter ci siano più location, succedono casini nel metodo _has_token_already_been_recovered
                print(
                    f"Step {step.name} error. new workflow {new_workflow.name}\n\tsteps",
                    new_workflow.steps.keys(),
                    "\n\tports",
                    new_workflow.ports.keys(),
                )
                raise e

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

        for step in new_workflow.steps.values():
            if isinstance(step, ScatterStep):
                port = step.get_output_port()

                print(
                    f"_save_for_retag wf {new_workflow.name} -> port_tokens[{port.name}]:",
                    [
                        (t_id, token_visited[t_id][0].tag, token_visited[t_id][1])
                        for t_id in port_tokens[port.name]
                    ],
                )
                for t_id in port_tokens[port.name]:
                    if (
                        t_id not in dag_ports[INIT_DAG_FLAG]
                        and not token_visited[t_id][1]
                    ):
                        # todo: salvare solo il tag. Cambiamento che dovrà essere applicanto anche in is_valid_tag
                        self.retags[new_workflow.name].setdefault(port.name, set()).add(
                            token_visited[t_id][0]
                        )

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
            if original_port is None:
                print(
                    f"Original_port is None. It is the input port {step_port_name} in the job {failed_job.name} of the step {failed_step.name}"
                )
            if original_port.name in new_workflow.ports.keys():
                new_inputs[step_port_name] = get_token_by_tag(
                    token.tag, new_workflow.ports[original_port.name].token_list
                )
            else:
                new_inputs[step_port_name] = get_token_by_tag(
                    token.tag, original_port.token_list
                )
        new_job_token.value.inputs = new_inputs

        async with self.job_requests[failed_job.name].lock:
            if self.job_requests[failed_job.name].job_token is not None:
                # succede ma non dovrebbe, sarà perché manca ancora la sincronizzazione
                print(
                    f"WARN WARN WARN job {failed_job.name} ha già un job_token {self.job_requests[failed_job.name].job_token.persistent_id}. Però qui non dovrebbe averne. Io volevo aggiungere job_token {new_job_token.persistent_id}."
                )
                pass
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

        await new_workflow.context.scheduler.notify_status(new_job.name, Status.RUNNING)
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
        if job_token.value.name in self.job_requests.keys():
            async with self.job_requests[job_token.value.name].lock:
                request = self.job_requests[job_token.value.name]
                # if it is not present, save it
                if not request.job_token:
                    request.job_token = job_token
                # return the valid job_token passed as parameter or that used in a rollback
                return request.job_token
        return job_token

    async def close(self):
        pass

    async def notify_jobs(self, job_name, out_port_name, token):
        print("Notify end job", job_name)
        if job_name in self.job_requests.keys():
            async with self.job_requests[job_name].lock:
                if self.job_requests[job_name].token_output is None:
                    self.job_requests[job_name].token_output = {}
                self.job_requests[job_name].token_output.setdefault(
                    out_port_name, token
                )
                self.job_requests[job_name].is_running = False
                # todo: fare a tutte le port nella queue la put del token
                elems = []
                try:
                    print(
                        f"job {job_name} sta notificando sulla port_name {out_port_name}. Ci sono in coda",
                        len(self.job_requests[job_name].queue),
                        "ports:",
                        "".join(
                            [
                                f"\n\tHa trovato port_name {elem.port.name} port_id {elem.port.persistent_id} workflow {elem.port.workflow.name} token_list {elem.port.token_list} queues {elem.port.queues}. Waiting per {elem.waiting_token} prima del terminationtoken"
                                if elem
                                else "\n\t\tElem-None"
                                for elem in self.job_requests[job_name].queue
                            ]
                        ),
                    )
                except:
                    print(
                        f"job {job_name} sta notificando sulla port_name {out_port_name}. Ci sono in coda",
                        len(self.job_requests[job_name].queue),
                        "ports. Qualcosa si è rotto nella print. Questa è una print raffazzonata",
                    )
                    raise
                for elem in self.job_requests[job_name].queue:
                    if elem.port.name == out_port_name:
                        elem.port.put(token)
                        # todo: non è giusto, potrebbe dover aspettare altri token
                        elem.waiting_token -= 1
                        if elem.waiting_token == 0:
                            elems.append(elem)
                            elem.port.put(TerminationToken())
                        print(
                            f"Port {elem.port.name} id {elem.port.persistent_id} wf {elem.port.workflow.name} ha ricevuto token {token.persistent_id}",
                            f"token_list: {elem.port.token_list} queues: {elem.port.queues}",
                            f"Aspetta {elem.waiting_token} tokens prima di mettere il terminationtoken"
                            if elem.waiting_token
                            else "Mandato anche termination token",
                        )

                for elem in elems:
                    self.job_requests[job_name].queue.remove(elem)
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

    async def handle_failure_transfer(self, job: Job, step: Step, port_name: str):
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
            status = await self._execute_transfer_step(step, new_workflow, port_name)
        # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except WorkflowTransferException as e:
            logger.exception(e)
            return await self._handle_failure_transfer(job, step, port_name)
        except Exception as e:
            logger.exception(e)
            return await self.handle_exception(job, step, e)
        return status

    async def _execute_transfer_step(self, failed_step, new_workflow, port_name):
        token_list = (
            new_workflow.steps[failed_step.name].get_output_port(port_name).token_list
        )
        if len(token_list) != 2:
            raise FailureHandlingException(
                f"Step recovery {failed_step.name} did not generate the right number of tokens: {len(token_list)}"
            )
        if not isinstance(token_list[1], TerminationToken):
            raise FailureHandlingException(
                f"Step recovery {failed_step.name} did not work well. It moved two tokens instead of one: {[t.persistent_id for t in token_list]}"
            )
        return token_list[0]


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

    async def handle_failure_transfer(self, job: Job, step: Step, port_name: str):
        return None
