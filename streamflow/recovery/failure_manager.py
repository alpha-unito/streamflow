from __future__ import annotations

import os
import json
import asyncio
import logging
from collections import deque
from typing import MutableMapping, MutableSequence

import pkg_resources

from streamflow.core.utils import random_name, get_class_from_name
from streamflow.core.workflow import Workflow
from streamflow.core.context import StreamFlowContext
from streamflow.core.recovery import FailureManager
from streamflow.core.workflow import CommandOutput, Job, Status, Step, Port, Token
from streamflow.core.exception import (
    FailureHandlingException,
    WorkflowTransferException,
    WorkflowExecutionException,
)

from streamflow.log_handler import logger
from streamflow.recovery.utils import (
    INIT_DAG_FLAG,
    TOKEN_WAITER,
    get_execute_step_out_token_ids,
    get_last_token,
    get_key_by_value,
    _execute_recovered_workflow,
)
from streamflow.recovery.recovery import (
    _is_token_available,
    get_necessary_tokens,
    is_next_of_someone,
    _put_tokens,
    ProvenanceGraphNavigation,
    _populate_workflow,
    set_combinator_status,
    RollbackRecoveryPolicy,
    _set_scatter_inner_state,
)

from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext

from streamflow.workflow.utils import get_job_token
from streamflow.workflow.step import TransferStep, ExecuteStep
from streamflow.workflow.token import (
    TerminationToken,
    JobToken,
)

from streamflow.recovery.utils import extra_data_print


async def _execute_transfer_step(failed_step, new_workflow, port_name):
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


class PortRecovery:
    def __init__(self, port):
        self.port = port
        self.waiting_token = 1


class JobRequest:
    def __init__(self):
        self.version = 1
        self.job_token: JobToken | None = None
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
        self.max_retries: int = max_retries
        self.retry_delay: int | None = retry_delay

        self.create_request_lock = asyncio.Condition()
        # { job.name : RequestJob }
        self.job_requests: MutableMapping[str, JobRequest] = {}

    async def has_token_already_been_recovered(
        self,
        token,
        token_visited,
        available_new_job_tokens,
        running_new_job_tokens,
        context,
    ):
        if (
            not isinstance(token, JobToken)
            or token.value.name not in self.job_requests.keys()
        ):
            return False

        async with self.job_requests[token.value.name].lock:
            job_request = self.job_requests[token.value.name]

            # todo: togliere parametro is_running, usare lo status dello step OPPURE status dentro lo scheduler
            # todo: questo controllo forse meglio toglierlo da qui. Dal momento tra cui li individuo is_running al
            #  momento in cui li attacco al workflow (sync_rollbacks) i job potrebbero terminare e non avrei i token
            #  in input. Una soluzione facile è costruire tutto il grafo ignorando quelli running in questo
            #  momento. Poi sync_rollbacks aggiusta tutto. La pecca è di costruire tutto il grafo quando in realtà
            #  non serve perché poi verrà prunato fino al job running.
            if job_request.is_running:
                logger.debug(
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
                out_tokens_json = await context.database.get_out_tokens_from_job_token(
                    token.persistent_id
                )
                # out_tokens_json dovrebbe essere una lista...caso in cui un job produce più token
                if not out_tokens_json:
                    logger.debug(
                        f"il job {token.value.name} ({token.persistent_id}) ha il out_token_json null",
                    )
                    return False
                logger.debug(
                    f"il job {token.value.name} ({token.persistent_id}) ha il out_token_json['id']: {out_tokens_json['id']} vs job_request.token_output: {job_request.token_output}"
                    f"\n\t out_tokens_json: {dict(out_tokens_json)}",
                )
                port_json = await context.database.get_port_from_token(
                    out_tokens_json["id"]
                )
                is_available = await _is_token_available(
                    job_request.token_output[port_json["name"]], context
                )
                logger.debug(
                    f"il job {token.value.name} ({token.persistent_id}) - out_token_json['id'] {out_tokens_json['id']} ha il port_json {dict(port_json)}"
                    f"\n\til token di output {job_request.token_output[port_json['name']]} è già disponibile? {is_available}"
                )
                if is_available:
                    available_new_job_tokens[token.persistent_id] = {
                        "out-token-port-name": port_json["name"],
                        # "new-job-token": job_request.job_token.persistent_id,
                        "new-out-token": job_request.token_output[
                            port_json["name"]
                        ].persistent_id,
                        # "old-job-token": token.persistent_id,
                        "old-out-token": out_tokens_json["id"],
                        "job-name": token.value.name,  # solo per debug
                        # "value": out_tokens_json["value"],
                    }
                    token_visited[token.persistent_id] = (
                        token,
                        True,
                    )
                    # token_visited[job_request.job_token.persistent_id] = (
                    #     job_request.job_token,
                    #     True,
                    # )
                    token_visited[
                        job_request.token_output[port_json["name"]].persistent_id
                    ] = (job_request.token_output[port_json["name"]], True)
                    return True
                else:
                    self.job_requests[token.value.name].job_token = None
                    # todo: come gestire i vari kport? Se un token non è valido mettere a None solo la kport
                    #  di quel token o tutta? la cosa migliore sarebbe solo di quella persa
                    self.job_requests[token.value.name].token_output = None
                    self.job_requests[token.value.name].workflow = None
        return False

    async def is_running_token(self, token):
        if isinstance(token, JobToken) and token.value.name in self.job_requests.keys():
            async with self.job_requests[token.value.name].lock:
                if self.job_requests[token.value.name].is_running:
                    return True
                elif self.job_requests[token.value.name].token_output and all(
                    [
                        await _is_token_available(t, self.context)
                        for t in self.job_requests[
                            token.value.name
                        ].token_output.values()
                    ]
                ):
                    return True
        return False

    async def _sync_requests(
        self,
        job_token,
        failed_step,
        new_workflow,
        wr,
        loading_context,
        job_token_list,
        map_job_port,
        enable_sync,
    ):
        async with self.job_requests[job_token.value.name].lock:
            if self.job_requests[job_token.value.name].is_running:
                logger.debug(
                    f"Job {job_token.value.name} già in esecuzione nel workflow {self.job_requests[job_token.value.name].workflow} -> Ricostruire dag"
                )

                # alternativa: dal dag_ports prendo tutte le port successive al job_token,
                # con il database ricavo quali di queste porte sono output di uno ExecuteStep
                # get outport ScheduleStep
                job_token_port_row = await wr.context.database.get_port_from_token(
                    job_token.persistent_id
                )
                job_token_port_name = job_token_port_row["name"]
                job_token_port_name_m2 = get_key_by_value(
                    job_token.persistent_id, wr.port_tokens
                )  # new version of job_token_port_row["name"]
                if job_token_port_name != job_token_port_name_m2:
                    raise FailureHandlingException(
                        "Ottenere port name del job token. Il nuovo metodo e quello vecchio hanno dato risultati diversi"
                    )

                # these can be outport of ExecuteStep and TransferStep
                next_port_names = wr.dag_ports[job_token_port_name]
                output_ports = []
                for port_name in next_port_names:
                    if port_name not in wr.port_name_ids.keys():
                        continue
                    port_id = min(wr.port_name_ids[port_name])
                    step_rows = await wr.context.database.get_steps_from_output_port(
                        port_id
                    )
                    for step_row in await asyncio.gather(
                        *(
                            asyncio.create_task(
                                wr.context.database.get_step(sr["step"])
                            )
                            for sr in step_rows
                        )
                    ):
                        if issubclass(
                            get_class_from_name(step_row["type"]), ExecuteStep
                        ):
                            output_ports.append(
                                await Port.load(
                                    wr.context,
                                    port_id,
                                    loading_context,
                                    new_workflow,
                                )
                            )

                rows = await wr.context.database.get_executestep_outports_from_jobtoken(
                    job_token.persistent_id
                )
                execute_step_outports = await asyncio.gather(
                    *(
                        asyncio.create_task(
                            Port.load(
                                wr.context,
                                row["id"],
                                loading_context,
                                new_workflow,
                            )
                        )
                        for row in rows
                        if row["name"] not in new_workflow.ports.keys()
                        and row["name"] not in failed_step.output_ports.values()
                    )
                )
                # controllo per debug
                for p in execute_step_outports:
                    if (
                        p.name not in next_port_names
                        and p.name not in failed_step.output_ports.values()
                    ):
                        if isinstance(failed_step, TransferStep):
                            pass
                        raise FailureHandlingException(
                            f"wf {new_workflow.name} Port {p.name} non presente tra quelle presente nel dag_ports {next_port_names}"
                        )
                    logger.debug(
                        f"wf {new_workflow.name} Porta {p.name} trovata tra le next con il secondo metodo {next_port_names}"
                    )

                # output_ports is the new version of execute_step_outports
                a = set([op.name for op in execute_step_outports])
                b = set([op.name for op in output_ports])
                c = a - b
                d = b - a
                if c or d:
                    raise FailureHandlingException(
                        "Recupero port. Il nuovo metodo e quello vecchio hanno dato risultati diversi"
                    )

                # if the port can have more tokens (todo: caso non testato)
                for row in rows:
                    if row["name"] not in new_workflow.ports.keys():
                        continue
                    for pr in self.job_requests[job_token.value.name].queue:
                        if pr.port.name == row["name"]:
                            pr.waiting_token += 1
                            logger.debug(
                                f"new_workflow {new_workflow.name} already has port {pr.port.name}. Increased waiting_tokens: {pr.waiting_token}"
                            )
                            break

                # add port in the workflow and create port recovery (request)
                if enable_sync:
                    for port in execute_step_outports:
                        port.workflow = None
                        map_job_port[job_token.value.name] = port
                        logger.debug(
                            f"new_workflow {new_workflow.name} added in map_job_port[{job_token.value.name}] = {port.name}"
                        )
                        new_workflow.add_port(port)
                        port.workflow = new_workflow
                        # logger.debug(
                        #     f"new_workflow {new_workflow.name} added port {port.name} -> {new_workflow.ports.keys()}"
                        # )
                        self.job_requests[job_token.value.name].queue.append(
                            PortRecovery(port)
                        )

                # rimuovo i token generati da questo job token, pero'
                # non posso togliere tutti i token della port perché non so quali job li genera
                execute_step_out_token_ids = await get_execute_step_out_token_ids(
                    [
                        row["depender"]
                        for row in await wr.context.database.get_dependers(
                            job_token.persistent_id
                        )
                    ],
                    wr.context,
                )
                for t_id in execute_step_out_token_ids:
                    for prev_t_id in (
                        row["dependee"]
                        for row in await wr.context.database.get_dependees(t_id)
                    ):
                        await wr.remove_token_by_id(prev_t_id)
                for p in execute_step_outports:
                    if not is_next_of_someone(p.name, wr.dag_ports):
                        wr.dag_ports[INIT_DAG_FLAG].add(p.name)
                    wr.port_tokens.setdefault(p.name, set()).add(TOKEN_WAITER)
                logger.debug(
                    f"SYNC job {job_token.value.name} "
                    f"in esecuzione in wf {self.job_requests[job_token.value.name].workflow} "
                    f"servirà al wf {new_workflow.name}",
                )
            else:
                logger.debug(
                    f"Job {job_token.value.name} posto running, mentre job_token e token_output posti a None. (Valore corrente jt: {self.job_requests[job_token.value.name].job_token} - t: {self.job_requests[job_token.value.name].token_output})"
                )
                job_token_list.append(job_token)
                if enable_sync:
                    self.job_requests[job_token.value.name].is_running = True
                    self.job_requests[job_token.value.name].job_token = None
                    self.job_requests[job_token.value.name].token_output = None
                    self.job_requests[job_token.value.name].workflow = new_workflow.name

    async def sync_rollbacks(
        self,
        new_workflow,
        loading_context,
        failed_step,
        wr,
        enable_sync=True,
    ):
        map_job_port = {}
        job_token_list = []
        for token in (
            t for t, _ in wr.token_visited.values() if isinstance(t, JobToken)
        ):
            # update job request
            if token.value.name not in self.job_requests.keys():
                if enable_sync:
                    self.job_requests[token.value.name] = JobRequest()
                    self.job_requests[token.value.name].workflow = new_workflow.name
                job_token_list.append(token)
            else:
                await self._sync_requests(
                    token,
                    failed_step,
                    new_workflow,
                    wr,
                    loading_context,
                    job_token_list,
                    map_job_port,
                    enable_sync,
                )
            if enable_sync:
                await self.update_job_statuses(job_token_list)
        return [t.value.name for t in job_token_list]

    def add_waiter(self, job_name, port_name, port_recovery=None):
        if port_recovery:
            port_recovery.waiting_token += 1
        else:
            port_recovery = PortRecovery(Port(None, port_name))
            self.job_requests[job_name].queue.append(port_recovery)
        return port_recovery

    async def setup_job_request(self, job_name, default_is_running=True):
        if job_name not in self.job_requests.keys():
            async with self.create_request_lock:
                request = JobRequest()
                request.is_running = default_is_running
                return self.job_requests.setdefault(job_name, request)
        return self.job_requests[job_name]

    async def update_job_statuses(self, job_token_list):
        for token in job_token_list:
            async with self.job_requests[token.value.name].lock:
                # save jobs recovered
                if (
                    self.max_retries is None
                    or self.job_requests[token.value.name].version < self.max_retries
                ):
                    self.job_requests[token.value.name].version += 1
                    logger.debug(
                        f"Updated Job {token.value.name} at {self.job_requests[token.value.name].version} times"
                    )
                    # free resources scheduler
                    await self.context.scheduler.notify_status(
                        token.value.name, Status.ROLLBACK
                    )
                    self.context.scheduler.deallocate_job(
                        token.value.name, keep_job_allocation=True
                    )
                else:
                    logger.error(
                        f"FAILED Job {token.value.name} {self.job_requests[token.value.name].version} times. Execution aborted"
                    )
                    raise FailureHandlingException()

    async def update_job_status(self, job_name, lock):
        if (
            self.max_retries is None
            or self.job_requests[job_name].version < self.max_retries
        ):
            self.job_requests[job_name].version += 1
            logger.debug(
                f"Updated Job {job_name} at {self.job_requests[job_name].version} times"
            )
            # free resources scheduler
            await self.context.scheduler.notify_status(job_name, Status.ROLLBACK)
            self.context.scheduler.deallocate_job(job_name, keep_job_allocation=True)
        else:
            logger.error(
                f"FAILED Job {job_name} {self.job_requests[job_name].version} times. Execution aborted"
            )
            raise FailureHandlingException()

    # todo: situazione problematica
    #  A -> B
    #  A -> C
    #  B -> C
    # A ha successo, B fallisce (cade ambiente), viene rieseguito A, in C che input di A arriva?
    # quello vecchio? quello vecchio e quello nuovo? In teoria solo quello vecchio, da gestire comunque?
    # oppure lasciamo che fallisce e poi il failure manager prende l'output nuovo di A?
    async def _recover_jobs(self, failed_job: Job, failed_step: Step):
        loading_context = DefaultDatabaseLoadingContext()
        rrp = RollbackRecoveryPolicy(self.context)
        new_workflow, last_iteration = await rrp.recover_workflow(
            failed_job, failed_step, loading_context
        )
        await _execute_recovered_workflow(
            new_workflow, failed_step.name, failed_step.output_ports
        )
        # if last_iteration:
        #     logger.debug(f"Create last iteration from wf {new_workflow.name}")
        #     new_workflow_last_iteration = await self._recover_jobs_3(
        #         failed_step,
        #         failed_job,
        #         last_iteration,
        #         loading_context,
        #         new_workflow,
        #     )
        #     logger.debug(
        #         f"Last iteration {new_workflow.name} managed by {new_workflow_last_iteration.name}"
        #     )
        #     for port_name in last_iteration:
        #         new_workflow_last_iteration.ports[port_name].token_list.pop(0)
        #         new_workflow_last_iteration.ports[port_name].token_list.insert(
        #             0, get_last_token(new_workflow.ports[port_name].token_list)
        #         )
        #     await _execute_recovered_workflow(
        #         new_workflow_last_iteration, failed_step.name, None
        #     )
        #     return new_workflow_last_iteration
        return new_workflow

    async def _recover_jobs_3(
        self,
        failed_step,
        failed_job,
        last_iteration,
        loading_context,
        prev_workflow,
    ):
        new_workflow = Workflow(
            context=prev_workflow.context,
            type=prev_workflow.type,
            name=random_name(),
            config=prev_workflow.config,
        )
        dag = {}
        for k, t in failed_job.inputs.items():
            if t.persistent_id is None:
                raise FailureHandlingException("Token has not a persistent_id")
            # se lo step è Transfer, allora non tutti gli input del job saranno nello step
            if k in failed_step.input_ports.keys():
                dag[failed_step.get_input_port(k).name] = {failed_step.name}
            else:
                logger.debug(f"Step {failed_step.name} has not the input port {k}")
        dag[failed_step.get_input_port("__job__").name] = {failed_step.name}
        wr = ProvenanceGraphNavigation(
            context=new_workflow.context,
            output_ports=list(failed_step.input_ports.values()),
            port_name_ids={k: {v for v in vals} for k, vals in last_iteration.items()},
            dag_ports=dag,
        )
        job_token = get_job_token(
            failed_job.name,
            failed_step.get_input_port("__job__").token_list,
        )
        tokens = deque(failed_job.inputs.values())  # tokens to check
        tokens.append(job_token)
        logger.debug(f"last_iteration: {set(last_iteration.keys())}")
        await wr.build_dag(
            tokens, new_workflow, loading_context, set(last_iteration.keys())
        )
        wr.token_visited = get_necessary_tokens(wr.port_tokens, wr.token_visited)

        # todo wr.inputs_ports non viene aggiornato
        # if (set(wr.input_ports) - set(wr.dag_ports[INIT_DAG_FLAG])) or (set(wr.dag_ports[INIT_DAG_FLAG]) - set(wr.input_ports)):
        #     pass

        p, s = await wr.get_port_and_step_ids()
        await _populate_workflow(
            wr,
            p,
            s,
            failed_step,
            new_workflow,
            loading_context,
        )
        logger.debug("end populate")

        for port in failed_step.get_input_ports().values():
            if port.name not in new_workflow.ports.keys():
                raise FailureHandlingException(
                    f"La input port {port.name} dello step fallito {failed_step.name} non è presente nel new_workflow {new_workflow.name}"
                )

        _set_scatter_inner_state(
            new_workflow, wr.dag_ports, wr.port_tokens, wr.token_visited
        )
        logger.debug("end _set_scatter_inner_state")

        last_iteration = await _put_tokens(
            new_workflow,
            wr.dag_ports[INIT_DAG_FLAG],
            wr.port_tokens,
            wr.token_visited,
            wr,
        )
        logger.debug("end _put_tokens")
        await set_combinator_status(new_workflow, prev_workflow, wr, loading_context)

        if last_iteration:
            raise FailureHandlingException(
                f"Workflow {new_workflow.name} has too much iteration (just 1 iteration is valid) {last_iteration}"
            )
        extra_data_print(
            prev_workflow,
            new_workflow,
            None,
            wr.token_visited,
            last_iteration,
        )
        return new_workflow

    async def get_job_token(self, job_token):
        if job_token.value.name in self.job_requests.keys():
            async with self.job_requests[job_token.value.name].lock:
                return self.job_requests[job_token.value.name].job_token
        return None

    async def get_token(self, job_name, output_name):
        if job_name not in self.job_requests.keys():
            raise WorkflowExecutionException(
                f"Job {job_name} was not rolled back. Unable to get token on port {output_name}"
            )
        async with self.job_requests[job_name].lock:
            if (
                self.job_requests[job_name].token_output is None
                or output_name not in self.job_requests[job_name].token_output.keys()
            ):
                raise WorkflowExecutionException(
                    f"Job rollback {job_name} has no token on port {output_name}"
                )
            return self.job_requests[job_name].token_output[output_name]

    async def get_tokens(self, job_name):
        if job_name not in self.job_requests.keys():
            raise WorkflowExecutionException(
                f"Job {job_name} was not rolled back. Unable to get tokens"
            )
        async with self.job_requests[job_name].lock:
            if self.job_requests[job_name].token_output is None:
                raise WorkflowExecutionException(
                    f"Job rollback {job_name} has no tokens"
                )
            return self.job_requests[job_name].token_output

    async def close(self):
        ...

    async def notify_jobs(self, job_token, out_port_name, token):
        job_name = job_token.value.name
        logger.info(f"Notify end job {job_name}")
        if job_name in self.job_requests.keys():
            async with self.job_requests[job_name].lock:
                if self.job_requests[job_name].job_token is None:
                    self.job_requests[job_name].job_token = job_token
                if self.job_requests[job_name].token_output is None:
                    self.job_requests[job_name].token_output = {}
                self.job_requests[job_name].token_output.setdefault(
                    out_port_name, token
                )

                # todo: fare a tutte le port nella queue la put del token
                elems = []
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Job {job_name} is notifing on port {out_port_name}. There are {len(self.job_requests[job_name].queue)} workflows in waiting"
                    )
                if len(self.job_requests[job_name].queue):
                    str_port = "".join(
                        [
                            f"\n\tHa trovato port_name {elem.port.name} port_id {elem.port.persistent_id} workflow {elem.port.workflow.name} token_list {elem.port.token_list} queues {elem.port.queues}. Waiting per {elem.waiting_token} prima del terminationtoken"
                            if elem
                            else "\n\t\tElem-None"
                            for elem in self.job_requests[job_name].queue
                        ]
                    )
                    logger.debug(f"port in coda: {str_port}")

                for elem in self.job_requests[job_name].queue:
                    if elem.port.name == out_port_name:
                        elem.port.put(token)
                        # todo: non è giusto, potrebbe dover aspettare altri token
                        elem.waiting_token -= 1
                        if elem.waiting_token == 0:
                            elems.append(elem)
                            elem.port.put(TerminationToken())
                        str_t = json.dumps(
                            {
                                "p.name": elem.port.name,
                                "p.id": elem.port.persistent_id,
                                "wf": elem.port.workflow.name,
                                "p.token_list_len": len(elem.port.token_list),
                                "p.queue": list(elem.port.queues.keys()),
                                "Ha ricevuto token": token.persistent_id,
                            },
                            indent=2,
                        )
                        msg_pt2 = (
                            f"Aspetta {elem.waiting_token} tokens prima di mettere il terminationtoken"
                            if elem.waiting_token
                            else "Mandato anche termination token"
                        )
                        logger.debug(
                            f"Token added into Port of another wf {str_t}. {msg_pt2}"
                        )

                for elem in elems:
                    self.job_requests[job_name].queue.remove(elem)
                logger.info(f"notify - job {job_name} is not running anymore")
                self.job_requests[job_name].is_running = False
                logger.info(f"Notify end job {job_name} - done")

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "default_failure_manager.json")
        )

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow = await self._recover_jobs(job, step)

            # debug
            if new_workflow.steps.keys():
                async with self.job_requests[job.name].lock:
                    new_job_token = get_job_token(
                        job.name,
                        new_workflow.steps[step.name]
                        .get_input_port("__job__")
                        .token_list,
                    )
                    if self.job_requests[job.name].job_token is None:
                        raise FailureHandlingException(
                            f"Job {job.name} has not a job_token. In the workflow {new_workflow.name} has been found job_token {new_job_token.persistent_id}."
                        )

            command_output = CommandOutput(
                value=None,
                status=new_workflow.steps[step.name].status
                if new_workflow.steps.keys()
                else Status.COMPLETED,
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
            logger.debug("WorkflowTransferException ma stavo gestendo execute job")
            raise
        except Exception as e:
            logger.exception(e)
            return await self.handle_exception(job, step, e)
        return command_output

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {type(exception).__name__} failure for job {job.name}"
            )
        if job.name in self.job_requests.keys():
            logger.info(f"handle_exception: job {job.name} is not running anymore")
            async with self.job_requests[job.name].lock:
                self.job_requests[job.name].is_running = False
        return await self._do_handle_failure(job, step)

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Handling command failure for job {job.name}")

        if job.name in self.job_requests.keys():
            logger.info(f"handle_failure: job {job.name} is not running anymore")
            async with self.job_requests[job.name].lock:
                self.job_requests[job.name].is_running = False
        return await self._do_handle_failure(job, step)

    async def handle_failure_transfer(
        self, job: Job, step: Step, port_name: str
    ) -> Token:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {WorkflowTransferException.__name__} failure for job {job.name}"
            )
        if job.name in self.job_requests.keys():
            logger.info(
                f"handle_failure_transfer: job {job.name} is not running anymore"
            )
            async with self.job_requests[job.name].lock:
                self.job_requests[job.name].is_running = False
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow = await self._recover_jobs(job, step)
            token = await _execute_transfer_step(step, new_workflow, port_name)
        # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except (WorkflowTransferException, WorkflowExecutionException) as e:
            logger.exception(e)
            return await self.handle_failure_transfer(job, step, port_name)
        except Exception as e:
            logger.exception(e)
            raise e
        return token


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

    async def get_job_token(self, job_token):
        return job_token

    async def notify_jobs(self, job_name, out_port_name, token):
        ...

    async def handle_failure_transfer(self, job: Job, step: Step, port_name: str):
        return None

    async def get_token(self, job_name, output_name):
        ...

    async def get_tokens(self, job_name):
        ...
