from __future__ import annotations

import os
import json
import asyncio
import logging
import datetime
from typing import MutableMapping, MutableSequence, cast, Iterable

import pkg_resources

from streamflow.core.utils import (
    random_name,
)
from streamflow.core.exception import (
    FailureHandlingException,
    WorkflowTransferException,
    WorkflowExecutionException,
)
from streamflow.core.recovery import FailureManager
from streamflow.core.workflow import CommandOutput, Job, Status, Step, Port, Token
from streamflow.cwl.step import CWLLoopConditionalStep, CWLRecoveryLoopConditionalStep
from streamflow.log_handler import logger
from streamflow.recovery.recovery import (
    JobVersion,
    _is_token_available,
    INIT_DAG_FLAG,
    get_necessary_tokens,
    is_next_of_someone,
    TOKEN_WAITER,
    _put_tokens,
    WorkflowRecovery,
    _populate_workflow_lean,
)
from streamflow.recovery.utils import (
    get_execute_step_out_token_ids,
    get_token_by_tag,
    str_id,
)
from streamflow.token_printer import dag_workflow
from streamflow.workflow.step import ScatterStep, TransferStep
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import ExecuteStep
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.token import (
    TerminationToken,
    JobToken,
)

from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow

from streamflow.workflow.utils import get_job_token


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
        if (
            not isinstance(token, JobToken)
            or token.value.name not in self.job_requests.keys()
        ):
            return False

        async with self.job_requests[token.value.name].lock:
            job_request = self.job_requests[token.value.name]

            # todo: togliere parametro is_running, usare lo status dello step OPPURE status dentro lo scheduler
            # todo: questo controllo forse meglio toglierlo da qui. Dal momento tra cui li individuo is_running al
            #  momento in cui li attacco al wofklow (sync_rollbacks) i job potrebbero terminare e non avrei i token
            #  in input. Una soluzione facile è costruire tutto il grafo ignorando quelli running in questo
            #  momento. Poi sync_rollbacks aggiusta tutto. La pecca è di costruire tutto il grafo quando in realtà
            #  non serve perché poi verrà prunato fino al job running.
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
                out_tokens_json = await context.database.get_out_tokens_from_job_token(
                    token.persistent_id
                )
                # out_tokens_json dovrebbe essere una lista...caso in cui un job produce più token
                if not out_tokens_json:
                    print(
                        f"il job {token.value.name} ({token.persistent_id}) ha il out_token_json null",
                    )
                    return False
                print(
                    f"il job {token.value.name} ({token.persistent_id}) ha il out_token_json['id']: {out_tokens_json['id']} vs job_request.token_output: {job_request.token_output}",
                    f"\n\t out_tokens_json: {dict(out_tokens_json)}",
                )
                port_json = await context.database.get_port_from_token(
                    out_tokens_json["id"]
                )
                is_available = await _is_token_available(
                    job_request.token_output[port_json["name"]], context
                )
                print(
                    f"il job {token.value.name} ({token.persistent_id}) - out_token_json['id'] {out_tokens_json['id']} ha il port_json {dict(port_json)}",
                    f"\n\til token di output {job_request.token_output[port_json['name']]} è già disponibile? {is_available}",
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

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow, loading_context = await self._recover_jobs(
                job, step, add_failed_step=True
            )
            async with self.job_requests[job.name].lock:
                new_job_token = get_job_token(
                    job.name,
                    new_workflow.steps[step.name].get_input_port("__job__").token_list,
                )
                if self.job_requests[job.name].job_token is not None:
                    print(
                        f"WARN WARN WARN job {job.name} ha già un job_token {self.job_requests[job.name].job_token.persistent_id}. Però qui non dovrebbe averne. Io volevo aggiungere job_token {new_job_token.persistent_id}."
                    )
                self.job_requests[job.name].job_token = new_job_token
            # get new job created by ScheduleStep
            # command_output = await self._execute_failed_job(
            #     job, step, new_workflow, loading_context
            # )
            command_output = CommandOutput(
                value=None, status=new_workflow.steps[step.name].status
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
        new_workflow,
        loading_context,
        dir_path,
        failed_step,
        wr,
    ):
        job_executed_in_new_workflow = set()  # debug variable
        job_token_list = []
        for token, _ in wr.token_visited.values():
            if isinstance(token, JobToken):
                job_token_list.append(token)

        job_token_list_updated = []
        for token in job_token_list:
            # update job request
            if token.value.name not in self.job_requests.keys():
                self.job_requests[token.value.name] = JobRequest()
                self.job_requests[token.value.name].workflow = new_workflow.name
                job_token_list_updated.append(token)
            else:
                async with self.job_requests[token.value.name].lock:
                    if self.job_requests[token.value.name].is_running:
                        print(
                            f"Job {token.value.name} già in esecuzione nel workflow {self.job_requests[token.value.name].workflow} -> Ricostruire dag"
                        )

                        # alternativa: dal dag_ports prendo tutte le port successive al job_token,
                        # con il database ricavo quali di queste porte sono output di uno ExecuteStep
                        # get outport ScheduleStep
                        job_token_port_row = (
                            await new_workflow.context.database.get_port_from_token(
                                token.persistent_id
                            )
                        )
                        job_token_port_name = job_token_port_row["name"]

                        # these can be outport of ExecuteStep and TransferStep
                        next_port_names = wr.dag_ports[job_token_port_name]

                        rows = await new_workflow.context.database.get_executestep_outports_from_jobtoken(
                            token.persistent_id
                        )
                        execute_step_outports = await asyncio.gather(
                            *(
                                asyncio.create_task(
                                    Port.load(
                                        new_workflow.context,
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
                            print(
                                f"wf {new_workflow.name} Porta {p.name} trovata tra le next con il secondo metodo {next_port_names}"
                            )

                        # if the port can have more tokens (todo: caso non testato)
                        for row in rows:
                            if row["name"] not in new_workflow.ports.keys():
                                continue
                            for pr in self.job_requests[token.value.name].queue:
                                if pr.port.name == row["name"]:
                                    pr.waiting_token += 1
                                    print(
                                        f"new_workflow {new_workflow.name} already has port {pr.port.name}. Increased waiting_tokens: {pr.waiting_token}"
                                    )
                                    break

                        # add port in the workflow and create port recovery (request)
                        for port in execute_step_outports:
                            new_workflow.add_port(port)
                            print(
                                f"new_workflow {new_workflow.name} added port {port.name} -> {new_workflow.ports.keys()}"
                            )
                            self.job_requests[token.value.name].queue.append(
                                PortRecovery(port)
                            )

                        # rimuovo i token generati da questo job token, pero'
                        # non posso togliere tutti i token della port perché non so quali job li genera
                        execute_step_out_token_ids = (
                            await get_execute_step_out_token_ids(
                                [
                                    row["depender"]
                                    for row in await wr.context.database.get_dependers(
                                        token.persistent_id
                                    )
                                ],
                                new_workflow.context,
                            )
                        )
                        for t_id in execute_step_out_token_ids:
                            for prev_t_id in [
                                row["dependee"]
                                for row in await wr.context.database.get_dependees(t_id)
                            ]:
                                await wr.remove_token_by_id(prev_t_id)

                        for p in execute_step_outports:
                            if not is_next_of_someone(p.name, wr.dag_ports):
                                wr.dag_ports[INIT_DAG_FLAG].add(p.name)
                            wr.port_tokens.setdefault(p.name, set()).add(TOKEN_WAITER)

                        print(
                            f"SYNCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC job {token.value.name}",
                            f"in esecuzione in wf {self.job_requests[token.value.name].workflow}",
                            f"servirà al wf {new_workflow.name}",
                        )
                    else:
                        print(
                            f"Job {token.value.name} posto running, mentre job_token e token_output posti a None. (Valore corrente jt: {self.job_requests[token.value.name].job_token} - t: {self.job_requests[token.value.name].token_output})"
                        )
                        job_token_list_updated.append(token)
                        self.job_requests[token.value.name].is_running = True
                        self.job_requests[token.value.name].job_token = None
                        self.job_requests[token.value.name].token_output = None
                        self.job_requests[token.value.name].workflow = new_workflow.name

        for token in job_token_list_updated:
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

                    # free resources scheduler
                    await wr.context.scheduler.notify_status(
                        token.value.name, Status.ROLLBACK
                    )
                    wr.context.scheduler.deallocate_job(
                        token.value.name, keep_job_allocation=True
                    )
                else:
                    logger.error(
                        f"FAILED Job {token.value.name} {self.jobs[token.value.name].version} times. Execution aborted"
                    )
                    raise FailureHandlingException()
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
    ):
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

        dt = str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
        dir_path = f"graphs/{dt}-{new_workflow.name}"
        # os.makedirs(dir_path)

        tokens = list(failed_job.inputs.values())  # tokens to check
        tokens.append(job_token)

        # todo: se la risorsa non torna più su, penso si vada in un loop continuo di:
        #  - costruisco il dag
        #  - controllo che il file esiste
        #  - errore, test fallito perché la risorsa è giù
        #  - costruisci dag ...

        dag = {}
        for k, t in failed_job.inputs.items():
            if t.persistent_id is None:
                raise FailureHandlingException("Token has not a persistent_id")
            # se lo step è Transfer, allora non tutti gli input del job saranno nello step
            if k in failed_step.input_ports.keys():
                dag[failed_step.get_input_port(k).name] = {failed_step.name}
            else:
                print(f"Step {failed_step.name} has not the input port {k}")
        dag[failed_step.get_input_port("__job__").name] = {failed_step.name}

        wr = WorkflowRecovery(
            context=workflow.context,
            output_ports=list(failed_step.input_ports.values()),
            port_name_ids={
                port.name: port.persistent_id
                for port in failed_step.get_input_ports().values()
            },
            dag_ports=dag,
        )
        await wr.build_dag(tokens, workflow, loading_context)
        wr.token_visited = get_necessary_tokens(wr.port_tokens, wr.token_visited)
        print("build_dag: end build dag")

        print("Start sync-rollbacks")
        # update class state (attributes) and jobs synchronization
        job_executed_in_new_workflow = await self.sync_rollbacks(
            new_workflow,
            loading_context,
            dir_path,
            failed_step,
            wr,
        )
        wr.token_visited = get_necessary_tokens(wr.port_tokens, wr.token_visited)
        print("End sync-rollbacks")

        # todo wr.inputs_ports non viene aggiornato
        # if (set(wr.input_ports) - set(wr.dag_ports[INIT_DAG_FLAG])) or (set(wr.dag_ports[INIT_DAG_FLAG]) - set(wr.input_ports)):
        #     pass

        print()
        p, s = await wr.get_port_and_step_ids()
        print()

        await _populate_workflow_lean(
            wr,
            p,
            s,
            failed_step,
            new_workflow,
            loading_context,
        )
        if "/subworkflow/i1-back-propagation-transformer" in new_workflow.steps.keys():
            pass
            # raise FailureHandlingException("Caricata i1-back-prop CHE NON SERVE")
        print("end populate")

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
                        f"WARNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN. La input port {port.name} dello step fallito {failed_step.name} non è presente nel new_workflow {new_workflow.name} ... La aggiungo"
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
            new_workflow, wr.dag_ports, wr.port_tokens, wr.token_visited
        )
        print("end save_for_retag")

        await _put_tokens(
            new_workflow,
            wr.dag_ports[INIT_DAG_FLAG],
            wr.port_tokens,
            wr.token_visited,
        )
        print("end _put_tokens")

        # for debug
        print(f"New workflow {new_workflow.name} popolato così:")
        sorted_jobs = list(job_executed_in_new_workflow)
        sorted_jobs.sort()
        print(
            f"\t{len(new_workflow.steps.keys())} steps e {len(new_workflow.ports.keys())} ports"
        )
        print(f"\tJobs da rieseguire: {sorted_jobs}")
        dag_workflow(new_workflow, dir_path + "/new-wf")
        for step in new_workflow.steps.values():
            print(f"step {step.name} wf {step.workflow.name}")
            try:
                print(
                    f"Step {step.name}\n\tinput ports",
                    {
                        k_p: [(str_id(t), t.tag) for t in port.token_list]
                        for k_p, port in step.get_input_ports().items()
                    },
                    "\n\tkey-port_name",
                    {k: v.name for k, v in step.get_input_ports().items()},
                )
            except Exception as e:
                print(f"exception {step.name} -> {e}")
                raise

        # PROBLEMA: Ci sono troppi when-recovery step
        if (
            a := len(
                [
                    s
                    for s in new_workflow.steps.values()
                    if isinstance(
                        s, (CWLLoopConditionalStep, CWLRecoveryLoopConditionalStep)
                    )
                ]
            )
        ) > 1:
            raise FailureHandlingException(f"Ci sono troppi LoopConditionalStep: {a}")

        # PROBLEMA: c'è uno step che non dovrebbe essere caricato
        if "/subworkflow/i1-back-propagation-transformer" in new_workflow.steps.keys():
            raise FailureHandlingException("Caricata i1-back-prop CHE NON SERVE")

        # INFO: ci sarà una iterazione precedente
        # "/subworkflow-loop-terminator" in new_workflow.steps.keys()
        pass

        print(f"VIAAAAAAAAAAAAAA {new_workflow.name}")
        await new_workflow.save(workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        await executor.run()
        print("executor.run", new_workflow.name, "terminated")
        return new_workflow, loading_context

    def is_valid_tag(self, workflow_name, tag, output_port):
        if workflow_name not in self.retags.keys():
            return True
        if output_port.name not in self.retags[workflow_name].keys():
            return True

        token_list = self.retags[workflow_name][output_port.name]
        for t in token_list:
            if t.tag == tag:
                return True
        return False

    def _save_for_retag(self, new_workflow, dag_ports, port_tokens, token_visited):
        # todo: aggiungere la possibilità di eserguire comunque tutti i job delle scatter aggiungendo un parametro nel StreamFlow file
        if new_workflow.name not in self.retags.keys():
            self.retags[new_workflow.name] = {}

        for step in new_workflow.steps.values():
            if isinstance(step, ScatterStep):
                port = step.get_output_port()
                print(
                    f"_save_for_retag wf {new_workflow.name} -> port_tokens[{step.get_output_port().name}]:",
                    [
                        (t_id, token_visited[t_id][0].tag, token_visited[t_id][1])
                        for t_id in port_tokens[port.name]
                    ],
                )
                for t_id in port_tokens[port.name]:
                    if (
                        t_id
                        not in dag_ports[
                            INIT_DAG_FLAG
                        ]  # todo: questo controllo è sempre true.... in dag_ports non ci sono t_id
                        # and not token_visited[t_id][1]
                    ):
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
                print(
                    f"WARN WARN WARN job {failed_job.name} ha già un job_token {self.job_requests[failed_job.name].job_token.persistent_id}. Però qui non dovrebbe averne. Io volevo aggiungere job_token {new_job_token.persistent_id}."
                )
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

    # deprecated
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
        pass

    async def notify_jobs(self, job_name, out_port_name, token):
        logger.info(f"Notify end job {job_name}")
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

                for elem in self.job_requests[job_name].queue:
                    if elem.port.name == out_port_name:
                        elem.port.put(token)
                        # todo: non è giusto, potrebbe dover aspettare altri token
                        elem.waiting_token -= 1
                        if elem.waiting_token == 0:
                            elems.append(elem)
                            elem.port.put(TerminationToken())
                        print(
                            "Token added into Port of another wf",
                            json.dumps(
                                {
                                    "p.name": elem.port.name,
                                    "p.id": elem.port.persistent_id,
                                    "wf": elem.port.workflow.name,
                                    "p.token_list_len": len(elem.port.token_list),
                                    "p.queue": list(elem.port.queues.keys()),
                                    "Ha ricevuto token": token.persistent_id,
                                },
                                indent=2,
                            ),
                            f"Aspetta {elem.waiting_token} tokens prima di mettere il terminationtoken"
                            if elem.waiting_token
                            else "Mandato anche termination token",
                        )

                for elem in elems:
                    self.job_requests[job_name].queue.remove(elem)
                logger.info("Notify end job {job_name} - done")

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

    async def handle_failure_transfer(
        self, job: Job, step: Step, port_name: str
    ) -> Token:
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
            token = await self._execute_transfer_step(step, new_workflow, port_name)
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
            # return await self.handle_exception(job, step, e)
        return token

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
