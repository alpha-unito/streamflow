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
)
from streamflow.core.recovery import FailureManager
from streamflow.core.workflow import CommandOutput, Job, Status, Step, Port, Token
from streamflow.cwl.token import CWLFileToken
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.recovery.recovery import JobVersion
from streamflow.token_printer import printa_token
from streamflow.workflow.step import ScatterStep
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import ExecuteStep
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.token import TerminationToken, JobToken

from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow

from streamflow.workflow.utils import get_job_token


async def _cleanup_dir(
    connector: Connector, location: Location, directory: str
) -> None:
    await remotepath.rm(
        connector, location, await remotepath.listdir(connector, location, directory)
    )


async def _load_prev_tokens(token_id, loading_context, context):
    rows = await context.database.get_dependees(token_id)

    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["dependee"]))
            for row in rows
        )
    )


def _get_data_location(path, context):
    data_locs = context.data_manager.get_data_locations(path)
    for data_loc in data_locs:
        if data_loc.path == path:
            return data_loc
    return None


async def _put_tokens(
    new_workflow: Workflow,
    token_port: MutableMapping[int, str],
    port_tokens_counter: MutableMapping[str, int],
    dag_tokens: MutableMapping[int, MutableSet[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
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
    return token_port, port_tokens_counter


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


def temp_print_retag(workflow_name, output_port, tag, retags, final_msg):
    print(
        f"wf {workflow_name} - port {output_port.name} - tag {tag}",
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

        # { workflow.name : { port.id: [ token.id ] } }
        self.retags: MutableMapping[str, MutableMapping[int, MutableSequence[int]]] = {}

        # { job.name : RequestJob }
        self.job_requests: MutableMapping[str, JobRequest] = {}

    async def _is_available(
        self,
        token,
        token_visited,
        available_new_job_tokens,
        running_new_job_tokens,
        context,
        loading_context,
    ):
        if isinstance(token, JobToken) and token.value.name in self.job_requests.keys():
            async with self.job_requests[token.value.name].lock:
                job_request = self.job_requests[token.value.name]

                # todo: togliere parametro is_running, usare lo status dello step
                if job_request.is_running:
                    print(
                        f"Il job {token.value.name} {token.persistent_id} - è running, dovrei aspettare il suo output"
                    )

                    running_new_job_tokens[token.persistent_id] = {
                        "jobtoken": job_request
                    }
                elif (
                    job_request.job_token
                    and job_request.job_token.persistent_id
                    and token.persistent_id != job_request.job_token.persistent_id
                    and job_request.token_output
                ):
                    # todo: Fare lo load del token direttamente quando recupero
                    #   così se più avanti serve, non si deve riaccedere al db ....
                    #   update: FORCE NON SERVE FARLO
                    job_out_token_json = await context.database.get_job_out_token(
                        token.persistent_id
                    )
                    print(
                        "Il job",
                        token.value.name,
                        token.persistent_id,
                        " - job_out_token_json:",
                        dict(job_out_token_json) if job_out_token_json else "null",
                    )
                    if job_out_token_json:
                        port_json = await context.database.get_port_by_token(
                            job_out_token_json["id"]
                        )
                        print(
                            "Il job",
                            token.value.name,
                            token.persistent_id,
                            " - port_json",
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
                                "old-out-token": job_out_token_json["id"],
                                "new-job-token": job_request.job_token.persistent_id,
                                "new-out-token": job_request.token_output[
                                    port_json["name"]
                                ].persistent_id,
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
                            return True
                    else:
                        print(
                            f"Il job {token.value.name} {token.persistent_id} - impossibile recuperare il token perché non so quale porta è: {job_request.token_output} "
                        )
                    pass
        return False

    def _remove_token(self, dag_tokens, remove_tokens):
        d = {}
        other_remove = set()
        for k, vals in dag_tokens.items():
            if k not in remove_tokens:
                d[k] = set()
                for v in vals:
                    if v not in remove_tokens:
                        d[k].add(v)
                if not d[k]:
                    other_remove.add(k)
        if other_remove:
            d = self._remove_token(d, other_remove)
        return d

    def clean_dag(self, dag_tokens, token_id_list):
        next_round = set()
        for key in dag_tokens.keys():
            dag_tokens[key].difference_update(token_id_list)
            if not dag_tokens[key]:
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
        # { token.id : set of next tokens' id | string }
        dag_tokens = {}
        for t in failed_job.inputs.values():
            dag_tokens[t.persistent_id] = set((failed_step.name,))

        # { token_id: (token, is_available)}
        all_token_visited = {}

        # {old job token id : (new job token id, new output token id)}
        available_new_job_tokens = {}

        # {old token id : job token running}
        running_new_job_tokens = {}

        while tokens:
            token = tokens.pop()

            if not await self._is_available(
                token,
                all_token_visited,
                available_new_job_tokens,
                running_new_job_tokens,
                workflow.context,
                loading_context,
            ):
                is_available = await _is_token_available(token, workflow.context)
                if isinstance(token, CWLFileToken):
                    print("token isav", token, is_available)

                # impossible case because when added in tokens, the elem is checked
                if token.persistent_id in all_token_visited.keys():
                    raise FailureHandlingException(
                        f"Token {token.persistent_id} already visited"
                    )
                all_token_visited[token.persistent_id] = (token, is_available)

                if not is_available:
                    prev_tokens = await _load_prev_tokens(
                        token.persistent_id,
                        loading_context,
                        workflow.context,
                    )
                    if prev_tokens:
                        for pt in prev_tokens:
                            dag_tokens.setdefault(pt.persistent_id, set()).add(
                                token.persistent_id
                            )
                            if (
                                pt.persistent_id not in all_token_visited.keys()
                                and not contains_token_id(pt.persistent_id, tokens)
                            ):
                                tokens.append(pt)
                    else:
                        dag_tokens.setdefault(INIT_DAG_FLAG, set()).add(
                            token.persistent_id
                        )
                else:
                    dag_tokens.setdefault(INIT_DAG_FLAG, set()).add(token.persistent_id)
                # alternativa ai due else ... però è più difficile la lettura del codice (ancora da provare)
                # if is_available or not prev_tokens:
                #     add_elem_dictionary(INIT_DAG_FLAG, token.persistent_id, dag_tokens)

        print(
            "available_new_job_tokens:",
            len(available_new_job_tokens) > 0,
            json.dumps(available_new_job_tokens, indent=2),
        )
        for k, v in available_new_job_tokens.items():
            if k not in all_token_visited.keys():
                print(k, "missing")
            for vv in v.values():
                if vv not in all_token_visited.keys():
                    print(vv, "missing")
        # print("running_new_job_tokens:", json.dumps(running_new_job_tokens, indent=2))
        pass

        all_token_visited = dict(sorted(all_token_visited.items()))
        await printa_token(
            all_token_visited,
            workflow,
            dag_tokens,
            loading_context,
            "prima-e-dopo/"
            + str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
            + "_graph_steps_"
            + new_workflow_name
            + "_prima",
        )
        to_remove = [v["old-out-token"] for v in available_new_job_tokens.values()]
        while to_remove:
            to_remove = self.clean_dag(
                dag_tokens,
                to_remove,
            )
            for k in to_remove:
                dag_tokens.pop(k)
        for values in available_new_job_tokens.values():
            if values["old-out-token"] in dag_tokens:
                await self._update_dag(
                    dag_tokens,
                    values["old-out-token"],
                    values["new-out-token"],
                    all_token_visited,
                    workflow,
                    loading_context,
                )
            else:
                print("ECCO CHI FACEVA CADERE TUTTO", values["old-out-token"])
        await printa_token(
            all_token_visited,
            workflow,
            dag_tokens,
            loading_context,
            "prima-e-dopo/graph_steps_"
            + str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
            + "_workflow-"
            + new_workflow_name
            + "_dopo",
        )
        # print_graph_figure(dag_tokens, "graph_token_recovery")
        pass

        # mentre costrisco dag_tokens tengo traccia dei possibili token che posso recuperare:
        # - sono già statuses prodotti da altri job di un rollback passato (e sono ancora disponibili)
        # - saranno prodotti da qualche job da un rollback in esecuzione
        # In questo caso, segno i token come disponibili (anche se sono persi)
        # e non continuo a ricostruire quel ramo del grafo

        # oggetti sub-prodotti dalla costruizione del dag_tokens:
        # - lista dei job in esecuzione che mi servono (e token che producono).
        # - lista dei token disponibili che mi servono

        # Attendo la terminazione dei job running che mi servono.
        # Aggiungo questi nuovi token nella lista dei token disponibili.
        # Scansiono il grafo dag_tokens.
        # Rimuovo i jobToken che non mi servono più delle keys. (lo trovo nei value di altri token? controllare)
        # (questo passo non dovrebbe servire dato che è il token è nei value del jobtoken rimosso) - Rimuovo i token persi (prodotti dai jobToken) dalle keys di dag_tokens.
        # Sostituisco nei value i token persi con i token nuovi

        token_visited = {}
        for k, values in dag_tokens.items():
            if isinstance(k, int):
                token_visited[k] = all_token_visited[k]
            for v in values:
                if isinstance(v, int):
                    token_visited[v] = all_token_visited[v]
        return dag_tokens, token_visited

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

        dag_tokens, token_visited = await self._build_dag(
            tokens,
            failed_job,
            failed_step,
            workflow,
            loading_context,
            new_workflow.name,
        )

        # update class state (attributes)
        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                # update job request
                if token.value.name not in self.job_requests.keys():
                    self.job_requests[token.value.name] = JobRequest()
                else:
                    async with self.job_requests[token.value.name].lock:
                        self.job_requests[token.value.name].is_running = True

                # save jobs recovered
                if token.value.name not in self.jobs.keys():
                    self.jobs[token.value.name] = JobVersion(
                        version=1,
                    )
                if (
                    self.max_retries is None
                    or self.jobs[token.value.name].version < self.max_retries
                ):
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
                        f"job {token.value.name}. Il mio job_token ha un valore",
                        self.job_requests[token.value.name].job_token,
                        "adesso però lo pongo a none",
                    )
                self.job_requests[token.value.name].job_token = None

        token_port, port_tokens_counter = await _populate_workflow(
            failed_step,
            token_visited,
            new_workflow,
            loading_context,
        )

        if add_failed_step:
            new_workflow.add_step(
                await Step.load(
                    new_workflow.context,
                    failed_step.persistent_id,
                    loading_context,
                    new_workflow,
                )
            )
            for port in failed_step.get_input_ports().values():
                new_workflow.add_port(
                    await Port.load(
                        new_workflow.context,
                        port.persistent_id,
                        loading_context,
                        new_workflow,
                    )
                )

        await _put_tokens(
            new_workflow,
            token_port,
            port_tokens_counter,
            dag_tokens,
            token_visited,
        )

        pass
        self._save_for_retag(new_workflow, dag_tokens, token_port, token_visited)
        pass

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

    def _save_for_retag(self, new_workflow, dag_token, token_port, token_visited):
        # todo: aggiungere la possibilità di eserguire comunque tutti i job delle scatter aggiungendo un parametro nel StreamFlow file
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
                    self.retags[new_workflow.name].setdefault(port.name, set()).add(
                        token_visited[t_id][0]
                    )

    async def _execute_failed_job(
        self, failed_job, failed_step, new_workflow, loading_context
    ):
        new_job_token = get_job_token(
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
                    f"Inserisco token {t} in port {k}({port.name}), failed step -> len(port.token_list) {len(failed_step.get_output_port(k).token_list)}",
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
