from __future__ import annotations

import asyncio
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
from streamflow.workflow.token import TerminationToken, JobToken, ListToken, ObjectToken

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


async def data_location_exists(data_locations, context, token):
    for data_loc in data_locations:
        if data_loc.path == token.value["path"]:
            connector = context.deployment_manager.get_connector(data_loc.deployment)
            # location_allocation = job_version.step.workflow.context.scheduler.location_allocations[data_loc.deployment][data_loc.name]
            # available_locations = job_version.step.workflow.context.scheduler.get_locations(location_allocation.jobs[0])
            if await remotepath.exists(connector, data_loc, token.value["path"]):
                return True
    return False


def _get_data_location(path, context):
    data_locs = context.data_manager.get_data_locations(path)
    for data_loc in data_locs:
        if data_loc.path == path:
            return data_loc
    return None


async def is_token_available(token, context, loading_context):
    if isinstance(token, CWLFileToken):
        # TODO: è giusto cercare una loc dal suo path? se va bene aggiustare data_location_exists method
        data_loc = _get_data_location(token.value["path"], context)
        # print(f"token.id: {token.persistent_id} ({token.value['path']})")
        if not data_loc:  # if the data are in invalid locations, data_loc is None
            return False
        if not await data_location_exists([data_loc], context, token):
            logger.debug(f"Invalidated path {token.value['path']}")
            # todo: invalidare tutti i path del data_loc
            # context.data_manager.invalidate_location(
            #     data_loc, token.value["path"]
            # )
            context.data_manager.invalidate_location(data_loc, "/")
            return False
        # todo: controllare checksum con token.value['checksum'] ?
        return True
    if isinstance(token, ListToken):
        # if at least one file does not exist, returns false
        return all(
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        is_token_available(inner_token, context, loading_context)
                    )
                    for inner_token in token.value
                )
            )
        )
    if isinstance(token, ObjectToken):
        return all(
            await asyncio.gather(
                *(
                    asyncio.create_task(is_token_available(t, context, loading_context))
                    for t in token.value.values()
                )
            )
        )
    if isinstance(token, JobToken):
        return False
    return True


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


def add_elem_dictionary(key, elem, dictionary):
    if key not in dictionary.keys():
        dictionary[key] = set()
    dictionary[key].add(elem)


def get_token_by_tag(token_tag, token_list):
    for token in token_list:
        if token_tag == token.tag:
            return token
    return None


def get_job_token_from_visited(job_name, token_visited):
    for token, _ in token_visited.values():
        if isinstance(token, JobToken):
            if token.value.name == job_name:
                return token
    raise Exception(f"Job {job_name} not found in token_visited")


def contains_token_id(token_id, token_list):
    for token in token_list:
        if token_id == token.persistent_id:
            return True
    return False


class JobRequest:
    def __init__(self):
        self.job_token: JobToken = None
        self.token_output: MutableMapping[str, Token] = {}
        self.lock = asyncio.Condition()
        self.is_running = True


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
        self, token, token_visited, available_new_job_tokens, context, loading_context
    ):
        if isinstance(token, JobToken) and token.value.name in self.job_requests.keys():
            async with self.job_requests[token.value.name].lock:
                if self.job_requests[token.value.name].is_running:
                    print(
                        f"Il job {token.value.name} {token.persistent_id} - è running, dovrei aspettare il suo output"
                    )
                elif (
                    self.job_requests[token.value.name].job_token
                    and token.persistent_id
                    != self.job_requests[token.value.name].job_token.persistent_id
                    and self.job_requests[token.value.name].token_output
                ):
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
                        print("Il job", token.value.name, token.persistent_id, " - port_json", dict(port_json))
                        disp = await is_token_available(
                            # list(self.job_requests[token.value.name].token_output.values())[
                            #     0
                            # ],
                            self.job_requests[token.value.name].token_output[port_json["name"]],
                            context,
                            loading_context,
                        )
                        print(
                            f"Il job {token.value.name} {token.persistent_id} - il token di output {self.job_requests[token.value.name].token_output[port_json['name']]} è già disponibile? {disp}"
                        )
                    else:
                        print(
                            f"Il job {token.value.name} {token.persistent_id} - impossibile recuperare il token perché non so quale porta è: {self.job_requests[token.value.name].token_output} "
                        )
                    pass
        return False
        if isinstance(token, JobToken) and token.value.name in self.job_requests.keys():
            async with self.job_requests[token.value.name].lock:
                if self.job_requests[token.value.name].is_running:
                    # se il job è ancora running, faccio finta di avere il token
                    # ma appena finisco di costruire il dag, mi metto in attesa
                    # e dopo che viene prodotto vado a recuperare il nuovo token
                    print(
                        f"Il job {token.value.name} è running .... TODO: ancora da gestire"
                    )
                    pass
                    return True
                elif (
                    self.job_requests[token.value.name].job_token
                    and token.persistent_id
                    != self.job_requests[token.value.name].job_token.persistent_id
                    and self.job_requests[token.value.name].token_output
                    and await is_token_available(
                        self.job_requests[token.value.name].token_output,
                        context,
                        loading_context,
                    )
                ):
                    # il token è stato prodotto da qualche job recovered
                    # ed è disponibile
                    token_1 = self.job_requests[token.value.name].job_token
                    print("il token è disponibile - token_1", token_1)
                    token_visited[token.persistent_id] = (token, False)
                    token_visited[token_1.persistent_id] = (token_1, True)
                    available_new_job_tokens[
                        token.persistent_id
                    ] = token_1.persistent_id
                    return True
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

    async def _build_dag(
        self, tokens, failed_job, failed_step, workflow, loading_context
    ):
        dag_tokens = {}  # { token.id : set of next tokens' id | string }
        for t in failed_job.inputs.values():
            dag_tokens[t.persistent_id] = set((failed_step.name,))

        token_visited = {}  # { token_id: (token, is_available)}
        available_new_job_tokens = {}  # {old token id : new token id}

        while tokens:
            token = tokens.pop()

            if not await self._is_available(
                token,
                token_visited,
                available_new_job_tokens,
                workflow.context,
                loading_context,
            ):
                is_available = await is_token_available(
                    token, workflow.context, loading_context
                )

                # impossible case because when added in tokens, the elem is checked
                if token.persistent_id in token_visited.keys():
                    raise FailureHandlingException(
                        f"Token {token.persistent_id} already visited"
                    )
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
                        add_elem_dictionary(
                            INIT_DAG_FLAG, token.persistent_id, dag_tokens
                        )
                else:
                    add_elem_dictionary(INIT_DAG_FLAG, token.persistent_id, dag_tokens)
                # alternativa ai due else ... però è più difficile la lettura del codice (ancora da provare)
                # if is_available or not prev_tokens:
                #     add_elem_dictionary(INIT_DAG_FLAG, token.persistent_id, dag_tokens)

        token_visited = dict(sorted(token_visited.items()))
        await printa_token(
            token_visited,
            workflow,
            dag_tokens,
            loading_context,
            "prima-e-dopo/"
            + str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
            + "_graph_steps_recovery",
        )
        # print_graph_figure(dag_tokens, "graph_token_recovery")
        pass

        # replace in the graph the old token output with the new
        # { token output id of new job : { tokens output is of old job } }
        # replace_token_outputs = {}
        #
        # remove_tokens = set()
        # for k, v in available_new_job_tokens.items():
        #     # recovery token generated by the new job
        #     port_token = self.job_requests[token_visited[v][0].value.name].token_output
        #     try:
        #         _ = port_token.persistent_id
        #     except:
        #         pass
        #     token_visited[t.persistent_id] = (t, True)
        #     pass
        #     # save subsequent tokens, before deleting the token
        #     replace_token_outputs[t.persistent_id] = {
        #         t2 for t1 in dag_tokens[k] for t2 in dag_tokens[t1]
        #     }
        #     # replace_token_outputs[t.persistent_id] = set()
        #     # for t1 in dag_tokens[k]:
        #     #     for t2 in dag_tokens[t1]:
        #     #         replace_token_outputs[t.persistent_id].add(t2)
        #     pass
        #     # add to the list the tokens to be removed in the graph
        #     for t in dag_tokens[k]:
        #         remove_tokens.add(t)
        #
        # # remove token in the graph
        # d = self._remove_token({k: v for k, v in dag_tokens.items()}, remove_tokens)
        #
        # # add old token dependencies but with the new token
        # for k, vals in replace_token_outputs.items():
        #     d[k] = {v for v in vals if v not in remove_tokens}
        #
        # # add new tokens in init
        # for v in available_new_job_tokens.values():
        #     d[INIT_DAG_FLAG].add(
        #         self.job_requests[
        #             token_visited[v][0].value.name
        #         ].token_output.persistent_id
        #     )
        # pass
        # await printa_token(
        #     token_visited,
        #     workflow,
        #     d,
        #     loading_context,
        #     "prima-e-dopo/"
        #     + str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
        #     + "_graph_steps_recovery_d",
        # )
        # # print_graph_figure(d, "graph_token_d")
        pass
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
            tokens, failed_job, failed_step, workflow, loading_context
        )

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
        # try:
        #     await executor.run()
        # except Exception as err:
        #     logger.exception(
        #         f"Sub workflow {new_workflow.name} to handle the failed job {failed_job.name} throws a exception."
        #     )
        #     raise err
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
                    add_elem_dictionary(
                        port.name,
                        token_visited[t_id][0],
                        self.retags[new_workflow.name],
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
            jt = new_step.get_input_port('__job__').token_list
            logger.error(f"FAILED Job {new_job.name} {get_job_token(new_job.name, jt if isinstance(jt, Iterable) else [jt]).persistent_id} with error:\n\t{cmd_out.value}")
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
            status = await self._execute_transfer_step(step, new_workflow)
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
        print(
            failed_step.name, "ci sono?", failed_step.name in new_workflow.steps.keys()
        )
        print(
            failed_step.name,
            "fs.old",
            {k: t.token_list for k, t in failed_step.get_output_ports().items()},
        )
        print(
            failed_step.name,
            "fs.new",
            {
                k: t.token_list
                for k, t in new_workflow.steps[failed_step.name]
                .get_output_ports()
                .items()
            },
        )

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
        for _, port in new_workflow.steps[failed_step.name].get_output_ports().items():
            for t in port.token_list:
                if isinstance(t, TerminationToken):
                    print(
                        failed_step.name,
                        "Ha già un termination token........Questo approccio non va bene",
                    )

        for k, port in new_workflow.steps[failed_step.name].get_output_ports().items():
            if len(port.token_list) < 3:
                print(
                    failed_step.name,
                    "ALLARMEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
                )
            for t in port.token_list:
                print(failed_step.name, f"Inserisco token {t} in port {k}({port.name})")
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
