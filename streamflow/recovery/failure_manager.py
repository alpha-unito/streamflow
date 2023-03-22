from __future__ import annotations

import posixpath
import re
import asyncio
import logging
import os
import itertools
from asyncio import Lock
from typing import MutableMapping, MutableSequence, cast

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
from streamflow.workflow.step import ExecuteStep
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


async def _load_steps_from_token(token, context, loading_context):
    # TODO: quando interrogo sulla tabella dependency (tra step e port) meglio recuperare anche il nome della dipendenza
    # così posso associare subito token -> step e a quale porta appartiene
    # edit. Impossibile. Recuperiamo lo step dalla porta di output. A noi serve la port di input
    row_token = await context.database.get_token(token.persistent_id)
    steps = []
    if row_token:
        row_steps = await context.database.get_step_from_output_port(row_token["port"])
        for r in row_steps:
            steps.append(await loading_context.load_step(context, r["step"]))
    return steps


async def _load_ports_from_token(token, context, loading_context):
    ports_id = await context.database.get_token_ports(token.persistent_id)
    # TODO: un token ha sempre una port? Da verificare
    if ports_id:
        return await asyncio.gather(
            *(
                asyncio.create_task(loading_context.load_port(context, port_id))
                for port_id in ports_id
            )
        )
    return None


async def data_location_exists(data_locations, context, token):
    for data_loc in data_locations:
        connector = context.deployment_manager.get_connector(data_loc.deployment)
        # location_allocation = job_version.step.workflow.context.scheduler.location_allocations[data_loc.deployment][data_loc.name]
        # available_locations = job_version.step.workflow.context.scheduler.get_locations(location_allocation.jobs[0])
        if exists := await remotepath.exists(connector, data_loc, token.value["path"]):
            return True
        print(
            f"t {token.persistent_id} ({get_class_fullname(type(token))}) in loc {data_loc} -> exists {exists} "
        )
    return False


def add_step(step, steps):
    found = False
    for s in steps:
        found = found or s.name == step.name
    if not found:
        steps.append(step)


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

    for k, v in graph.items():
        print(f"{k}: {v}")

    graph_steps = {}
    steps = []
    steps_name = set()
    for k, values in graph.items():
        if k != -1:
            k_step = (
                await _load_steps_from_token(
                    tokens[k], job_version.step.workflow.context, loading_context
                )
            ).pop()
            add_step(k_step, steps)
            steps_name.add(k_step.name)
        step_name = k_step.name if k != -1 else "None"
        if step_name not in graph_steps.keys():
            graph_steps[step_name] = set()
        for v in values:
            s = (
                await _load_steps_from_token(
                    tokens[v], job_version.step.workflow.context, loading_context
                )
            ).pop()
            graph_steps[step_name].add(s.name)
            add_step(s, steps)
            steps_name.add(s.name)
    steps_name = sorted(steps_name)
    wf_steps = sorted(job_version.step.workflow.steps.keys())
    pass


async def is_token_available(token, context):
    if isinstance(token, CWLFileToken):
        return await data_location_exists(
            context.data_manager.get_data_locations(token.value["path"]),
            context,
            token,
        )
    if isinstance(token, ListToken):
        # if at least one file doesn't exist, returns false
        return all(
            await asyncio.gather(
                *(
                    asyncio.create_task(is_token_available(inner_token, context))
                    for inner_token in token.value
                )
            )
        )
    if isinstance(token, ObjectToken):
        # TODO: sistemare
        return True
    if isinstance(token, JobToken):
        return False
    return True


async def _load_and_add_port(token, context, loading_context, curr_dict):
    ports = await _load_ports_from_token(token, context, loading_context)
    # TODO: ports, in teoria, è sempre solo una. Sarebbe la port di output dello step che genera il token
    for p in ports:
        if p.name not in curr_dict.keys():
            curr_dict[p.name] = set()
        curr_dict[p.name].add(token)



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
        command_output = await self._replay_job(self.jobs[job.name])
        return command_output

    async def _recover_jobs(self, job_version, loading_context):
        # await print_graph(job_version, loading_context)

        tokens = set(job_version.job.inputs.values())  # tokens to check

        # the step is a ExecuteStep, thus it has the get_job_token method...
        # but, is it necessary add this job_token in the tokens list?
        # in the provenance there is not because the step is failed before the _persist_token call
        # evaluate it when will be created the new workflow
        job_token = job_version.step.get_job_token(job_version.job)
        tokens.add(job_token)
        steps_token = {job_version.step: tokens.copy()} # step to rollback
        token_visited = set()  # to break cyclic in token dependencies
        # tokens_step = {
        #     k: set((job_version.step,)) for k in job_version.job.inputs.values()
        # } # dict{ token : list of steps with token some input port }
        # tokens_step[job_token] = set((job_version.step,))

        while tokens:
            token = tokens.pop()
            token_visited.add(token)
            res = await is_token_available(token, job_version.step.workflow.context)
            if not res:
                steps = await _load_steps_from_token(
                    token, job_version.step.workflow.context, loading_context
                )
                # TODO: un token è generato da un solo step, quindi modificare il metodo affinche ritorna un solo step e non una lista (in sqlite.py cambiare il catchall in catchone)
                step = steps.pop()
                if step not in steps_token.keys():
                    steps_token[step] = set()

                prev_tokens = await _load_prev_tokens(
                    token.persistent_id,
                    loading_context,
                    job_version.step.workflow.context,
                )
                for pt in prev_tokens:
                    if pt not in token_visited:
                        tokens.add(pt)
                    steps_token[step].add(pt)
                    # if pt not in tokens_step.keys():
                    #     tokens_step[pt] = set()
                    # tokens_step[pt].add(step)
        return steps_token


    async def _replay_job(self, job_version: JobVersion) -> CommandOutput:
        job = job_version.job
        if self.max_retries is None or self.jobs[job.name].version < self.max_retries:
            # Update version
            self.jobs[job.name].version += 1
            try:
                loading_context = DefaultDatabaseLoadingContext()

                rollback_steps = await self._recover_jobs(job_version, loading_context)

                return None
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
