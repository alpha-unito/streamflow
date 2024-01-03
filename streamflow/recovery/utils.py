from __future__ import annotations


import asyncio
import json
import logging
import posixpath
from typing import MutableMapping, Tuple, MutableSequence, Collection

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.core.utils import get_class_fullname, get_class_from_name
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import (
    FailureHandlingException,
)
from streamflow.core.workflow import Token, Port, Step


from streamflow.cwl import utils
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.transformer import (
    BackPropagationTransformer,
    OutputForwardTransformer,
)
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.workflow.combinator import LoopTerminationCombinator
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.port import JobPort, ConnectorPort
from streamflow.workflow.step import ExecuteStep, InputInjectorStep, LoopOutputStep
from streamflow.workflow.token import (
    JobToken,
    ListToken,
    ObjectToken,
)

INIT_DAG_FLAG = "init"
TOKEN_WAITER = "twaiter"

# todo: spostare alcuni metodi in altri file esempio
#  - get_token_by_tag forse meglio in utils core?
#  - get_input_ports in persistence.utils?
#    oppure cambiare query ritornando le row delle ports


async def get_input_ports(step_id, context):
    return await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_port(dep_row["port"]))
            for dep_row in await context.database.get_input_ports(step_id)
        )
    )


async def get_output_ports(step_id, context):
    return await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_port(dep_row["port"]))
            for dep_row in await context.database.get_output_ports(step_id)
        )
    )


def get_files_from_token(token: Token) -> MutableSequence[str]:
    if isinstance(token, CWLFileToken):
        return [token.value["path"]]
    if isinstance(token, ListToken):
        return [
            file
            for inner_token in token.value
            for file in get_files_from_token(inner_token)
        ]
    if isinstance(token, ObjectToken):
        return [
            file
            for inner_token in token.value.values()
            for file in get_files_from_token(inner_token)
        ]
    if isinstance(token.value, Token):
        return get_files_from_token(token.value)
    return []


async def get_steps_from_output_port(port_id, context):
    step_id_rows = await context.database.get_steps_from_output_port(port_id)
    return await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_step(step_id_row["step"]))
            for step_id_row in step_id_rows
        )
    )


async def get_execute_step_out_token_ids(next_token_ids, context):
    execute_step_out_token_ids = set()
    for t_id in next_token_ids:
        if t_id > 0:
            port_row = await context.database.get_port_from_token(t_id)
            for step_id_row in await context.database.get_steps_from_output_port(
                port_row["id"]
            ):
                step_row = await context.database.get_step(step_id_row["step"])
                if step_row["type"] == get_class_fullname(ExecuteStep):
                    execute_step_out_token_ids.add(t_id)
        else:
            execute_step_out_token_ids.add(t_id)
    return execute_step_out_token_ids


async def _cleanup_dir(
    connector: Connector, location: Location, directory: str
) -> None:
    await remotepath.rm(
        connector, location, await remotepath.listdir(connector, location, directory)
    )


def increase_tag(tag):
    tag_list = tag.rsplit(".", maxsplit=1)
    if len(tag_list) == 1:
        return None
    elif len(tag_list) == 2:
        return ".".join((tag_list[0], str(int(tag_list[1]) + 1)))
    return None


def get_prev_vertices(searched_vertex, dag):
    prev_vertices = set()
    for vertex, next_vertices in dag.items():
        if searched_vertex in next_vertices and vertex != INIT_DAG_FLAG:
            prev_vertices.add(vertex)
    return prev_vertices


def contains_class(class_t, object_instances):
    for instance in object_instances:
        if issubclass(class_t, type(instance)):
            return True
    return False


def search_from_values(value, dictionary):
    for k, v in dictionary.items():
        if value == v:
            return k
    return None


def get_token_by_tag(token_tag, token_list):
    for token in token_list:
        if token_tag == token.tag:
            return token
    return None


async def _is_file_available(data_location, context):
    connector = context.deployment_manager.get_connector(data_location.deployment)
    if not (
        res := await remotepath.exists(connector, data_location, data_location.path)
    ):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Invalidated location {data_location.deployment} (Lost path {data_location.path})"
            )
        context.data_manager.invalidate_location(data_location, "/")
    return res


async def _is_token_available(token: Token, context: StreamFlowContext, valid_data):
    if isinstance(token, JobToken):
        return False
    elif isinstance(token, CWLFileToken):
        data_locs = []
        token_path = utils.get_path_from_token(token.value)
        for data_loc in context.data_manager.get_data_locations(token_path):
            if data_loc.data_type == DataType.PRIMARY and token_path not in valid_data:
                data_locs.append(data_loc)
        for data_loc, is_avai in zip(
            data_locs,
            await asyncio.gather(
                *(
                    asyncio.create_task(_is_file_available(data_loc, context))
                    for data_loc in data_locs
                )
            ),
        ):
            if is_avai:
                valid_data.add(data_loc.path)
    return await token.is_available(context)


def get_necessary_tokens(
    port_tokens, all_token_visited
) -> MutableMapping[int, Tuple[Token, bool]]:
    d = {
        t_id: all_token_visited[t_id]
        for token_list in port_tokens.values()
        for t_id in token_list
        if t_id != TOKEN_WAITER
    }
    return dict(sorted(d.items()))


def is_next_of_someone(p_name, dag_ports):
    for port_name, next_port_names in dag_ports.items():
        if port_name != INIT_DAG_FLAG and p_name in next_port_names:
            return True
    return False


async def is_output_port_forward(port_id, context):
    step_rows = await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_step(dep_row["step"]))
            for dep_row in await context.database.get_input_steps(port_id)
        )
    )
    result = False
    for step_row in step_rows:
        result = result or issubclass(
            get_class_from_name(step_row["type"]), BackPropagationTransformer
        )
    return result


async def is_input_port_forward(port_id, context):
    step_rows = await asyncio.gather(
        *(
            asyncio.create_task(context.database.get_step(dep_row["step"]))
            for dep_row in await context.database.get_output_steps(port_id)
        )
    )
    result = False
    for step_row in step_rows:
        result = result or issubclass(
            get_class_from_name(step_row["type"]), BackPropagationTransformer
        )
    return result


def get_port_from_token(token, port_tokens, token_visited):
    for port_name, token_ids in port_tokens.items():
        if token.tag in (token_visited[t_id][0].tag for t_id in token_ids):
            return port_name
    raise FailureHandlingException("Token assente")


def get_key_by_value(
    searched_value: int, dictionary: MutableMapping[str, Collection[int]]
):
    for key, values in dictionary.items():
        if searched_value in values:
            return key
    raise FailureHandlingException(
        f"Searched value {searched_value} not found in {dictionary}"
    )


def get_recovery_loop_expression(upper_limit):
    return f"$(inputs.index < {upper_limit})"


def get_last_token(token_list):
    for token in token_list[::-1]:
        if not isinstance(token, (IterationTerminationToken, TerminationToken)):
            return token
    return None


def get_value(elem, dictionary):
    for k, v in dictionary.items():
        if v == elem:
            return k
    raise Exception("Value not found in dictionary")


async def _execute_recovered_workflow(new_workflow, step_name, output_ports):
    if not new_workflow.steps.keys():
        logger.info(
            f"Workflow {new_workflow.name} is empty. Waiting output ports {[p.name for p in new_workflow.ports.values() if not isinstance(p, (JobPort, ConnectorPort))]}"
        )

        # for debug. Versione corretta quella con la gather
        for p in new_workflow.ports.values():
            if not isinstance(p, (JobPort, ConnectorPort)):
                await p.get(posixpath.join(step_name, get_value(p.name, output_ports)))
        # await asyncio.gather(
        #     *(
        #         asyncio.create_task(
        #             p.get(posixpath.join(step_name, get_value(p.name, output_ports)))
        #         )
        #         for p in new_workflow.ports.values()
        #         if not isinstance(p, (JobPort, ConnectorPort))
        #     )
        # )
        logger.info(f"Workflow {new_workflow.name}: Port terminated")
    else:
        await new_workflow.save(new_workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        await executor.run()
        logger.info(f"COMPLETED Workflow execution {new_workflow.name}")


async def load_and_add_ports(port_ids, new_workflow, loading_context):
    for port in await asyncio.gather(
        *(
            asyncio.create_task(
                Port.load(
                    new_workflow.context,
                    port_id,
                    loading_context,
                    new_workflow,
                )
            )
            for port_id in port_ids
        )
    ):
        if port.name not in new_workflow.ports.keys():
            new_workflow.add_port(port)
            logger.debug(
                f"populate_workflow: wf {new_workflow.name} add_1 port {port.name}"
            )
        else:
            logger.debug(
                f"populate_workflow: La port {port.name} è già presente nel workflow {new_workflow.name}"
            )
    logger.debug("populate_workflow: Port caricate")


async def load_and_add_steps(step_ids, new_workflow, wr, loading_context):
    new_step_ids = set()
    step_name_id = {}
    for sid, step in zip(
        step_ids,
        await asyncio.gather(
            *(
                asyncio.create_task(
                    Step.load(
                        new_workflow.context,
                        step_id,
                        loading_context,
                        new_workflow,
                    )
                )
                for step_id in step_ids
            )
        ),
    ):
        logger.debug(f"Loaded step {step.name} (id {sid})")
        step_name_id[step.name] = sid

        # if there are not the input ports in the workflow, the step is not added
        if not (set(step.input_ports.values()) - set(new_workflow.ports.keys())):
            # removesuffix python 3.9
            if isinstance(step, CWLLoopConditionalStep) and (
                wr.external_loop_step_name.removesuffix("-recovery")
                == step.name.removesuffix("-recovery")
            ):
                if not wr.external_loop_step:
                    wr.external_loop_step = step
                else:
                    continue
            elif isinstance(step, OutputForwardTransformer):
                port_id = min(wr.port_name_ids[step.get_output_port().name])
                for (
                    step_dep_row
                ) in await new_workflow.context.database.get_steps_from_input_port(
                    port_id
                ):
                    step_row = await new_workflow.context.database.get_step(
                        step_dep_row["step"]
                    )
                    if step_row["name"] not in new_workflow.steps.keys() and issubclass(
                        get_class_from_name(step_row["type"]), LoopOutputStep
                    ):
                        logger.debug(
                            f"Step {step_row['name']} from id {step_row['id']} will be added soon (2)"
                        )
                        new_step_ids.add(step_row["id"])
            elif isinstance(step, BackPropagationTransformer):
                # for port_name in step.output_ports.values(): # potrebbe sostituire questo for
                for (
                    port_dep_row
                ) in await new_workflow.context.database.get_output_ports(
                    step_name_id[step.name]
                ):
                    # if there are more iterations
                    if len(wr.port_tokens[step.output_ports[port_dep_row["name"]]]) > 1:
                        for (
                            step_dep_row
                        ) in await new_workflow.context.database.get_steps_from_output_port(
                            port_dep_row["port"]
                        ):
                            step_row = await new_workflow.context.database.get_step(
                                step_dep_row["step"]
                            )
                            if issubclass(
                                get_class_from_name(step_row["type"]), CombinatorStep
                            ) and issubclass(
                                get_class_from_name(
                                    json.loads(step_row["params"])["combinator"]["type"]
                                ),
                                LoopTerminationCombinator,
                            ):
                                logger.debug(
                                    f"Step {step_row['name']} from id {step_row['id']} will be added soon (1)"
                                )
                                new_step_ids.add(step_row["id"])
            logger.debug(
                f"populate_workflow: (1) Step {step.name} caricato nel wf {new_workflow.name}"
            )
            new_workflow.add_step(step)
        else:
            logger.debug(
                f"populate_workflow: Step {step.name} non viene essere caricato perché nel wf {new_workflow.name} mancano le ports {set(step.input_ports.values()) - set(new_workflow.ports.keys())}. It is present in the workflow: {step.name in new_workflow.steps.keys()}"
            )
    for sid, other_step in zip(
        new_step_ids,
        await asyncio.gather(
            *(
                asyncio.create_task(
                    Step.load(
                        new_workflow.context,
                        step_id,
                        loading_context,
                        new_workflow,
                    )
                )
                for step_id in new_step_ids
            )
        ),
    ):
        logger.debug(
            f"populate_workflow: (2) Step {other_step.name} (from step id {sid}) caricato nel wf {new_workflow.name}"
        )
        step_name_id[other_step.name] = sid
        new_workflow.add_step(other_step)
    logger.debug("populate_workflow: Step caricati")
    return step_name_id


async def load_missing_ports(new_workflow, step_name_id, loading_context):
    missing_ports = set()
    for step in new_workflow.steps.values():
        if isinstance(step, InputInjectorStep):
            continue
        for dep_name, p_name in step.output_ports.items():
            if p_name not in new_workflow.ports.keys():
                # problema nato dai loop. when-loop ha output tutti i param. Però il grafo è costruito sulla presenza o
                # meno dei file, invece i param str, int, ..., no. Quindi le port di questi param non vengono esplorate
                # le aggiungo ma durante l'esecuzione verranno utilizzati come "port pozzo" dei token prodotti
                logger.debug(
                    f"populate_workflow: Aggiungo port {p_name} al wf {new_workflow.name} perché è un output port dello step {step.name}"
                )
                depe_row = await new_workflow.context.database.get_output_port(
                    step_name_id[step.name], dep_name
                )
                missing_ports.add(depe_row["port"])
    for port in await asyncio.gather(
        *(
            asyncio.create_task(
                Port.load(new_workflow.context, p_id, loading_context, new_workflow)
            )
            for p_id in missing_ports
        )
    ):
        logger.debug(
            f"populate_workflow: wf {new_workflow.name} add_2 port {port.name}"
        )
        new_workflow.add_port(port)


#########################################################################
############################ debug ######################################
#########################################################################


import sys
import objsize
import datetime
from typing import Iterable, Any, cast
from streamflow.persistence import SqliteDatabase
from streamflow.workflow.token import (
    IterationTerminationToken,
    TerminationToken,
)
from streamflow.cwl.step import (
    CWLBaseConditionalStep,
    CWLLoopConditionalStep,
    CWLRecoveryLoopConditionalStep,
)
from streamflow.token_printer import dag_workflow
from streamflow.core.workflow import Workflow

from streamflow.workflow.step import CombinatorStep
from streamflow.cwl.transformer import CWLTokenTransformer

from streamflow.cwl.processor import CWLTokenProcessor


def extra_data_print(
    workflow,
    new_workflow,
    job_executed_in_new_workflow,
    token_visited,
    last_iteration,
):
    if not workflow:
        workflow = Workflow(
            new_workflow.context, new_workflow.type, new_workflow.config, "None"
        )
    if not job_executed_in_new_workflow:
        job_executed_in_new_workflow = []
        for t, _ in token_visited.values():
            if isinstance(t, JobToken):
                job_executed_in_new_workflow.append(t.value.name)
    dt = str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
    dir_path = f"graphs/{dt}-{new_workflow.name}"

    print(
        f"\nNew workflow {new_workflow.name} si occupa di recuperare l'esecuzione del workflow {workflow.name}",
        file=sys.stderr,
    )
    sorted_jobs = list(job_executed_in_new_workflow)
    sorted_jobs.sort()
    print(
        f"\t- Number of {len(new_workflow.steps.keys())} steps and {len(new_workflow.ports.keys())} ports",
        file=sys.stderr,
    )
    print(f"\t- Jobs to re-execute: {sorted_jobs}", file=sys.stderr)
    print(f"Init ports in last_iteration are: {last_iteration}", file=sys.stderr)
    dag_workflow(new_workflow, dir_path + "/new-wf")
    for step in new_workflow.steps.values():
        try:
            print(
                f"\t- Step {step.name} (wf {step.workflow.name})"
                f"\n\t\t* input tokens: { {k_p: [(str_id(t), t.tag) for t in port.token_list] for k_p, port in step.get_input_ports().items()} }",
                f"\n\t\t* dependency names: { {k: v.name for k, v in step.get_input_ports().items()} }",
                f"\n\t\t* skip ports: { {k: v.name for k, v in step.get_skip_ports().items()} }"
                if isinstance(step, CWLBaseConditionalStep)
                else "",
                file=sys.stderr,
            )
        except Exception as e:
            print(f"exception {step.name} -> {e}", file=sys.stderr)
            raise

    # PROBLEMA: Ci sono troppi when-recovery step
    loop_cond = [
        s
        for s in new_workflow.steps.values()
        if isinstance(s, (CWLLoopConditionalStep, CWLRecoveryLoopConditionalStep))
    ]
    if (a := len(loop_cond)) > 1:
        raise FailureHandlingException(f"Ci sono troppi LoopConditionalStep: {a}")

    # PROBLEMA: c'è uno step che non dovrebbe essere caricato
    if "/subworkflow/i1-back-propagation-transformer" in new_workflow.steps.keys():
        raise FailureHandlingException("Caricata i1-back-prop CHE NON SERVE")

    # INFO: ci sarà una iterazione precedente
    # "/subworkflow-loop-terminator" in new_workflow.steps.keys()
    if "/subworkflow-loop-terminator" in new_workflow.steps.keys():
        print("There will be a/some prev iteration/s", file=sys.stderr)
        pass
    # print_wr_size(wr)
    # print_wf_size(new_workflow)
    # print_context_size(new_workflow.context)
    # print_cached_db_size(new_workflow)
    # for s in new_workflow.steps.values():
    #     if (
    #         isinstance(s, CWLTokenTransformer)
    #         and isinstance(s.processor, CWLTokenProcessor)
    #         and s.processor.format_graph
    #     ):
    #         logger.debug(f"Graph: {s.processor.format_graph.serialize()}")
    #         break
    logger.debug(f"Running workflow {new_workflow.name}")


def str_id(token):
    if token.persistent_id:
        return token.persistent_id
    if isinstance(token, IterationTerminationToken):
        return "itt"
    if isinstance(token, TerminationToken):
        return "t"
    return "un"


def convert_to_json(dictionary: MutableMapping[Any, Iterable[Any]]):
    return json.dumps(
        dict(sorted({k: list(v) for k, v in dictionary.items()}.items())),
        indent=2,
    )


def str_tok(token):
    if isinstance(token, JobToken):
        return token.value.name
    elif isinstance(token, CWLFileToken):
        return token.value["path"]
    else:
        return token.value


# debug
def check_double_reference(dag_ports):
    for tmpp in dag_ports[INIT_DAG_FLAG]:
        for tmpport_name, tmpnext_port_names in dag_ports.items():
            if tmpport_name != INIT_DAG_FLAG and tmpp in tmpnext_port_names:
                msg = f"Port {tmpp} appartiene sia a INIT che a {tmpport_name}"
                print("WARN", msg, file=sys.stderr)
                # print("OOOOOOOOOOOOOOOOOOOOOOOOOOOO" * 100, "\n", msg)
                # raise FailureHandlingException(msg)


def get_size_h(obj):
    return convert_bytes(objsize.get_deep_size(obj))


def convert_bytes(size):
    for x in ["bytes", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return "%3.1f %s" % (size, x)
        size /= 1024.0
    return size


def print_context_size(context):
    context_attrs = {
        attr: getattr(context, attr)
        for attr in dir(context)
        if not attr.startswith("__") and not callable(getattr(context, attr))
    }
    for k, v in context_attrs.items():
        if k != "process_executor" and k != "config":
            v.context = None
    tmp_deployment = context.deployment_manager.deployments_map.get("__LOCAL__", None)
    if tmp_deployment:
        tmp_deployment.config_dir = None
    print(
        f"context_size: {get_size_h(context)}\n",
        "\n".join([f"\t- {k}: {get_size_h(v)}" for k, v in context_attrs.items()]),
        file=sys.stderr,
    )
    for k, v in context_attrs.items():
        if k != "process_executor" and k != "config":
            v.context = context
    tmp_deployment.config_dir = context


def print_cached_db_size(new_workflow):
    db = cast(SqliteDatabase, new_workflow.context.database)
    dictionary = {
        "port_cache": db.port_cache,
        "step_cache": db.step_cache,
        "token_cache": db.token_cache,
        "deployment_cache": db.deployment_cache,
        "target_cache": db.target_cache,
        "workflow_cache": db.workflow_cache,
    }
    curr_size = 0
    max_size = 0
    curr_size_0 = 0
    curr_size_1 = 0
    curr_objsize = 0
    # lrucache_maintenance = 0
    for obj in dictionary.values():
        curr_size += obj.currsize
        max_size += obj.maxsize
        for row in obj.values():
            for k, v in zip(row.keys(), row):
                curr_size_0 += get_size_0(k) + get_size_0(v)
                # curr_size_1 += get_size_1(k) + get_size_1(v)
                curr_size_1 = -1
                curr_objsize += objsize.get_deep_size(k) + objsize.get_deep_size(v)
    print(
        "Cached database size:\n",
        "\n".join(
            [
                f"\t- {k}: {convert_bytes(v.currsize)}/{convert_bytes(v.maxsize)}"
                f"\n\t\t* objsize: {convert_bytes(sum(objsize.get_deep_size(vv) for vv in v.values())):<10} | "
                f"size_0: {convert_bytes(sum(get_size_0(vv) for vv in v.values())):<10} | "
                # f"size_1: {convert_bytes(sum(get_size_1(vv) for vv in v.values())):<10}"
                f"\n\t\t* lrucache maintenance cost: {convert_bytes(objsize.get_deep_size(v) - sum(objsize.get_deep_size(vv) for vv in v.values()))}"
                for k, v in dictionary.items()
            ]
        ),
        f"\n\ttotal curr on total max: {convert_bytes(curr_size)} on "
        f"{convert_bytes(max_size)}"
        f"\n\t\t* objsize: {convert_bytes(curr_objsize):<10} | "
        f"size_0: {convert_bytes(curr_size_0):<10} | "
        f"size_1: {convert_bytes(curr_size_1):<10}",
        file=sys.stderr,
    )
    # print(
    #     "Cached database size:\n",
    #     "\n".join(
    #         [
    #             f"\t- {k}: {convert_bytes(v.currsize)}/{convert_bytes(v.maxsize)}"
    #             f"\n\t\t* objsize: {get_size_h(v):<10} | size_0: {convert_bytes(get_size_0(v)):<10} | size_1: {convert_bytes(get_size_1(v)):<10}"
    #             for k, v in dictionary.items()
    #         ]
    #     ),
    #     f"\n\ttotal curr on total max: {convert_bytes(sum([v.currsize for v in dictionary.values()]))} on "
    #     f"{convert_bytes(sum([v.maxsize for v in dictionary.values()]))}"
    #     f"\n\t\t* objsize: {convert_bytes(sum([sum(objsize.get_deep_size(k) + objsize.get_deep_size(v) for k, v in obj.items()) for obj in dictionary.values()])):<10} | "
    #     f"size_0: {convert_bytes(sum([sum(get_size_0(k) + get_size_0(v) for k, v in obj.items()) for obj in dictionary.values()])):<10} | "
    #     f"size_1: {convert_bytes(sum([sum(get_size_1(k) + get_size_1(v) for k, v in obj.items()) for obj in dictionary.values()])):<10}",
    #     file=sys.stderr,
    # )
    pass


def print_wr_size(wr):
    tmp_context = wr.context
    tmp_step = wr.external_loop_step
    attrs = {
        attr: getattr(wr, attr)
        for attr in dir(wr)
        if not attr.startswith("__") and not callable(getattr(wr, attr))
    }
    wr.context = None
    wr.external_loop_step = None
    print(
        f"ProvenanceGraphNavigation size: {get_size_h(wr)}\n",
        "\n".join([f"\t- {k}: {get_size_h(v)}" for k, v in attrs.items()]),
        file=sys.stderr,
    )
    wr.context = tmp_context
    wr.external_loop_step = tmp_step


def print_wf_size(
    wf: Workflow, deeper_print=False, print_graph=False, print_step_attr=False
):
    for p in wf.ports.values():
        p.workflow = None
    for s in wf.steps.values():
        s.workflow = None
        if isinstance(s, CWLTokenTransformer):
            s.processor.workflow = None
        elif isinstance(s, CombinatorStep):
            s.combinator.workflow = None
        elif isinstance(s, ExecuteStep):
            for v in s.output_processors.values():
                v.workflow = None
    tmp_context = wf.context
    wf.context = None
    print(
        f"Workflow size: {get_size_h(wf)}\n\t- steps size: {get_size_h(wf.steps)}",
        file=sys.stderr,
    )
    wf.context = tmp_context
    max_len_step_name = max(len(s) for s in wf.steps.keys())
    max_len_step_type = max(len(get_class_fullname(type(s))) for s in wf.steps.values())
    for s in wf.steps.values():
        if deeper_print:
            size = get_size_h(s)
            print(
                f"\t\t* {s.name:<{max_len_step_name}} | {get_class_fullname(type(s)):^{max_len_step_type}} | size: {size:<15}{'| warn. it is a large obj' if 'MB' in size or 'GB' in size else ''}",
                file=sys.stderr,
            )
        if print_graph and isinstance(s, CWLTokenTransformer):
            print(f"\t\t\t** .processor size: {get_size_h(s.processor)}")
            if isinstance(s.processor, CWLTokenProcessor):
                print(
                    f"\t\t\t\t*** .format_graph size: {get_size_h(s.processor.format_graph)}"
                )
        if print_step_attr:
            attrs = {
                attr: getattr(s, attr)
                for attr in dir(s)
                if not attr.startswith("__") and not callable(getattr(s, attr))
            }
            print(
                "\n".join([f"\t- {k}: {get_size_h(v)}" for k, v in attrs.items()]),
                file=sys.stderr,
            )
    print(f"\t- ports size: {get_size_h(wf.ports)}", file=sys.stderr)
    max_len_port_type = max(len(get_class_fullname(type(p))) for p in wf.ports.values())
    for p in wf.ports.values():
        if deeper_print:
            print(
                f"\t\t* {p.name} | {get_class_fullname(type(p)):^{max_len_port_type}} | size: {get_size_h(p)}",
                file=sys.stderr,
            )
    for p in wf.ports.values():
        p.workflow = wf
    for s in wf.steps.values():
        s.workflow = wf
        if isinstance(s, CWLTokenTransformer):
            s.processor.workflow = wf
        elif isinstance(s, CombinatorStep):
            s.combinator.workflow = wf
        elif isinstance(s, ExecuteStep):
            for v in s.output_processors.values():
                v.workflow = wf


def get_size_with_comparisons(obj):
    size_0 = 0
    size_1 = 0
    size_2 = 0
    for k, v in zip(obj.keys(), obj):
        if not isinstance(k, (int, str, bool)):
            print(
                f"k | type k: {type(k)} | type v: {type(v)}\n\t- {f'val k: {k}, val v: {v}' if k != 'params' else f'val k (params): {k}, val compressed v: {v[:10]}'}\n\t- size k: {sys.getsizeof(k)} size v: {sys.getsizeof(v)}"
            )
        if v and not isinstance(v, (int, str, bool)):
            print(
                f"v | type k: {type(k)} | type v: {type(v)}\n\t- {f'val k: {k}, val v: {v}' if k != 'params' else f'val k (params): {k}, val compressed v: {v[:10]}'}\n\t- size k: {sys.getsizeof(k)} size v: {sys.getsizeof(v)}"
            )
        size_0 += get_size_0(k) + get_size_0(v)
        # size_1 += get_size_1(k) + get_size_1(v)
        size_1 = -1
        size_2 += objsize.get_deep_size(k) + objsize.get_deep_size(v)
    if max(size_0, size_1, size_2) != size_1:
        print(
            f"{'size_0(k+v)':<10} | {'size_1(k+v)':<10}\n"
            f"\n{size_0:<10} | {size_1:<10} | {size_2:<10}\n"
        )
        raise Exception("Size_1 non è il più grande")
    return size_1


def get_size_0(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size_0(v, seen) for v in obj.values()])
        size += sum([get_size_0(k, seen) for k in obj.keys()])
    elif hasattr(obj, "__dict__"):
        size += get_size_0(obj.__dict__, seen)
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size_0(i, seen) for i in obj])
    return size
