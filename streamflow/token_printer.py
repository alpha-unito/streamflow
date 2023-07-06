import asyncio
import os

import graphviz

from streamflow.core.workflow import Step, Token
from streamflow.cwl.token import CWLFileToken
from streamflow.workflow.token import (
    ListToken,
    TerminationToken,
    JobToken,
    IterationTerminationToken,
)

from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext

INIT_DAG_FLAG = "init"

GRAPH_PATH = "dev/"


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
    filepath = GRAPH_PATH + title + ".gv"
    dot.render(filepath)
    os.system("rm " + filepath)


def print_graph_figure_label(graph, title):
    dot = graphviz.Digraph(title)
    for vertex, neighbors in graph.items():
        dot.node(str(vertex))
        for n, l in neighbors:
            dot.edge(str(vertex), str(n), label=str(l))
    filepath = GRAPH_PATH + title + ".gv"
    dot.render(filepath)
    os.system("rm " + filepath)


def get_name_and_type(step_name_1, steps):
    return f"{step_name_1}\n{steps[step_name_1].__class__.__name__}"


async def check_and_load(dependency_actor, row, workflow, loading_context):
    return (
        await loading_context.load_token(workflow.context, row[dependency_actor])
        if row[dependency_actor]
        else -1
    )


async def print_all_provenance(workflow, loading_context):
    """
    FUNCTION FOR DEBUGGING
    """
    rows = await workflow.context.database.get_all_provenance()
    tokens = {}
    graph = {}
    steps = {}
    for row in rows:
        # dependee = (
        #     await loading_context.load_token(
        #         workflow.context, row["dependee"]
        #     )
        #     if row["dependee"]
        #     else -1
        # )
        # depender = (
        #     await loading_context.load_token(
        #         workflow.context, row["depender"]
        #     )
        #     if row["depender"]
        #     else -1
        # )
        dependee = await check_and_load("dependee", row, workflow, loading_context)
        depender = await check_and_load("depender", row, workflow, loading_context)

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
                    workflow.context,
                    loading_context,
                    workflow,
                )
            ).pop()
        step_name = k_step.name if k != -1 else INIT_DAG_FLAG
        steps[step_name] = k_step
        if step_name not in graph_steps.keys():
            graph_steps[step_name] = set()
        if step_name not in steps_token.keys():
            steps_token[step_name] = set()
        steps_token[step_name].add(k)

        for v in values:
            s = (
                await _load_steps_from_token(
                    tokens[v],
                    workflow.context,
                    loading_context,
                    workflow,
                )
            ).pop()
            graph_steps[step_name].add(s.name)
            steps[s.name] = s
            if s.name not in steps_token.keys():
                steps_token[s.name] = set()
            steps_token[s.name].add(v)

    valid_steps_graph = {}
    for step_name_1, steps_name in graph_steps.items():
        step_name_1_new = get_name_and_type(step_name_1, steps)
        valid_steps_graph[step_name_1_new] = []
        for step_name_2 in steps_name:
            step_name_2 = get_name_and_type(step_name_2, steps)
            for label in steps_token[step_name_1]:
                add_pair(
                    step_name_2,
                    str_token_value(tokens[label]), # + f"({label})",
                    valid_steps_graph[step_name_1_new],
                    tokens,
                )
    print_graph_figure(graph, "get_all_provenance_tokens")
    print_graph_figure_label(valid_steps_graph, "get_all_provenance_steps")
    #    wf_steps = sorted(workflow.steps.keys())
    pass


def str_token_value(token):
    if isinstance(token, CWLFileToken):
        return token.value["class"] + f"({token.persistent_id})" # token.value['path']
    if isinstance(token, ListToken):
        return str([str_token_value(t) for t in token.value]) + f"({token.persistent_id})"
    if isinstance(token, JobToken):
        return token.value.name + f"({token.persistent_id})"
    if isinstance(token, TerminationToken):
        return "T"
    if isinstance(token, IterationTerminationToken):
        return "IT"
    if isinstance(token, Token):
        if isinstance(token.value, Token):
            return "t(" + str_token_value(token.value) + f")({token.persistent_id})"
        else:
            return f"{token.value}({token.persistent_id})" # str(token.value)
    return "None"


def search_step_name_into_graph(graph_tokens):
    for tokens in graph_tokens.values():
        for s in tokens:
            if isinstance(s, str) and s != INIT_DAG_FLAG:
                return s
    raise Exception("Step name non trovato")


def add_graph_tuple(graph_steps, step_1, tuple):
    for s, l in graph_steps[step_1]:
        if s == tuple[0] and l == tuple[1]:
            return
    graph_steps[step_1].append(tuple)


async def _get_step_from_token(
    graph_tokens, token_visited, token_values, workflow, loading_context
):
    steps = {}
    # for token_id, (token, _) in token_visited.items():
    token_list = set()
    for k, v in graph_tokens.items():
        if isinstance(k, int):
            token_list.add(token_visited[k][0])
        for vv in v:
            if isinstance(vv, int):
                token_list.add(token_visited[vv][0])
    for token in token_list:
        token_id = token.persistent_id
        token_values[token_id] = str_token_value(token)
        steps[token_id] = (
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
    return steps


async def printa_token(
    token_visited,
    workflow,
    graph_tokens,
    loading_context,
    pdf_name="graph",
):
    token_values = {}
    steps = await _get_step_from_token(
        graph_tokens, token_visited, token_values, workflow, loading_context
    )
    token_values[INIT_DAG_FLAG] = INIT_DAG_FLAG
    step_name = search_step_name_into_graph(graph_tokens)
    token_values[step_name] = step_name

    graph_steps = {}
    for token_id, tokens_id in graph_tokens.items():
        step_1 = steps[token_id] if isinstance(token_id, int) else token_id
        if step_1 == INIT_DAG_FLAG:
            continue
        if step_1 not in graph_steps.keys():
            graph_steps[step_1] = []
        label = (
            str_token_value(token_visited[token_id][0]) # + f"({token_id})"
            if isinstance(token_id, int)
            else token_values[token_id]
        )
        for token_id_2 in tokens_id:
            step_2 = steps[token_id_2] if isinstance(token_id_2, int) else token_id_2

            add_graph_tuple(graph_steps, step_1, (step_2, label))
    print_graph_figure_label(graph_steps, pdf_name)


def add_elem_dictionary(key, elem, dictionary):
    if key not in dictionary.keys():
        dictionary[key] = set()
    dictionary[key].add(elem)


async def _load_prev_tokens(token_id, loading_context, context):
    rows = await context.database.get_dependees(token_id)

    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["dependee"]))
            for row in rows
        )
    )


def contains_token_id(token_id, token_list):
    for token in token_list:
        if token_id == token.persistent_id:
            return True
    return False


async def _build_dag(token_list, workflow, loading_context):
    loading_context = DefaultDatabaseLoadingContext()
    tokens = [t for t in token_list if not isinstance(t, TerminationToken)]
    dag_tokens = {}  # { token.id : set of next tokens' id | string }
    for t in tokens:
        dag_tokens[t.persistent_id] = set(("END",))
    token_visited = {}

    while tokens:
        token = tokens.pop()
        token_visited[token.persistent_id] = (token, True)
        prev_tokens = await _load_prev_tokens(
            token.persistent_id,
            loading_context,
            workflow.context,
        )
        if prev_tokens:
            for pt in prev_tokens:
                if not pt:
                    continue
                add_elem_dictionary(pt.persistent_id, token.persistent_id, dag_tokens)
                if (
                    pt.persistent_id not in token_visited.keys()
                    and not contains_token_id(pt.persistent_id, tokens)
                ):
                    tokens.append(pt)
        else:
            add_elem_dictionary(INIT_DAG_FLAG, token.persistent_id, dag_tokens)

    token_visited = dict(sorted(token_visited.items()))
    await printa_token(
        token_visited,
        workflow,
        dag_tokens,
        loading_context,
        "graph_step",
    )
    print_graph_figure(dag_tokens, "graph_prev_tokens")
    await print_all_provenance(workflow, loading_context)

    # workflow.steps.inputs.keys()

    return dag_tokens
