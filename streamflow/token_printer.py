import json
import os
import random
import asyncio
import graphviz

from streamflow.core.context import StreamFlowContext
from streamflow.core.utils import get_class_fullname
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
        dot.node(str(vertex), color="black" if neighbors else "red")
        for n in neighbors:
            dot.edge(str(vertex), str(n))
    filepath = GRAPH_PATH + title + ".gv"
    dot.render(filepath)
    os.system("rm " + filepath)


def print_graph_figure_label(graph, title, colors=None):
    dot = graphviz.Digraph(title)
    for vertex, neighbors in graph.items():
        dot.node(
            str(vertex), color=colors[vertex.split("\n")[-1]] if colors else "black"
        )
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
                    tokens[k], workflow.context, loading_context, None
                )
            ).pop()
        step_name = (
            k_step.name + "\n" + k_step.workflow.name if k != -1 else INIT_DAG_FLAG
        )
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
            inner_s_name = s.name + "\n" + s.workflow.name
            graph_steps[step_name].add(inner_s_name)
            steps[inner_s_name] = s
            if inner_s_name not in steps_token.keys():
                steps_token[inner_s_name] = set()
            steps_token[inner_s_name].add(v)

    valid_steps_graph = {}
    for step_name_1, steps_name in graph_steps.items():
        step_name_1_new = get_name_and_type(step_name_1, steps)
        valid_steps_graph[step_name_1_new] = []
        for step_name_2 in steps_name:
            step_name_2 = get_name_and_type(step_name_2, steps)
            for label in steps_token[step_name_1]:
                add_pair(
                    step_name_2,
                    str_token_value(tokens[label]),  # + f"({label})",
                    valid_steps_graph[step_name_1_new],
                    tokens,
                )
    # { step_name : [ (next_step_name, label) ] }
    print_graph_figure(graph, "get_all_provenance_tokens")
    print_graph_figure_label(valid_steps_graph, "get_all_provenance_steps")
    #    wf_steps = sorted(workflow.steps.keys())
    pass


def str_token_value_dict(token):
    if isinstance(token, CWLFileToken):
        return f"id: {token.persistent_id}\nval: {token.value['class']}\ntag: {token.tag}"  # token.value['path']
    if isinstance(token, ListToken):
        return (
            str([str_token_value(t) for t in token.value]) + f"({token.persistent_id})"
        )
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
            return f"id: {token.persistent_id}\nval: {token.value}\ntag: {token.tag}"  # str(token.value)
    return "None"


def label_token_availability(token_available):
    return "A" if token_available else "NA"


def str_token_value_shorter(token):
    if isinstance(token, CWLFileToken):
        return f"{token.value['class']} {token.value['basename']}"
    if isinstance(token, ListToken):
        return str([str_token_value_shorter(t) for t in token.value])
    if isinstance(token, JobToken):
        return token.value.name
    if isinstance(token, TerminationToken):
        return "T"
    if isinstance(token, IterationTerminationToken):
        return "IT"
    if isinstance(token, Token):
        if isinstance(token.value, Token):
            return "t(" + str_token_value_shorter(token.value)
        else:
            return f"{token.value}"
    return "None"


def str_token_value(token):
    if isinstance(token, CWLFileToken):
        return f"{token.value['class']} {token.tag}({token.persistent_id})"
    if isinstance(token, ListToken):
        return (
            str([str_token_value(t) for t in token.value]) + f"({token.persistent_id})"
        )
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
            return f"{token.value} {token.tag}({token.persistent_id})"
    return "None"


def str_value(value):
    if isinstance(value, dict):
        return value["basename"]
    return value


def temp_print_retag(workflow_name, output_port, tag, retags, final_msg):
    if False:
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
                    if v
                },
                indent=2,
            ),
            final_msg,
        )
    pass


def print_debug_divergenza(all_token_visited, port_tokens):
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
                            f"DIVERGENZAAA port {port_name} ma sono due job token, quindi tutto regolare.",
                            all_token_visited[token_id][0].value.name,
                            "(id:",
                            all_token_visited[token_id][0].persistent_id,
                            ")",
                            "and",
                            all_token_visited[token_id_2][0].value.name,
                            "(id:",
                            all_token_visited[token_id_2][0].persistent_id,
                            ")",
                        )
                    else:
                        t_a = all_token_visited[token_id][0]
                        # t_b = all_token_visited[token_id_2][0]
                        print(
                            "DIVERGENZAAA port",
                            port_name,
                            "type:",
                            type(t_a),
                            ", id:",
                            token_id,
                            ", tag:",
                            t_a.tag,
                            ", value:",
                            t_a.value.name
                            if isinstance(t_a[0], JobToken)
                            else t_a[0].value,
                        )
                        pass
    print("DEBUG: divergenza controllata")


async def print_grafici_post_remove(
    dag_ports,
    dir_path,
    new_workflow_name,
    port_tokens,
    port_name_id,
    workflow,
    failed_step,
):
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


async def print_grafici_parte_uno(
    all_token_visited,
    dag_tokens,
    dag_ports,
    dir_path,
    new_workflow_name,
    port_tokens,
    port_name_id,
    workflow,
    failed_step,
):
    print_graph_figure(
        {
            (
                k,
                all_token_visited[k][0].tag,
                label_token_availability(all_token_visited[k][1]),
            ): [
                (
                    vv,
                    all_token_visited[vv][0].tag,
                    label_token_availability(all_token_visited[vv][1]),
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

    print("DEBUG: grafici creati")


async def print_step_from_ports(
    dag_ports,
    ports,
    port_names,
    context: StreamFlowContext,
    failed_step_name,
    complete_dir_path,
):
    port_step = {}
    for port_name in port_names:
        step_row = await context.database.get_step_from_outport(ports[port_name])
        port_step[port_name] = step_row["name"]
    port_step[INIT_DAG_FLAG] = INIT_DAG_FLAG
    port_step[failed_step_name] = failed_step_name

    dag_steps = {}
    for port_name, next_port_names in dag_ports.items():
        for next_port_name in next_port_names:
            dag_steps.setdefault(port_step[port_name], set()).add(
                port_step[next_port_name]
            )

    print_graph_figure(
        {k: v for k, v in dag_steps.items() if k != INIT_DAG_FLAG},
        complete_dir_path,
    )


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
        s = (
            await _load_steps_from_token(
                token_visited[token_id][0],
                workflow.context,
                loading_context,
                workflow,
            )
        ).pop()
        steps[token_id] = (
            s.name + "\n" + s.workflow.name
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
        step_1 = (
            steps[token_id] if isinstance(token_id, int) else token_id
        )  # + "\nstep1\n" + workflow.name
        if step_1 == INIT_DAG_FLAG:
            continue
        if step_1 not in graph_steps.keys():
            graph_steps[step_1] = []
        label = (
            str_token_value(token_visited[token_id][0])  # + f"({token_id})"
            if isinstance(token_id, int)
            else token_values[token_id]
        )
        for token_id_2 in tokens_id:
            step_2 = (
                steps[token_id_2]
                if isinstance(token_id_2, int)
                else token_id_2 + "\nstep2\n" + workflow.name
            )

            add_graph_tuple(graph_steps, step_1, (step_2, label))

    colors = {}
    for vertex in graph_steps.keys():
        workflow_init_name = vertex.split("\n")[-1]
        if workflow_init_name not in colors.keys():
            colors[workflow_init_name] = None
    number_of_colors = len(colors.keys())
    all_color = [
        "#" + "".join([random.choice("0123456789ABCDEF") for _ in range(6)])
        for _ in range(number_of_colors)
    ]
    for i, k in enumerate(colors.keys()):
        colors[k] = all_color[i]
    # print("colors", colors)
    print_graph_figure_label(graph_steps, pdf_name, colors)


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


async def _build_dag(token_list, workflow, loading_context=None):
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
    # await print_all_provenance(workflow, loading_context)

    # workflow.steps.inputs.keys()

    return dag_tokens
