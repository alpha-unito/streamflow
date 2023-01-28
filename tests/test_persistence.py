import posixpath
from rdflib import Graph
from collections.abc import Iterable
from asyncio.locks import Lock

import pytest

from tests.conftest import get_docker_deployment_config

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.config import Config, BindingConfig
from streamflow.core.deployment import Target, LocalTarget
from streamflow.core.persistence import PersistableEntity
from streamflow.core.workflow import Job, Step, Port, Token, Workflow

from streamflow.workflow.port import JobPort, ConnectorPort

# abstract class: ConditionalStep, InputInjectorStep, LoopOutputStep, TransferStep, Transformer
from streamflow.workflow.step import (
    CombinatorStep,
    DeployStep,
    ExecuteStep,
    GatherStep,
    ScheduleStep,
    ScatterStep,
    LoopCombinatorStep,
)
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
    LoopCombinator,
    LoopTerminationCombinator,
)
from streamflow.workflow.token import (
    JobToken,
    ListToken,
    ObjectToken,
    TerminationToken,
    IterationTerminationToken,
)


from streamflow.cwl.utils import LoadListing, SecondaryFile
from streamflow.cwl.step import CWLConditionalStep  # ConditionalStep
from streamflow.cwl.combinator import ListMergeCombinator  # CombinatorStep
from streamflow.cwl.processor import (
    CWLTokenProcessor,
    CWLMapTokenProcessor,
    CWLObjectTokenProcessor,
    CWLUnionTokenProcessor,
)
from streamflow.cwl.transformer import (
    DefaultTransformer,
    DefaultRetagTransformer,
    CWLTokenTransformer,
    ValueFromTransformer,
    LoopValueFromTransformer,
    AllNonNullTransformer,
    FirstNonNullTransformer,
    ForwardTransformer,
    ListToElementTransformer,
    OnlyNonNullTransformer,
)


from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext


def is_primiteve_type(elem):
    return type(elem) in (int, float, str, bool)


# The function given in input an object return a dictionary with attribute:value
def object_to_dict(obj):
    return {
        attr: getattr(obj, attr)
        for attr in dir(obj)
        if not attr.startswith("__") and not callable(getattr(obj, attr))
    }


# The function return True if the elems are the same, otherwise False
# The param obj_compared is useful to break a circul reference inside the objects
# remembering the objects already encountered
def are_equals(elem1, elem2, obj_compared=None):
    obj_compared = obj_compared if obj_compared else []

    # if the objects are of different types, they are definitely not the same
    if type(elem1) != type(elem2):
        return False

    if type(elem1) == Lock:
        return True

    if is_primiteve_type(elem1):
        return elem1 == elem2

    if isinstance(elem1, Iterable) and not isinstance(elem1, dict):
        if len(elem1) != len(elem2):
            return False
        for e1, e2 in zip(elem1, elem2):
            if not are_equals(e1, e2, obj_compared):
                return False
        return True

    if isinstance(elem1, dict):
        dict1 = elem1
        dict2 = elem2
    else:
        dict1 = object_to_dict(elem1)
        dict2 = object_to_dict(elem2)

    # WARN: if the key is an Object, override __eq__ and __hash__ for a correct result
    if dict1.keys() != dict2.keys():
        return False

    # if their references are in the obj_compared list there is a circular reference to break
    if elem1 in obj_compared:
        return True
    else:
        obj_compared.append(elem1)

    if elem2 in obj_compared:
        return True
    else:
        obj_compared.append(elem2)

    # save the different values on the same attribute in the two dicts in a list:
    #   - if we find objects in the list, they must be checked recursively on their attributes
    #   - if we find elems of primitive types, their values are actually different
    differences = [
        (dict1[attr], dict2[attr])
        for attr in dict1.keys()
        if dict1[attr] != dict2[attr]
    ]
    for value1, value2 in differences:
        # check recursively the elements
        if not are_equals(value1, value2, obj_compared):
            return False
    return True


async def save_load_and_test(elem: PersistableEntity, context):
    assert elem.persistent_id is None
    await elem.save(context)
    assert elem.persistent_id is not None

    # created a new DefaultDatabaseLoadingContext to have the objects fetched from the database
    # (and not take their reference saved in the attributes)
    loading_context = DefaultDatabaseLoadingContext()
    loaded = None
    if isinstance(elem, Step):
        loaded = await loading_context.load_step(context, elem.persistent_id)
    elif isinstance(elem, Port):
        loaded = await loading_context.load_port(context, elem.persistent_id)
    elif isinstance(elem, Token):
        loaded = await loading_context.load_token(context, elem.persistent_id)
    elif isinstance(elem, Workflow):
        loaded = await loading_context.load_workflow(context, elem.persistent_id)
    elif isinstance(elem, Target):
        loaded = await loading_context.load_target(context, elem.persistent_id)
    elif isinstance(elem, Config):
        loaded = await loading_context.load_deployment(context, elem.persistent_id)
    assert are_equals(elem, loaded)


def create_cwl_token_processor(name, workflow):
    return CWLTokenProcessor(
        name=name,
        workflow=workflow,
        token_type="enum",
        enum_symbols=["path1", "path2"],
        expression_lib=["expr_lib1", "expr_lib2"],
        secondary_files=[SecondaryFile("file1", True)],
        format_graph=Graph(),
        load_listing=LoadListing.no_listing,
    )


# Testing Workflow
@pytest.mark.asyncio
async def test_workflow(context: StreamFlowContext):
    """Test saving and loading Workflow from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await save_load_and_test(workflow, context)


# Testing Port and its extension classes
@pytest.mark.asyncio
async def test_port(context: StreamFlowContext):
    """Test saving and loading Port from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    # dependency with workflow for saving port
    port = workflow.create_port()
    assert isinstance(port, Port)
    await save_load_and_test(port, context)


@pytest.mark.asyncio
async def test_job_port(context: StreamFlowContext):
    """Test saving and loading JobPort from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    # dependency with workflow for saving port
    port = workflow.create_port(JobPort)
    assert isinstance(port, JobPort)
    await save_load_and_test(port, context)


@pytest.mark.asyncio
async def test_connector_port(context: StreamFlowContext):
    """Test saving and loading ConnectorPort from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    # dependency with workflow for saving port
    port = workflow.create_port(ConnectorPort)
    assert isinstance(port, ConnectorPort)
    await save_load_and_test(port, context)


# Testing Step and its extension classes
@pytest.mark.asyncio
async def test_combinator_step(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with CartesianProductCombinator from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=name + "-combinator",
        combinator=CartesianProductCombinator(
            name=utils.random_name(), workflow=workflow, depth=1
        ),
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_loop_combinator_step(context: StreamFlowContext):
    """Test saving and loading LoopCombinatorStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    step = workflow.create_step(
        cls=LoopCombinatorStep,
        name=name + "-combinator",
        combinator=CartesianProductCombinator(
            name=utils.random_name(), workflow=workflow, depth=1
        ),
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_deploy_step(context: StreamFlowContext):
    """Test saving and loading DeployStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    connector_port = workflow.create_port(cls=ConnectorPort)
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    deployment_config = get_docker_deployment_config()

    step = workflow.create_step(
        cls=DeployStep,
        name=posixpath.join("__deploy__", deployment_config.name),
        deployment_config=deployment_config,
        connector_port=connector_port,
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_schedule_step(context: StreamFlowContext):
    """Test saving and loading ScheduleStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    binding_config = BindingConfig(targets=[LocalTarget(workdir=utils.random_name())])
    schedule_step = workflow.create_step(
        cls=ScheduleStep,
        name=posixpath.join(utils.random_name() + "-injector", "__schedule__"),
        connector_ports={binding_config.targets[0].deployment.name: port},
        input_directory=binding_config.targets[0].workdir,
        output_directory=binding_config.targets[0].workdir,
        tmp_directory=binding_config.targets[0].workdir,
        binding_config=binding_config,
    )

    await save_load_and_test(schedule_step, context)


@pytest.mark.asyncio
async def test_execute_step(context: StreamFlowContext):
    """Test saving and loading ExecuteStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_gather_step(context: StreamFlowContext):
    """Test saving and loading GatherStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    step = workflow.create_step(
        cls=GatherStep, name=utils.random_name() + "-gather", depth=1
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_scatter_step(context: StreamFlowContext):
    """Test saving and loading ScatterStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    step = workflow.create_step(cls=ScatterStep, name=utils.random_name() + "-scatter")
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_default_transformer(context: StreamFlowContext):
    """Test saving and loading DefaultTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=DefaultTransformer, name=name + "-transformer", default_port=port
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_default_retag_transformer(context: StreamFlowContext):
    """Test saving and loading DefaultRetagTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=DefaultRetagTransformer, name=name + "-transformer", default_port=port
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_token_transformer(context: StreamFlowContext):
    """Test saving and loading CWLTokenTransformer with CWLTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name + "-transformer",
        port_name=port.name,
        processor=create_cwl_token_processor(port.name, workflow),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_value_from_transformer(context: StreamFlowContext):
    """Test saving and loading ValueFromTransformer with CWLTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=ValueFromTransformer,
        name=name + "-value-from-transformer",
        processor=create_cwl_token_processor(port.name, workflow),
        port_name=port.name,
        expression_lib=True,
        full_js=False,
        value_from="$(1 + 1)",
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_loop_value_from_transformer(context: StreamFlowContext):
    """Test saving and loading LoopValueFromTransformer with CWLTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=LoopValueFromTransformer,
        name=name + "-loop-value-from-transformer",
        processor=create_cwl_token_processor(port.name, workflow),
        port_name=port.name,
        expression_lib=True,
        full_js=False,
        value_from="$(1 + 1 == 0)",
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_map_token_transformer(context: StreamFlowContext):
    """Test saving and loading CWLTokenTransformer with CWLMapTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name + "-transformer",
        port_name=port.name,
        processor=CWLMapTokenProcessor(
            name=port.name,
            workflow=workflow,
            processor=create_cwl_token_processor(port.name, workflow),
        ),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_object_token_transformer(context: StreamFlowContext):
    """Test saving and loading CWLTokenTransformer with CWLObjectTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name + "-transformer",
        port_name=port.name,
        processor=CWLObjectTokenProcessor(
            name=port.name,
            workflow=workflow,
            processors={
                utils.random_name(): create_cwl_token_processor(port.name, workflow)
            },
        ),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_union_token_transformer(context: StreamFlowContext):
    """Test saving and loading CWLTokenTransformer with CWLUnionTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name + "-transformer",
        port_name=port.name,
        processor=CWLUnionTokenProcessor(
            name=port.name,
            workflow=workflow,
            processors=[create_cwl_token_processor(port.name, workflow)],
        ),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_all_non_null_transformer(context: StreamFlowContext):
    """Test saving and loading AllNonNullTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=AllNonNullTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(name, in_port)
    transformer.add_output_port(name, out_port)
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_first_non_null_transformer(context: StreamFlowContext):
    """Test saving and loading FirstNonNullTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=FirstNonNullTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(name, in_port)
    transformer.add_output_port(name, out_port)
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_forward_transformer(context: StreamFlowContext):
    """Test saving and loading ForwardTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=ForwardTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(name, in_port)
    transformer.add_output_port(name, out_port)
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_list_to_element_transformer(context: StreamFlowContext):
    """Test saving and loading ListToElementTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=ListToElementTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(name, in_port)
    transformer.add_output_port(name, out_port)
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_only_non_null_transformer(context: StreamFlowContext):
    """Test saving and loading OnlyNonNullTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=OnlyNonNullTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(name, in_port)
    transformer.add_output_port(name, out_port)
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_conditional_step(context: StreamFlowContext):
    """Test saving and loading CWLConditionalStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    step = workflow.create_step(
        cls=CWLConditionalStep,
        name=utils.random_name() + "-when",
        expression="inputs.name.length == 10",
        expression_lib=[],  # MutableSequence[Any]
        full_js=True,
    )
    await save_load_and_test(step, context)


# Subtest - Step param combinator
@pytest.mark.asyncio
async def test_dot_product_combinator(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with DotProductCombinator from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=name + "-combinator",
        combinator=DotProductCombinator(name=utils.random_name(), workflow=workflow),
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_list_merge_combinator(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with ListMergeCombinator from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=name + "-combinator",
        combinator=ListMergeCombinator(
            name=utils.random_name(),
            workflow=workflow,
            input_names=[port.name],
            output_name=name,
            flatten=False,
        ),
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_loop_combinator(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with LoopCombinator from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=name + "-combinator",
        combinator=LoopCombinator(name=utils.random_name(), workflow=workflow),
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_loop_termination_combinator(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with LoopTerminationCombinator from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=name + "-combinator",
        combinator=LoopTerminationCombinator(
            name=utils.random_name(), workflow=workflow
        ),
    )
    await save_load_and_test(step, context)


# Testing the Target and its extension classes
@pytest.mark.asyncio
async def test_target(context: StreamFlowContext):
    """Test saving and loading Target from database"""
    target = Target(
        deployment=get_docker_deployment_config(),
        service="test-persistence",
        workdir=utils.random_name(),
    )
    await save_load_and_test(target, context)


@pytest.mark.asyncio
async def test_local_target(context: StreamFlowContext):
    """Test saving and loading LocalTarget from database"""
    target = LocalTarget(workdir=utils.random_name())
    await save_load_and_test(target, context)


# Testing the Token and its extension classes
@pytest.mark.asyncio
async def test_token(context: StreamFlowContext):
    """Test saving and loading Token from database"""
    token = Token(value=["test", "token"])
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_job_token(context: StreamFlowContext):
    """Test saving and loading JobToken from database"""
    token = JobToken(
        value=Job(
            workflow_id=0,
            name=utils.random_name(),
            inputs={"test": Token(value="jobtoken")},
            input_directory=utils.random_name(),
            output_directory=utils.random_name(),
            tmp_directory=utils.random_name(),
        ),
    )
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_list_token(context: StreamFlowContext):
    """Test saving and loading ListToken from database"""
    token = ListToken(value=[Token("list"), Token("test")])
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_object_token(context: StreamFlowContext):
    """Test saving and loading ObjectToken from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    token = ObjectToken(value=dict({"test": Token("object")}))
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_termination_token(context: StreamFlowContext):
    """Test saving and loading IterationTerminationToken from database"""
    token = TerminationToken()
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_iteration_termination_token(context: StreamFlowContext):
    """Test saving and loading IterationTerminationToken from database"""
    token = IterationTerminationToken("1")
    await save_load_and_test(token, context)


# Deployment test
@pytest.mark.asyncio
async def test_deployment(context: StreamFlowContext):
    """Test saving and loading deployment configuration from database"""
    config = get_docker_deployment_config()
    await save_load_and_test(config, context)
