import pytest
import tempfile
import posixpath
from collections.abc import Iterable

from tests.conftest import get_docker_deployment_config

from streamflow.core import utils
from streamflow.core.deployment import Target
from streamflow.core.workflow import Token, Workflow
from streamflow.core.context import StreamFlowContext
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext

from streamflow.workflow.step import DeployStep, ConnectorPort


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
# The param obj_compared is usefull to break a circul reference inside the objects
# remembering the objects already encountered
def are_equals(elem1, elem2, obj_compared=[]):

    # if the objects are of different types, they are definitely not the same
    if type(elem1) != type(elem2):
        return False

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

    if dict1.keys() != dict2.keys():
        return False

    # if their reference are in the obj_compared list there is a circular reference to break
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


@pytest.mark.asyncio
async def test_workflow(context: StreamFlowContext):
    workflow = Workflow(context=context, type="cwl", name=utils.random_name())
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    # created a new DefaultDatabaseLoadingContext to have the objects fetched from the database
    # (and not take their reference saved in the attributes)
    loading_context = DefaultDatabaseLoadingContext()
    loaded = await loading_context.load_workflow(context, workflow.persistent_id)
    assert are_equals(workflow, loaded)


@pytest.mark.asyncio
async def test_port(context: StreamFlowContext):
    workflow = Workflow(context=context, type="cwl", name=utils.random_name())
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    # dependency with workflow for saving port
    port = workflow.create_port()

    assert port.persistent_id is None
    await port.save(context)
    assert port.persistent_id is not None

    # created a new DefaultDatabaseLoadingContext to have the objects fetched from the database
    # (and not take their reference saved in the attributes)
    loading_context = DefaultDatabaseLoadingContext()
    loaded = await loading_context.load_port(context, port.persistent_id)
    assert are_equals(port, loaded)


@pytest.mark.asyncio
async def test_step(context: StreamFlowContext):
    workflow = Workflow(context=context, type="cwl", name=utils.random_name())
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    deployment_config = get_docker_deployment_config()

    connector_port = workflow.create_port(cls=ConnectorPort)
    assert connector_port.persistent_id is None
    await connector_port.save(context)
    assert connector_port.persistent_id is not None

    # dependency with workflow and connector_port for saving step
    step = workflow.create_step(
        cls=DeployStep,
        name=posixpath.join("__deploy__", deployment_config.name),
        deployment_config=deployment_config,
        connector_port=connector_port,
    )
    assert step.persistent_id is None
    await step.save(context)
    assert step.persistent_id is not None
    # created a new DefaultDatabaseLoadingContext to have the objects fetched from the database
    # (and not take their reference saved in the attributes)
    loading_context = DefaultDatabaseLoadingContext()
    loaded = await loading_context.load_step(context, step.persistent_id)
    assert are_equals(step, loaded)


### indipendet objects from the other ####


@pytest.mark.asyncio
async def test_target(context: StreamFlowContext):
    """Test saving and loading deployment configuration from database"""
    target = Target(
        deployment=get_docker_deployment_config(),
        service="test-persistence",
        workdir=utils.random_name(),
    )

    assert target.persistent_id is None
    await target.save(context)
    assert target.persistent_id is not None

    # created a new DefaultDatabaseLoadingContext to have the objects fetched from the database
    # (and not take their reference saved in the attributes)
    loading_context = DefaultDatabaseLoadingContext()
    loaded = await loading_context.load_target(context, target.persistent_id)
    assert are_equals(target, loaded)


@pytest.mark.asyncio
async def test_token(context: StreamFlowContext):
    token = Token(value=["test", "token"])

    assert token.persistent_id is None
    await token.save(context)
    assert token.persistent_id is not None

    # created a new DefaultDatabaseLoadingContext to have the objects fetched from the database
    # (and not take their reference saved in the attributes)
    loading_context = DefaultDatabaseLoadingContext()
    loaded = await loading_context.load_token(context, token.persistent_id)
    assert are_equals(token, loaded)


@pytest.mark.asyncio
async def test_deployment(context: StreamFlowContext):
    """Test saving and loading deployment configuration from database"""
    config = get_docker_deployment_config()

    assert config.persistent_id is None
    await config.save(context)
    assert config.persistent_id is not None

    # created a new DefaultDatabaseLoadingContext to have the objects fetched from the database
    # (and not take their reference saved in the attributes)
    loading_context = DefaultDatabaseLoadingContext()
    loaded = await loading_context.load_deployment(context, config.persistent_id)
    assert are_equals(config, loaded)
