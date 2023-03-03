import asyncio
import os
import tempfile
from asyncio.locks import Lock
from collections.abc import Iterable

import pytest
import pytest_asyncio

from streamflow.main import build_context
from streamflow.core.config import Config
from streamflow.core.deployment import Target
from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import PersistableEntity
from streamflow.core.workflow import Step, Port, Token, Workflow
from streamflow.core.deployment import DeploymentConfig, LOCAL_LOCATION, Location
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext


async def get_location(
    context: StreamFlowContext, request: pytest.FixtureRequest
) -> Location:
    if request.param == "local":
        return Location(deployment=LOCAL_LOCATION, name=LOCAL_LOCATION)
    elif request.param == "docker":
        connector = context.deployment_manager.get_connector("alpine")
        locations = await connector.get_available_locations()
        return Location(deployment="alpine", name=next(iter(locations.keys())))
    else:
        raise Exception(f"{request.param} location type not supported")


def get_docker_deployment_config():
    return DeploymentConfig(
        name="alpine",
        type="docker",
        config={"image": "alpine:3.16.2"},
        external=False,
        lazy=False,
    )


@pytest_asyncio.fixture(scope="session")
async def context() -> StreamFlowContext:
    context = build_context(
        os.path.realpath(tempfile.gettempdir()),
        {"database": {"type": "default", "config": {"connection": ":memory:"}}},
    )
    await context.deployment_manager.deploy(
        DeploymentConfig(
            name=LOCAL_LOCATION,
            type="local",
            config={},
            external=True,
            lazy=False,
            workdir=os.path.realpath(tempfile.gettempdir()),
        )
    )
    await context.deployment_manager.deploy(get_docker_deployment_config())
    yield context
    await context.deployment_manager.undeploy_all()
    # close the database connection
    await context.database.close()


@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


def is_primitive_type(elem):
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

    if is_primitive_type(elem1):
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
