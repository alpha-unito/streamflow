import pytest

from tests.conftest import get_docker_deployment_config, get_local_deployment_config

from streamflow.core import utils
from streamflow.core.deployment import DeploymentConfig
from streamflow.core.context import StreamFlowContext
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext

# The function given in input an object return a dictionary with attribute:value
def object_to_dict(obj):
    return {attr: getattr(obj, attr) for attr in dir(obj) if not attr.startswith("__")}


# The function return True if the elem is a type primitive like int, str, float, otherwise return False if it is an object with attributes
def is_type_primitive(elem):
    return type(elem) is not object and not hasattr(elem, "__dict__")


# The function return True if the elems are the same, otherwise False
# the elems can be objects or primitive
def are_equals(elem1, elem2):
    if type(elem1) != type(elem2):
        return False

    if is_type_primitive(elem1):
        return elem1 == elem2

    dict1 = object_to_dict(elem1)
    dict2 = object_to_dict(elem2)

    if dict1.keys() != dict2.keys():
        return False

    # save the different values on the same attribute in the two dicts in a list:
    #   - if we find objects in the list, they must be checked recursively on their attributes
    #   - if we find elems of primitive types, their values are actually different
    differences = [
        (attr, dict1[attr], dict2[attr])
        for attr in dict1.keys()
        if dict1[attr] != dict2[attr]
    ]
    for _, value1, value2 in differences:
        # check recursively the elements
        if not are_equals(value1, value2):
            return False
    return True


@pytest.mark.asyncio
async def test_deployment(context: StreamFlowContext):
    """Test saving and loading deployment configuration from database"""
    config = get_docker_deployment_config()

    assert config.persistent_id is None
    _ = await config.save(context)  # return None
    assert config.persistent_id is not None

    # created a new DefaultDatabaseLoadingContext to have the objects fetched from the database
    # (and not take their reference saved in the attributes)
    loading_context = DefaultDatabaseLoadingContext()

    loaded = await DeploymentConfig.load(context, config.persistent_id, loading_context)
    assert are_equals(config, loaded)

    # close the database connection
    await context.database.close()


@pytest.mark.asyncio
async def test_port(context: StreamFlowContext):
    pass


@pytest.mark.asyncio
async def test_step(context: StreamFlowContext):
    pass


@pytest.mark.asyncio
async def test_target(context: StreamFlowContext):
    pass


@pytest.mark.asyncio
async def test_token(context: StreamFlowContext):
    pass


@pytest.mark.asyncio
async def test_workflow(context: StreamFlowContext):
    pass
