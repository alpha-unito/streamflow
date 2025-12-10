from __future__ import annotations

import argparse
import os
import platform
from asyncio.locks import Lock
from collections.abc import AsyncGenerator, Callable, Collection, MutableSequence
from typing import Any

import pytest
import pytest_asyncio

import streamflow.deployment.connector
import streamflow.deployment.filter
from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import PersistableEntity
from streamflow.main import build_context
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from tests.utils.connector import (
    FailureConnector,
    ParameterizableHardwareConnector,
    TarConnector,
)
from tests.utils.deployment import ReverseTargetsBindingFilter, get_deployment_config


def csvtype(choices):
    """Return a function that splits and checks comma-separated values."""

    def splitarg(arg):
        values = arg.split(",")
        for value in values:
            if value not in choices:
                raise argparse.ArgumentTypeError(
                    "invalid choice: {!r} (choose from {})".format(
                        value, ", ".join(map(repr, choices))
                    )
                )
        return values

    return splitarg


def pytest_addoption(parser):
    parser.addoption(
        "--deploys",
        type=csvtype(all_deployment_types()),
        default=all_deployment_types(),
        help="List of deployments to deploy. Use the comma as delimiter e.g. --deploys "
        f"local,docker. (default: {all_deployment_types()})",
    )


def pytest_configure(config):
    streamflow.deployment.connector.connector_classes.update(
        {
            "aiotar": TarConnector,
            "failure": FailureConnector,
            "parameterizable_hardware": ParameterizableHardwareConnector,
        }
    )
    streamflow.deployment.filter.binding_filter_classes.update(
        {"reverse": ReverseTargetsBindingFilter}
    )


@pytest.fixture(scope="session")
def chosen_deployment_types(request):
    return request.config.getoption("--deploys")


def pytest_generate_tests(metafunc):
    if "deployment" in metafunc.fixturenames:
        metafunc.parametrize(
            "deployment", metafunc.config.getoption("deploys"), scope="session"
        )
    if "deployment_src" in metafunc.fixturenames:
        metafunc.parametrize(
            "deployment_src",
            metafunc.config.getoption("deploys"),
            scope="session",
        )
    if "deployment_dst" in metafunc.fixturenames:
        metafunc.parametrize(
            "deployment_dst",
            metafunc.config.getoption("deploys"),
            scope="session",
        )
    if "communication_pattern" in metafunc.fixturenames:
        fst_remote = next(
            (d for d in metafunc.config.getoption("deploys") if d != "local"), None
        )
        test_local = "local" in metafunc.config.getoption("deploys")
        comm_pattern = [
            # Local to Remote (and Local to Local)
            *(
                (("local", d) for d in metafunc.config.getoption("deploys"))
                if test_local
                else ()
            ),
            # Remote to Local
            *(
                (
                    (d, "local")
                    for d in metafunc.config.getoption("deploys")
                    if d != "local"
                )
                if test_local
                else ()
            ),
            # Remote to Remote in the same location
            *((d, d) for d in metafunc.config.getoption("deploys") if d != "local"),
            # Remote to Remote testing the data receiving
            *(
                (
                    (fst_remote, d)
                    for d in metafunc.config.getoption("deploys")
                    if d != fst_remote and d != "local"
                )
                if fst_remote
                else ()
            ),
            # Remote to Remote testing the data sending
            *(
                (
                    (d, fst_remote)
                    for d in metafunc.config.getoption("deploys")
                    if d != fst_remote and d != "local"
                )
                if fst_remote
                else ()
            ),
        ]
        metafunc.parametrize(
            "communication_pattern",
            comm_pattern,
            ids=["-".join(locs) for locs in comm_pattern],
            scope="session",
        )


def all_deployment_types():
    deployments_ = ["local", "docker", "docker-compose", "docker-wrapper", "slurm"]
    if platform.system() == "Linux":
        deployments_.extend(["kubernetes", "singularity", "ssh"])
    return deployments_


@pytest_asyncio.fixture(scope="session")
async def context(
    chosen_deployment_types: MutableSequence[str],
) -> AsyncGenerator[StreamFlowContext, Any]:
    _context = build_context(
        {
            "database": {"type": "default", "config": {"connection": ":memory:"}},
            "path": os.getcwd(),
        },
    )
    for deployment_t in (*chosen_deployment_types, "parameterizable_hardware"):
        config = await get_deployment_config(_context, deployment_t)
        await _context.deployment_manager.deploy(config)
    yield _context
    await _context.deployment_manager.undeploy_all()
    await _context.close()


def is_primitive_type(elem: Any) -> bool:
    """The function given in input an object returns True if it has a primitive types (i.e. int, float, str or bool)"""
    return type(elem) in (int, float, str, bool)


def get_class_callables(class_type) -> MutableSequence[Callable]:
    """The function given in input a class type returns a list of strings with the method names"""
    return [
        getattr(class_type, func)
        for func in dir(class_type)
        if callable(getattr(class_type, func)) and not func.startswith("__")
    ]


def object_to_dict(obj):
    """The function given in input an object returns a dictionary with attribute:value"""
    return {
        attr: getattr(obj, attr)
        for attr in dir(obj)
        if not attr.startswith("__") and not callable(getattr(obj, attr))
    }


def are_equals(elem1, elem2, obj_compared=None):
    """
    The function return True if the elems are the same, otherwise False
    The param obj_compared is useful to break a circul reference inside the objects
    remembering the objects already encountered
    """
    obj_compared = obj_compared if obj_compared else []

    # If the objects are of different types, they are definitely different
    if type(elem1) is not type(elem2):
        return False

    if isinstance(elem1, Lock):
        return True

    if is_primitive_type(elem1):
        return elem1 == elem2

    if isinstance(elem1, Collection) and not isinstance(elem1, dict):
        if len(elem1) != len(elem2):
            return False
        for e1, e2 in zip(elem1, elem2, strict=True):
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

    # If their references are in the obj_compared list there is a circular reference to break
    if elem1 in obj_compared:
        return True
    else:
        obj_compared.append(elem1)

    if elem2 in obj_compared:
        return True
    else:
        obj_compared.append(elem2)

    # Save the different values on the same attribute in the two dicts in a list:
    #   - if we find objects in the list, they must be checked recursively on their attributes
    #   - if we find elems of primitive types, their values are actually different
    differences = {
        attr: (dict1[attr], dict2[attr])
        for attr in dict1.keys()
        if dict1[attr] != dict2[attr]
    }
    for key, (value1, value2) in differences.items():
        # Check recursively the elements
        if not are_equals(value1, value2, obj_compared):
            print(f"Attribute {key} has different values")
            return False
    return True


async def save_load_and_test(elem: PersistableEntity, context):
    assert elem.persistent_id is None
    await elem.save(context)
    assert elem.persistent_id is not None

    # Created a new DefaultDatabaseLoadingContext to have the objects fetched from the database
    # (and not take their reference saved in the attributes)
    loading_context = DefaultDatabaseLoadingContext()
    loaded = await type(elem).load(context, elem.persistent_id, loading_context)
    assert are_equals(elem, loaded)
