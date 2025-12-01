import hashlib
from typing import cast

from pytest import raises

from streamflow.config.schema import SfSchema
from streamflow.config.validator import SfValidator
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.main import build_context
from streamflow.persistence import SqliteDatabase
from streamflow.recovery import DefaultCheckpointManager, DefaultFailureManager
from streamflow.scheduling import DefaultScheduler


def test_cwl_workflow():
    """Check that CWL workflows are supported."""
    config = {
        "version": "v1.0",
        "workflows": {
            "example": {
                "type": "cwl",
                "config": {"file": "main.cwl", "settings": "config.yml"},
            }
        },
    }
    SfValidator().validate(config)


def test_cwl_workflow_fail_missing_file():
    """Check that validation fails when the `file` clause is not specified for a CWL workflow."""
    config = {
        "version": "v1.0",
        "workflows": {
            "example": {
                "type": "cwl",
                "config": {"settings": "config.yml"},
            }
        },
    }
    with raises(WorkflowDefinitionException):
        SfValidator().validate(config)


def test_cwl_workflow_fail_unsupported_property():
    """Check that validation fails when an unsupported property is specified for a CWL workflow."""
    config = {
        "version": "v1.0",
        "workflows": {
            "example": {
                "type": "cwl",
                "config": {
                    "file": "main.cwl",
                    "settings": "config.yml",
                    "unsupported": {},
                },
            }
        },
    }
    with raises(WorkflowDefinitionException):
        SfValidator().validate(config)


def test_cwl_workflow_missing_settings():
    """Check that validation does not fail when the `settings` clause is not specified for a CWL workflow."""
    config = {
        "version": "v1.0",
        "workflows": {
            "example": {
                "type": "cwl",
                "config": {"file": "main.cwl"},
            }
        },
    }
    SfValidator().validate(config)


def test_ext_support():
    """Check that all extension points are supported."""
    config = {
        "version": "v1.0",
        "workflows": {
            "example": {
                "type": "cwl",
                "config": {"file": "main.cwl", "settings": "config.yml", "docker": []},
                "bindings": [
                    {
                        "step": "/",
                        "target": {
                            "deployment": "example",
                            "service": "example",
                            "locations": 2,
                            "workdir": "/path/to/workdir",
                        },
                    }
                ],
            }
        },
        "bindingFilters": {
            "example": {
                "type": "shuffle",
                "config": {},
            }
        },
        "checkpointManager": {"type": "default", "config": {}},
        "database": {"type": "sqlite", "config": {"connection": ":memory:"}},
        "dataManager": {"type": "default", "config": {}},
        "deployments": {
            "example": {
                "type": "slurm",
                "config": {"maxConcurrentJobs": 10},
                "scheduling_policy": "data_locality",
            }
        },
        "deploymentManager": {"type": "default", "config": {}},
        "failureManager": {"type": "default", "config": {}},
        "scheduling": {
            "scheduler": {"type": "default", "config": {}},
            "policies": {"example": {"type": "data_locality", "config": {}}},
        },
    }
    SfValidator().validate(config)


def test_ext_support_deprecated():
    """Check that all deprecated extension points are still supported."""
    config = {
        "version": "v1.0",
        "workflows": {
            "example": {
                "type": "cwl",
                "config": {"file": "main.cwl", "settings": "config.yml"},
                "bindings": [
                    {
                        "step": "/",
                        "target": {
                            "model": "example",
                            "resources": 2,
                        },
                    }
                ],
            }
        },
        "models": {
            "example": {
                "type": "docker",
                "config": {"image": "alpine:latest"},
            }
        },
    }
    SfValidator().validate(config)


def test_ext_fail_unsupported_extension_point():
    """Check that validation fails when an unsupported extension point is specified."""
    config = {
        "version": "v1.0",
        "workflows": {
            "example": {
                "type": "cwl",
                "config": {"file": "main.cwl", "settings": "config.yml"},
            }
        },
        "unsupported_ext": {"type": "unsupported", "config": {}},
    }
    with raises(WorkflowDefinitionException):
        SfValidator().validate(config)


def test_ext_fail_unsupoorted_type():
    """Check that validation fails when an extension point with unsupported type is specified."""
    config = {
        "version": "v1.0",
        "workflows": {
            "example": {
                "type": "cwl",
                "config": {"file": "main.cwl", "settings": "config.yml"},
            }
        },
        "deployments": {"type": "unsupported", "config": {}},
    }
    with raises(WorkflowDefinitionException):
        SfValidator().validate(config)


def test_schema_generation():
    """Check that the `streamflow schema` command generates a correct JSON Schema."""
    assert (
        hashlib.sha256(SfSchema().dump("v1.0", False).encode()).hexdigest()
        == "1905364613920875ae79e3d93647477d5e5872fd79bfb5bf08576ccdaf72ea4c"
    )
    assert (
        hashlib.sha256(SfSchema().dump("v1.0", True).encode()).hexdigest()
        == "6afb9fec0c4f90bdb59800f6e6b85f4cde80408acf58759078bd6fb790d57413"
    )


def test_schema_generation_fail_invalid_version():
    """Check that the `streamflow schema` command fails when an invalid version is passed."""
    with raises(WorkflowDefinitionException):
        SfSchema().dump("invalid", False)


def test_target_fail_unsupported_property():
    """Check that validation fails when an unsupported property is specified in the `target` clause."""
    config = {
        "version": "v1.0",
        "workflows": {
            "example": {
                "type": "cwl",
                "config": {"file": "main.cwl", "settings": "config.yml"},
                "bindings": [
                    {
                        "step": "/",
                        "target": {
                            "deployment": "example",
                            "unsupported": {},
                        },
                    }
                ],
            }
        },
        "deployments": {
            "example": {
                "type": "docker",
                "config": {"image": "alpine:latest"},
            }
        },
    }
    with raises(WorkflowDefinitionException):
        SfValidator().validate(config)


def test_version_fail_invalid():
    """Check that validation fails when `version` is not supported."""
    config = {
        "version": "v1000.0",
        "workflows": {
            "example": {
                "type": "cwl",
                "config": {"file": "main.cwl", "settings": "config.yml"},
            }
        },
    }
    with raises(WorkflowDefinitionException):
        SfValidator().validate(config)


def test_version_fail_missing():
    """Check that validation fails when the `version` clause is not specified."""
    config = {
        "workflows": {
            "example": {
                "type": "cwl",
                "config": {"file": "main.cwl", "settings": "config.yml"},
            }
        }
    }
    with raises(WorkflowDefinitionException):
        SfValidator().validate(config)


def test_workflow_fail_unsupported_type():
    """Check that validation fails when a workflow with unsupported type is specified."""
    config = {
        "version": "v1.0",
        "workflows": {
            "example": {
                "type": "unsupported",
                "config": {},
            }
        },
    }
    with raises(WorkflowDefinitionException):
        SfValidator().validate(config)


def test_sf_context():
    assert set(SfSchema().get_config("v1.0").contents["properties"].keys()) == {
        "bindingFilters",
        "checkpointManager",
        "dataManager",
        "database",
        "deploymentManager",
        "deployments",
        "failureManager",
        "models",
        "scheduling",
        "version",
        "workflows",
    }
    config = {
        "checkpointManager": {
            "type": "default",
            "config": {"checkpoint_dir": "/home/resilient_volume"},
        },
        "database": {"type": "default", "config": {"connection": ":memory:"}},
        "dataManager": {"type": "default", "config": {}},
        "deploymentManager": {"type": "default", "config": {}},
        "failureManager": {
            "type": "default",
            "config": {"max_retries": 10, "retry_delay": 61},
        },
        "scheduler": {"type": "default", "config": {"retry_delay": 101}},
    }
    context = build_context(config)
    assert (
        cast(DefaultCheckpointManager, context.checkpoint_manager).checkpoint_dir
        == config["checkpointManager"]["config"]["checkpoint_dir"]
    )
    assert (
        cast(SqliteDatabase, context.database).connection.connection
        == config["database"]["config"]["connection"]
    )
    assert (
        cast(DefaultFailureManager, context.failure_manager).retry_delay
        == config["failureManager"]["config"]["retry_delay"]
    )
    assert (
        cast(DefaultFailureManager, context.failure_manager).max_retries
        == config["failureManager"]["config"]["max_retries"]
    )
    assert (
        cast(DefaultScheduler, context.scheduler).retry_interval
        == config["scheduler"]["config"]["retry_delay"]
    )
