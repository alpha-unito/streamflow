import hashlib

from pytest import raises

from streamflow.config.schema import SfSchema
from streamflow.config.validator import SfValidator
from streamflow.core.exception import WorkflowDefinitionException


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
        == "de0e5736eaa46a70b4d9b28e2faa7b235b2d965886fa1b8bfe80428d131ee31b"
    )
    assert (
        hashlib.sha256(SfSchema().dump("v1.0", True).encode()).hexdigest()
        == "5aef0ec1925e490075e126d0bc38ed987391cbf10b401bea3ca3a1f4ccb0c0fd"
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
