from __future__ import annotations

import os
from typing import TYPE_CHECKING

import jsonref
import pkg_resources
from jsonschema.validators import validator_for
from ruamel.yaml import YAML

from streamflow.core import utils
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.cwl.requirement.docker import cwl_docker_translator_classes
from streamflow.data import data_manager_classes
from streamflow.deployment import deployment_manager_classes
from streamflow.deployment.connector import connector_classes
from streamflow.deployment.filter import binding_filter_classes
from streamflow.persistence import database_classes
from streamflow.recovery import checkpoint_manager_classes, failure_manager_classes
from streamflow.scheduling import scheduler_classes
from streamflow.scheduling.policy import policy_classes

if TYPE_CHECKING:
    from typing import Any, MutableMapping


def handle_errors(errors):
    errors = list(sorted(errors, key=str))
    if not errors:
        return
    raise WorkflowDefinitionException(
        "The StreamFlow configuration is invalid because:\n{error_msgs}".format(
            error_msgs="\n".join([f" - {err}" for err in errors])
        )
    )


def load_jsonschema(config_file: MutableMapping[str, Any]):
    if "version" not in config_file:
        raise WorkflowDefinitionException(
            "The `version` clause is mandatory and should be equal to `v1.0`."
        )
    try:
        base_path = pkg_resources.resource_filename(
            __name__, os.path.join("schemas", config_file["version"])
        )
    except pkg_resources.ResolutionError:
        raise WorkflowDefinitionException(
            f"Version {config_file['version']} is unsupported. The `version` clause should be equal to `v1.0`."
        )
    filename = os.path.join(base_path, "config_schema.json")
    if not os.path.exists(filename):
        raise WorkflowDefinitionException(
            f"Version {config_file['version']} is unsupported. The `version` clause should be equal to `v1.0`."
        )
    with open(os.path.join(base_path, "config_schema.json")) as f:
        return jsonref.loads(f.read(), base_uri=f"file://{base_path}/", jsonschema=True)


class SfValidator:
    def __init__(self) -> None:
        super().__init__()
        self.yaml = YAML(typ="safe")

    def validate_file(self, streamflow_file: str) -> MutableMapping[str, Any]:
        with open(streamflow_file) as f:
            streamflow_config = self.yaml.load(f)
        return self.validate(streamflow_config)

    def validate(self, streamflow_config: MutableMapping[str, Any]):
        schema = load_jsonschema(streamflow_config)
        utils.inject_schema(schema, binding_filter_classes, "bindingFilter")
        utils.inject_schema(schema, checkpoint_manager_classes, "checkpointManager")
        utils.inject_schema(schema, cwl_docker_translator_classes, "cwl/docker")
        utils.inject_schema(schema, database_classes, "database")
        utils.inject_schema(schema, data_manager_classes, "dataManager")
        utils.inject_schema(schema, connector_classes, "deployment")
        utils.inject_schema(schema, deployment_manager_classes, "deploymentManager")
        utils.inject_schema(schema, failure_manager_classes, "failureManager")
        utils.inject_schema(schema, policy_classes, "policy")
        utils.inject_schema(schema, scheduler_classes, "scheduler")
        cls = validator_for(schema)
        validator = cls(schema)
        handle_errors(validator.iter_errors(streamflow_config))
        return streamflow_config
