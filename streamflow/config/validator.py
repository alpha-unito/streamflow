from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pkg_resources
from jsonref import loads
from jsonschema import Draft7Validator
from ruamel.yaml import YAML

from streamflow.core import utils
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.data import data_manager_classes
from streamflow.deployment import deployment_manager_classes
from streamflow.deployment.connector import connector_classes
from streamflow.persistence import database_classes
from streamflow.recovery import checkpoint_manager_classes, failure_manager_classes
from streamflow.scheduling.policy import policy_classes

if TYPE_CHECKING:
    from typing import Any, MutableMapping


def handle_errors(errors):
    errors = list(sorted(errors, key=str))
    if not errors:
        return
    raise WorkflowDefinitionException(
        "The StreamFlow configuration is invalid because:\n{error_msgs}".format(
            error_msgs="\n".join([" - {msg}".format(msg=err) for err in errors])))


def load_jsonschema(config_file: MutableMapping[str, Any]):
    try:
        base_path = pkg_resources.resource_filename(
            __name__, os.path.join('schemas', config_file['version']))
    except pkg_resources.ResolutionError:
        raise Exception(
            'Version {} is unsupported'.format(config_file['version']))
    filename = os.path.join(base_path, "config_schema.json")
    if not os.path.exists(filename):
        raise Exception(
            'Version in "{}" is unsupported'.format(filename))
    with open(filename, "r") as f:
        return loads(f.read(), base_uri='file://{}/'.format(base_path), jsonschema=True)


class SfValidator(object):

    def __init__(self) -> None:
        super().__init__()
        self.yaml = YAML(typ='safe')

    def validate_file(self, streamflow_file: str) -> MutableMapping[str, Any]:
        with open(streamflow_file) as f:
            streamflow_config = self.yaml.load(f)
        return self.validate(streamflow_config)

    def validate(self, streamflow_config: MutableMapping[str, Any]):
        schema = load_jsonschema(streamflow_config)
        utils.inject_schema(schema, checkpoint_manager_classes, 'checkpointManager')
        utils.inject_schema(schema, database_classes, 'database')
        utils.inject_schema(schema, data_manager_classes, 'dataManager')
        utils.inject_schema(schema, connector_classes, 'deployment')
        utils.inject_schema(schema, deployment_manager_classes, 'deploymentManager')
        utils.inject_schema(schema, failure_manager_classes, 'failureManager')
        utils.inject_schema(schema, policy_classes, 'policy')
        validator = Draft7Validator(schema)
        handle_errors(validator.iter_errors(streamflow_config))
        return streamflow_config
