from __future__ import annotations

import os
from typing import TYPE_CHECKING

from jsonref import loads
from jsonschema import Draft7Validator
from ruamel.yaml import YAML

from streamflow.core.exception import WorkflowDefinitionException

if TYPE_CHECKING:
    from typing import Any, MutableMapping


def load_jsonschema(config_file):
    base_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'schemas', config_file['version'])
    filename = os.path.join(base_path, "config_schema.json")
    if not os.path.exists(filename):
        raise Exception(
            'Version in "{}" is unsupported'.format(filename))
    with open(filename, "r") as f:
        return loads(f.read(), base_uri='file://{}/'.format(base_path), jsonschema=True)


def handle_errors(errors):
    errors = list(sorted(errors, key=str))
    if not errors:
        return
    raise WorkflowDefinitionException(
        "The StreamFlow configuration is invalid because:\n{error_msgs}".format(
            error_msgs="\n".join([" - {msg}".format(msg=err) for err in errors])))


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
        validator = Draft7Validator(schema)
        handle_errors(validator.iter_errors(streamflow_config))
        return streamflow_config
