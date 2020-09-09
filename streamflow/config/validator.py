from __future__ import annotations

import os
from typing import TYPE_CHECKING

from jsonref import loads
from jsonschema import Draft7Validator
from ruamel.yaml import YAML

if TYPE_CHECKING:
    from typing import Any, MutableMapping
    from typing_extensions import Text


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


def handle_errors(errors, filename):
    errors = list(sorted(errors, key=str))
    if not errors:
        return
    raise Exception(
        "The {file_msg} file is invalid because:\n{error_msg}".format(
            file_msg="'{}'".format(filename) if filename else "",
            error_msg=errors[0]))


class SfValidator(object):

    def __init__(self) -> None:
        super().__init__()
        self.yaml = YAML(typ='safe')

    def validate(self, streamflow_file: Text) -> MutableMapping[Text, Any]:
        with open(streamflow_file) as f:
            streamflow_config = self.yaml.load(f)
        schema = load_jsonschema(streamflow_config)
        validator = Draft7Validator(schema)
        handle_errors(
            validator.iter_errors(streamflow_config), streamflow_file)
        return streamflow_config
