import json
import os
from typing import Any, MutableMapping

from jsonschema import Draft7Validator
from ruamel.yaml import YAML


def load_jsonschema(config_file):
    filename = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'schemas',
        config_file['version'], "config_schema.json")
    if not os.path.exists(filename):
        raise Exception(
            'Version in "{}" is unsupported'.format(config_file.filename))
    with open(filename, "r") as f:
        return json.load(f)


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

    def validate(self, streamflow_file: str) -> MutableMapping[str, Any]:
        with open(streamflow_file) as f:
            streamflow_config = self.yaml.load(f)
        schema = load_jsonschema(streamflow_config)
        validator = Draft7Validator(schema)
        handle_errors(
            validator.iter_errors(streamflow_config), streamflow_file)
        config_file_path = os.path.abspath(streamflow_file)
        streamflow_config['config_file'] = {
            'path': config_file_path,
            'basename': os.path.basename(config_file_path),
            'dirname': os.path.dirname(config_file_path)
        }
        return streamflow_config
