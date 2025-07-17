from collections.abc import Iterable, MutableMapping
from typing import Any

from jsonschema import ValidationError
from jsonschema.validators import validator_for
from ruamel.yaml import YAML

from streamflow.config.schema import SfSchema
from streamflow.core.exception import WorkflowDefinitionException


def handle_errors(errors: Iterable[ValidationError]) -> None:
    if not (errors := list(sorted(errors, key=str))):
        return
    raise WorkflowDefinitionException(
        "The StreamFlow configuration is invalid because:\n{error_msgs}".format(
            error_msgs="\n".join([f" - {err}" for err in errors])
        )
    )


class SfValidator:
    def __init__(self) -> None:
        super().__init__()
        self.schema: SfSchema = SfSchema()
        self.yaml = YAML(typ="safe")

    def validate_file(self, streamflow_file: str) -> MutableMapping[str, Any]:
        with open(streamflow_file) as f:
            streamflow_config = self.yaml.load(f)
        return self.validate(streamflow_config)

    def validate(
        self, streamflow_config: MutableMapping[str, Any]
    ) -> MutableMapping[str, Any]:
        if "version" not in streamflow_config:
            raise WorkflowDefinitionException(
                "The `version` clause is mandatory and should be equal to `v1.0`."
            )
        config = self.schema.get_config(streamflow_config["version"]).contents
        cls = validator_for(config)
        validator = cls(config, registry=self.schema.registry)
        handle_errors(validator.iter_errors(streamflow_config))
        return streamflow_config
