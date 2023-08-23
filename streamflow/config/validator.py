from __future__ import annotations

from typing import TYPE_CHECKING

from jsonschema.validators import validator_for
from ruamel.yaml import YAML

from streamflow.config.schema import SfSchema
from streamflow.core.exception import WorkflowDefinitionException

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


class SfValidator:
    def __init__(self) -> None:
        super().__init__()
        self.schema: SfSchema = SfSchema()
        self.yaml = YAML(typ="safe")

    def validate_file(self, streamflow_file: str) -> MutableMapping[str, Any]:
        with open(streamflow_file) as f:
            streamflow_config = self.yaml.load(f)
        return self.validate(streamflow_config)

    def validate(self, streamflow_file: MutableMapping[str, Any]):
        if "version" not in streamflow_file:
            raise WorkflowDefinitionException(
                "The `version` clause is mandatory and should be equal to `v1.0`."
            )
        config = self.schema.get_config(streamflow_file["version"]).contents
        cls = validator_for(config)
        validator = cls(config, registry=self.schema.registry)
        handle_errors(validator.iter_errors(streamflow_file))
        return streamflow_file
