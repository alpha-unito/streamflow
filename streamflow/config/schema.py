from importlib.resources import files

from streamflow.config import ext_schemas
from streamflow.core.config import Schema
from streamflow.cwl.requirement.docker import cwl_docker_translator_classes
from streamflow.data import data_manager_classes
from streamflow.deployment import deployment_manager_classes
from streamflow.deployment.connector import connector_classes
from streamflow.deployment.filter import binding_filter_classes
from streamflow.persistence import database_classes
from streamflow.recovery import checkpoint_manager_classes, failure_manager_classes
from streamflow.scheduling import scheduler_classes
from streamflow.scheduling.policy import policy_classes


class SfSchema(Schema):
    def __init__(self) -> None:
        super().__init__(
            {
                "v1.0": "https://streamflow.di.unito.it/schemas/config/v1.0/config_schema.json"
            }
        )
        for version in self.configs.keys():
            self.add_schema(
                schema=files(__package__)
                .joinpath("schemas")
                .joinpath(version)
                .joinpath("config_schema.json")
                .read_text("utf-8")
            )
        self.inject_ext(binding_filter_classes, "bindingFilter")
        self.inject_ext(checkpoint_manager_classes, "checkpointManager")
        self.inject_ext(cwl_docker_translator_classes, "cwl/docker")
        self.inject_ext(database_classes, "database")
        self.inject_ext(data_manager_classes, "dataManager")
        self.inject_ext(connector_classes, "deployment")
        self.inject_ext(deployment_manager_classes, "deploymentManager")
        self.inject_ext(failure_manager_classes, "failureManager")
        self.inject_ext(policy_classes, "policy")
        self.inject_ext(scheduler_classes, "scheduler")
        for schema in ext_schemas:
            self.add_schema(schema.read_text("utf-8"), embed=True)
        self._registry = self.registry.crawl()
