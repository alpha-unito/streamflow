import logging
import sys
from abc import ABC, abstractmethod
from collections.abc import MutableMapping, MutableSequence
from typing import Any

from streamflow.config import ext_schemas
from streamflow.core.data import DataManager
from streamflow.core.deployment import BindingFilter, Connector, DeploymentManager
from streamflow.core.persistence import Database
from streamflow.core.recovery import CheckpointManager, FailureManager
from streamflow.core.scheduling import Policy, Scheduler
from streamflow.cwl.requirement.docker import cwl_docker_translator_classes
from streamflow.cwl.requirement.docker.translator import CWLDockerTranslator
from streamflow.data import data_manager_classes
from streamflow.deployment import deployment_manager_classes
from streamflow.deployment.connector import connector_classes
from streamflow.deployment.filter import binding_filter_classes
from streamflow.log_handler import logger
from streamflow.persistence import database_classes
from streamflow.recovery import checkpoint_manager_classes, failure_manager_classes
from streamflow.scheduling import scheduler_classes
from streamflow.scheduling.policy import policy_classes

if sys.version_info < (3, 11):
    from importlib.abc import Traversable
else:
    from importlib.resources.abc import Traversable


extension_points: MutableMapping[str, MutableMapping[str, Any]] = {
    "binding_filter": binding_filter_classes,
    "checkpoint_manager": checkpoint_manager_classes,
    "cwl_docker_translator": cwl_docker_translator_classes,
    "connector": connector_classes,
    "data_manager": data_manager_classes,
    "database": database_classes,
    "deployment_manager": deployment_manager_classes,
    "failure_manager": failure_manager_classes,
    "policy": policy_classes,
    "scheduler": scheduler_classes,
}


class StreamFlowPlugin(ABC):
    def __init__(self) -> None:
        self.classes_: MutableMapping[str, MutableSequence[Any]] = {}

    def _register(self, name: str, cls: type, extension_point: str) -> None:
        self.classes_.setdefault(extension_point, []).append(
            {
                "name": name,
                "class": cls,
            }
        )
        if name in extension_points[extension_point]:
            if logger.isEnabledFor(logging.WARNING):
                logger.warning(
                    "{} is already installed and will be overridden by {}".format(
                        name, self.__class__.__module__ + "." + self.__class__.__name__
                    )
                )
        extension_points[extension_point][name] = cls

    @abstractmethod
    def register(self) -> None: ...

    def register_binding_filter(self, name: str, cls: type[BindingFilter]) -> None:
        self._register(name, cls, "binding_filter")

    def register_checkpoint_manager(
        self, name: str, cls: type[CheckpointManager]
    ) -> None:
        self._register(name, cls, "checkpoint_manager")

    def register_cwl_docker_translator(
        self, name: str, cls: type[CWLDockerTranslator]
    ) -> None:
        self._register(name, cls, "cwl_docker_translator")

    def register_connector(self, name: str, cls: type[Connector]) -> None:
        self._register(name, cls, "connector")

    def register_data_manager(self, name: str, cls: type[DataManager]) -> None:
        self._register(name, cls, "data_manager")

    def register_database(self, name: str, cls: type[Database]) -> None:
        self._register(name, cls, "database")

    def register_deployment_manager(
        self, name: str, cls: type[DeploymentManager]
    ) -> None:
        self._register(name, cls, "deployment_manager")

    def register_failure_manager(self, name: str, cls: type[FailureManager]) -> None:
        self._register(name, cls, "failure_manager")

    def register_policy(self, name: str, cls: type[Policy]) -> None:
        self._register(name, cls, "policy")

    def register_scheduler(self, name: str, cls: type[Scheduler]) -> None:
        self._register(name, cls, "scheduler")

    def register_schema(self, schema: Traversable) -> None:
        ext_schemas.append(schema)
