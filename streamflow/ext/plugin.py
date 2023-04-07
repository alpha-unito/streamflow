from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import MutableMapping

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


class StreamFlowPlugin(ABC):
    def _register(self, name: str, cls: type, classes: MutableMapping[str, type]):
        if name in classes:
            if logger.isEnabledFor(logging.WARN):
                logger.warn(
                    "{} is already installed and will be overridden by {}".format(
                        name, self.__class__.__module__ + "." + self.__class__.__name__
                    )
                )
        classes[name] = cls

    @abstractmethod
    def register(self) -> None:
        ...

    def register_binding_filter(self, name: str, cls: type[BindingFilter]):
        self._register(name, cls, binding_filter_classes)

    def register_checkpoint_manager(self, name: str, cls: type[CheckpointManager]):
        self._register(name, cls, checkpoint_manager_classes)

    def register_cwl_docker_translator(self, name: str, cls: type[CWLDockerTranslator]):
        self._register(name, cls, cwl_docker_translator_classes)

    def register_connector(self, name: str, cls: type[Connector]):
        self._register(name, cls, connector_classes)

    def register_data_manager(self, name: str, cls: type[DataManager]):
        self._register(name, cls, data_manager_classes)

    def register_database(self, name: str, cls: type[Database]):
        self._register(name, cls, database_classes)

    def register_deployment_manager(self, name: str, cls: type[DeploymentManager]):
        self._register(name, cls, deployment_manager_classes)

    def register_failure_manager(self, name: str, cls: type[FailureManager]):
        self._register(name, cls, failure_manager_classes)

    def register_policy(self, name: str, cls: type[Policy]):
        self._register(name, cls, policy_classes)

    def register_scheduler(self, name: str, cls: type[Scheduler]):
        self._register(name, cls, scheduler_classes)
