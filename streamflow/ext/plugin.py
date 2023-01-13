from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import MutableMapping

from streamflow.core.deployment import BindingFilter, Connector
from streamflow.core.scheduling import Policy, Scheduler
from streamflow.deployment.connector import connector_classes
from streamflow.deployment.filter import binding_filter_classes
from streamflow.log_handler import logger
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

    def register_binding_order(self, name: str, cls: type[BindingFilter]):
        self._register(name, cls, binding_filter_classes)

    def register_connector(self, name: str, cls: type[Connector]):
        self._register(name, cls, connector_classes)

    def register_policy(self, name: str, cls: type[Policy]):
        self._register(name, cls, policy_classes)

    def register_scheduler(self, cls: type[Scheduler]):
        self._register(
            cls.__class__.__module__ + "." + cls.__class__.__name__,
            cls,
            scheduler_classes,
        )
