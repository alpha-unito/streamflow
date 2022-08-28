from __future__ import annotations

from abc import ABC, abstractmethod
from typing import MutableMapping, Type

from streamflow.core.deployment import Connector
from streamflow.core.scheduling import Policy, Scheduler
from streamflow.deployment.connector import connector_classes
from streamflow.log_handler import logger
from streamflow.scheduling import scheduler_classes
from streamflow.scheduling.policy import policy_classes


class StreamFlowPlugin(ABC):

    def _register(self,
                  name: str,
                  cls: Type,
                  classes: MutableMapping[str, Type]):
        if name in classes:
            logger.warning("{} is already installed and will be overridden by {}".format(
                name, self.__class__.__module__ + "." + self.__class__.__name__))
        classes[name] = cls

    @abstractmethod
    def register(self) -> None:
        ...

    def register_connector(self,
                           name: str,
                           cls: Type[Connector]):
        self._register(name, cls, connector_classes)

    def register_policy(self,
                        name: str,
                        cls: Type[Policy]):
        self._register(name, cls, policy_classes)

    def register_scheduler(self,
                           cls: Type[Scheduler]):
        self._register(cls.__class__.__module__ + '.' + cls.__class__.__name__, cls, scheduler_classes)
