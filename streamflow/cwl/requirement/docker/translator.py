from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, MutableMapping

from streamflow.core.config import Config
from streamflow.core.context import SchemaEntity
from streamflow.core.deployment import Target


class CWLDockerTranslatorConfig(Config):
    def __init__(
        self,
        name: str,
        type: str,
        config: MutableMapping[str, Any],
        wrapper: bool = True,
    ):
        super().__init__(name, type, config)
        self.wrapper: bool = wrapper


class CWLDockerTranslator(SchemaEntity, ABC):
    def __init__(self, config_dir: str, wrapper: bool):
        self.config_dir: str = config_dir
        self.wrapper: bool = wrapper

    @abstractmethod
    def get_target(
        self,
        image: str,
        output_directory: str | None,
        network_access: bool,
        target: Target,
    ) -> Target:
        ...
