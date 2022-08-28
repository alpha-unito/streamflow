from abc import ABC, abstractmethod
from typing import Any, MutableMapping


class Config(object):
    __slots__ = ('name', 'type', 'config')

    def __init__(self,
                 name: str,
                 type: str,
                 config: MutableMapping[str, Any]) -> None:
        super().__init__()
        self.name: str = name
        self.type: str = type
        self.config: MutableMapping[str, Any] = config or {}


class SchemaEntity(ABC):

    @classmethod
    @abstractmethod
    def get_schema(cls) -> str:
        ...
