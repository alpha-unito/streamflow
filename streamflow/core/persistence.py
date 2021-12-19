from __future__ import annotations

from abc import ABC, abstractmethod
from typing import MutableMapping, Any, MutableSequence, Tuple

import pandas as pd


class Database(ABC):

    @abstractmethod
    def add_step(self, name: str, status: int) -> int:
        ...

    @abstractmethod
    def update_step(self, step_id: int, updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    def get_steps(self) -> pd.DataFrame:
        ...

    @abstractmethod
    def add_command(self, step_id: int, cmd: str) -> int:
        ...

    @abstractmethod
    def update_command(self, command_id: int, updates: MutableMapping[str, Any]):
        ...

    @abstractmethod
    def get_report(self) -> pd.DataFrame:
        ...


class PersistenceManager(ABC):

    def __init__(self,
                 db: Database):
        self.db: Database = db
