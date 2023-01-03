from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor
from typing import Any, MutableMapping, TYPE_CHECKING

from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.data import DataManager
    from streamflow.core.deployment import DeploymentManager
    from streamflow.core.persistence import Database
    from streamflow.core.recovery import CheckpointManager, FailureManager
    from streamflow.core.scheduling import Scheduler


class SchemaEntity(ABC):
    @classmethod
    @abstractmethod
    def get_schema(cls) -> str:
        ...


class StreamFlowContext:
    def __init__(self, config: MutableMapping[str, Any]):
        self.config: MutableMapping[str, Any] = config
        self.checkpoint_manager: CheckpointManager | None = None
        self.database: Database | None = None
        self.data_manager: DataManager | None = None
        self.deployment_manager: DeploymentManager | None = None
        self.failure_manager: FailureManager | None = None
        self.process_executor: ProcessPoolExecutor = ProcessPoolExecutor()
        self.scheduler: Scheduler | None = None

    async def close(self):
        try:
            await asyncio.gather(
                self.checkpoint_manager.close(),
                self.data_manager.close(),
                self.deployment_manager.close(),
                self.failure_manager.close(),
                self.scheduler.close(),
            )
        except Exception as e:
            logger.exception(e)
        finally:
            await self.database.close()
