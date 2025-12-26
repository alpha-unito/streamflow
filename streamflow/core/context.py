from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Any

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
    def get_schema(cls) -> str: ...


class StreamFlowContext:
    def __init__(
        self,
        config: MutableMapping[str, Any],
        checkpoint_manager_class: type[CheckpointManager],
        database_class: type[Database],
        data_manager_class: type[DataManager],
        deployment_manager_class: type[DeploymentManager],
        failure_manager_class: type[FailureManager],
        scheduler_class: type[Scheduler],
    ):
        self.config: MutableMapping[str, Any] = config
        self.checkpoint_manager: CheckpointManager = checkpoint_manager_class(
            context=self, **config.get("checkpointManager", {}).get("config", {})
        )
        self.database: Database = database_class(
            context=self, **config.get("database", {}).get("config", {})
        )
        self.data_manager: DataManager = data_manager_class(
            context=self, **config.get("dataManager", {}).get("config", {})
        )
        self.deployment_manager: DeploymentManager = deployment_manager_class(
            context=self, **config.get("deploymentManager", {}).get("config", {})
        )
        self.failure_manager: FailureManager = failure_manager_class(
            context=self, **config.get("failureManager", {}).get("config", {})
        )
        self.process_executor: ProcessPoolExecutor = ProcessPoolExecutor()
        self.scheduler: Scheduler = scheduler_class(
            context=self,
            **config.get("scheduling", {}).get("scheduler", {}).get("config", {}),
        )

    async def close(self) -> None:
        try:
            await asyncio.gather(
                asyncio.create_task(self.checkpoint_manager.close()),
                asyncio.create_task(self.data_manager.close()),
                asyncio.create_task(self.deployment_manager.close()),
                asyncio.create_task(self.failure_manager.close()),
                asyncio.create_task(self.scheduler.close()),
            )
        except Exception as e:
            logger.exception(e)
        finally:
            await self.database.close()
