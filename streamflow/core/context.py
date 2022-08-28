from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING

from streamflow.core.recovery import CheckpointManager, FailureManager
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.data import DataManager
    from streamflow.core.deployment import DeploymentManager
    from streamflow.core.persistence import Database
    from streamflow.core.scheduling import Scheduler
    from typing import Optional


class StreamFlowContext(object):

    def __init__(self, streamflow_config_dir: str):
        self.config_dir = streamflow_config_dir
        self.checkpoint_manager: Optional[CheckpointManager] = None
        self.database: Optional[Database] = None
        self.data_manager: Optional[DataManager] = None
        self.deployment_manager: Optional[DeploymentManager] = None
        self.failure_manager: Optional[FailureManager] = None
        self.process_executor: ProcessPoolExecutor = ProcessPoolExecutor()
        self.scheduler: Optional[Scheduler] = None

    async def close(self):
        try:
            await asyncio.gather(
                self.checkpoint_manager.close(),
                self.data_manager.close(),
                self.deployment_manager.close(),
                self.failure_manager.close(),
                self.scheduler.close())
        except BaseException as e:
            logger.exception(e)
        finally:
            await self.database.close()
