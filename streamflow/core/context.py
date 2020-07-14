from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.data import DataManager
    from streamflow.core.deployment import DeploymentManager
    from streamflow.core.scheduling import Scheduler
    from typing import Optional
    from typing_extensions import Text


class StreamflowContext(object):

    def __init__(self, streamflow_config_dir: Text):
        self.config_dir = streamflow_config_dir
        self.data_manager: Optional[DataManager] = None
        self.deployment_manager: Optional[DeploymentManager] = None
        self.scheduler: Optional[Scheduler] = None
