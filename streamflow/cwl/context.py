from pathlib import PurePosixPath

from cwltool.context import RuntimeContext, LoadingContext

from streamflow.connector.deployment_manager import DeploymentManager
from streamflow.data.data_manager import DataManager
from streamflow.scheduling.scheduler import Scheduler


class SfLoadingContext(LoadingContext):

    def __init__(self, kwargs=None):
        super().__init__(kwargs)
        self.streamflow_config = None
        self.step_path = PurePosixPath('/')


class SfRuntimeContext(RuntimeContext):

    def __init__(self, kwargs=None):
        super().__init__(kwargs)
        self.streamflow_config = None
        self.deployment_manager = DeploymentManager()
        self.scheduler = Scheduler()
        self.data_manager = DataManager(self.scheduler, self.deployment_manager)
