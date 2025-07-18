from __future__ import annotations

from collections.abc import MutableMapping

from streamflow.core.deployment import DeploymentManager
from streamflow.deployment.manager import DefaultDeploymentManager

deployment_manager_classes: MutableMapping[str, type[DeploymentManager]] = {
    "default": DefaultDeploymentManager
}
