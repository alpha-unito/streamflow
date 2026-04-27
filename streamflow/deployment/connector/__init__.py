from __future__ import annotations

from collections.abc import MutableMapping

from streamflow.core.deployment import Connector
from streamflow.deployment.connector.container import (
    DockerComposeConnector,
    DockerConnector,
    SingularityConnector,
)
from streamflow.deployment.connector.kubernetes import (
    Helm3Connector,
    Helm4Connector,
    KubernetesConnector,
)
from streamflow.deployment.connector.local import LocalConnector
from streamflow.deployment.connector.queue_manager import (
    FluxConnector,
    PBSConnector,
    SlurmConnector,
)
from streamflow.deployment.connector.ssh import SSHConnector

connector_classes: MutableMapping[str, type[Connector]] = {
    "docker": DockerConnector,
    "docker-compose": DockerComposeConnector,
    "flux": FluxConnector,
    "helm": Helm4Connector,
    "helm3": Helm3Connector,
    "helm4": Helm4Connector,
    "kubernetes": KubernetesConnector,
    "local": LocalConnector,
    "pbs": PBSConnector,
    "singularity": SingularityConnector,
    "slurm": SlurmConnector,
    "ssh": SSHConnector,
}
