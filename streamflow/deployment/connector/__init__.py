from streamflow.deployment.connector.container import (
    DockerComposeConnector,
    DockerConnector,
    SingularityConnector,
)
from streamflow.deployment.connector.kubernetes import (
    Helm3Connector,
    KubernetesConnector,
)
from streamflow.deployment.connector.local import LocalConnector
from streamflow.deployment.connector.occam import OccamConnector
from streamflow.deployment.connector.queue_manager import (
    FluxConnector,
    PBSConnector,
    SlurmConnector,
)
from streamflow.deployment.connector.ssh import SSHConnector

connector_classes = {
    "docker": DockerConnector,
    "docker-compose": DockerComposeConnector,
    "flux": FluxConnector,
    "helm": Helm3Connector,
    "helm3": Helm3Connector,
    "kubernetes": KubernetesConnector,
    "local": LocalConnector,
    "occam": OccamConnector,
    "pbs": PBSConnector,
    "singularity": SingularityConnector,
    "slurm": SlurmConnector,
    "ssh": SSHConnector,
}
