from streamflow.deployment.connector.container import DockerComposeConnector, DockerConnector, SingularityConnector
from streamflow.deployment.connector.kubernetes import Helm3Connector
from streamflow.deployment.connector.local import LocalConnector
from streamflow.deployment.connector.occam import OccamConnector
from streamflow.deployment.connector.queue_manager import PBSConnector, SlurmConnector
from streamflow.deployment.connector.ssh import SSHConnector

connector_classes = {
    'docker': DockerConnector,
    'docker-compose': DockerComposeConnector,
    'helm': Helm3Connector,
    'helm3': Helm3Connector,
    'local': LocalConnector,
    'occam': OccamConnector,
    'pbs': PBSConnector,
    'singularity': SingularityConnector,
    'slurm': SlurmConnector,
    'ssh': SSHConnector
}
