from collections.abc import MutableSequence
from importlib.resources import files
from importlib.resources.abc import Traversable

ext_schemas: MutableSequence[Traversable] = [
    files("streamflow.deployment.connector")
    .joinpath("schemas")
    .joinpath("base")
    .joinpath("docker.json"),
    files("streamflow.deployment.connector")
    .joinpath("schemas")
    .joinpath("base")
    .joinpath("helm.json"),
    files("streamflow.deployment.connector")
    .joinpath("schemas")
    .joinpath("base")
    .joinpath("kubernetes.json"),
    files("streamflow.deployment.connector")
    .joinpath("schemas")
    .joinpath("base")
    .joinpath("queue_manager.json"),
    files("streamflow.deployment.connector")
    .joinpath("schemas")
    .joinpath("base")
    .joinpath("singularity.json"),
    files("streamflow.deployment.connector")
    .joinpath("schemas")
    .joinpath("base")
    .joinpath("ssh.json"),
]
