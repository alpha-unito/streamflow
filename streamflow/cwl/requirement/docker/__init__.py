from collections.abc import MutableMapping

from streamflow.cwl.requirement.docker.docker import DockerCWLDockerTranslator
from streamflow.cwl.requirement.docker.kubernetes import KubernetesCWLDockerTranslator
from streamflow.cwl.requirement.docker.nocontainer import NoContainerCWLDockerTranslator
from streamflow.cwl.requirement.docker.singularity import SingularityCWLDockerTranslator
from streamflow.cwl.requirement.docker.translator import CWLDockerTranslator

cwl_docker_translator_classes: MutableMapping[str, type[CWLDockerTranslator]] = {
    "default": DockerCWLDockerTranslator,
    "docker": DockerCWLDockerTranslator,
    "kubernetes": KubernetesCWLDockerTranslator,
    "none": NoContainerCWLDockerTranslator,
    "singularity": SingularityCWLDockerTranslator,
}
