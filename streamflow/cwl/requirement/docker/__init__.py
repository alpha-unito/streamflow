from streamflow.cwl.requirement.docker.docker import DockerCWLDockerTranslator
from streamflow.cwl.requirement.docker.kubernetes import KubernetesCWLDockerTranslator
from streamflow.cwl.requirement.docker.singularity import SingularityCWLDockerTranslator

cwl_docker_translator_classes = {
    "default": DockerCWLDockerTranslator,
    "docker": DockerCWLDockerTranslator,
    "kubernetes": KubernetesCWLDockerTranslator,
    "singularity": SingularityCWLDockerTranslator,
}
