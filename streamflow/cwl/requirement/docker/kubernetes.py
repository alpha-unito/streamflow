from __future__ import annotations

import tempfile

from importlib_resources import files
from jinja2 import Template

from streamflow.core import utils
from streamflow.core.deployment import DeploymentConfig, Target
from streamflow.cwl.requirement.docker.translator import CWLDockerTranslator


class KubernetesCWLDockerTranslator(CWLDockerTranslator):
    def __init__(
        self,
        config_dir: str,
        wrapper: bool,
        template: str | None = None,
        debug: bool = False,
        inCluster: bool | None = False,
        kubeconfig: str | None = None,
        kubeContext: str | None = None,
        maxConcurrentConnections: int = 4096,
        namespace: str | None = None,
        locationsCacheSize: int | None = None,
        locationsCacheTTL: int | None = None,
        transferBufferSize: int = (2**25) - 1,
        timeout: int | None = 60000,
        wait: bool = True,
    ):
        super().__init__(config_dir=config_dir, wrapper=wrapper)
        if template is not None:
            with open(template) as t:
                self.template: Template = Template(t.read())
        else:
            self.template: Template = Template(
                files(__package__)
                .joinpath("schemas")
                .joinpath("kubernetes.jinja2")
                .read_text("utf-8")
            )
        self.debug: bool = debug
        self.inCluster: bool = inCluster
        self.kubeconfig: str | None = kubeconfig
        self.kubeContext: str | None = kubeContext
        self.maxConcurrentConnections: int = maxConcurrentConnections
        self.namespace: str | None = namespace
        self.locationsCacheSize: int | None = locationsCacheSize
        self.locationsCacheTTL: int | None = locationsCacheTTL
        self.transferBufferSize: int = transferBufferSize
        self.timeout: int | None = timeout
        self.wait: bool = wait

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("kubernetes.json")
            .read_text("utf-8")
        )

    def get_target(
        self,
        image: str,
        output_directory: str | None,
        network_access: bool,
        target: Target,
    ) -> Target:
        name = utils.random_name()
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            self.template.stream(
                name=name,
                image=image,
                network_access=network_access,
                output_directory=output_directory,
            ).dump(f.name)
            return Target(
                deployment=DeploymentConfig(
                    name=name,
                    type="kubernetes",
                    config={
                        "files": [f.name],
                        "debug": self.debug,
                        "inCluster": self.inCluster,
                        "kubeconfig": self.kubeconfig,
                        "kubeContext": self.kubeContext,
                        "maxConcurrentConnections": self.maxConcurrentConnections,
                        "namespace": self.namespace,
                        "locationsCacheSize": self.locationsCacheSize,
                        "locationsCacheTTL": self.locationsCacheTTL,
                        "transferBufferSize": self.transferBufferSize,
                        "timeout": self.timeout,
                        "wait": self.wait,
                    },
                    workdir="/tmp/streamflow",  # nosec
                    wraps=target if self.wrapper else None,
                ),
                service=name,
            )
