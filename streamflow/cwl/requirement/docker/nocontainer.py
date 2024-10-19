from __future__ import annotations

from importlib.resources import files

from streamflow.core.deployment import Target
from streamflow.cwl.requirement.docker.translator import CWLDockerTranslator


class NoContainerCWLDockerTranslator(CWLDockerTranslator):
    def __init__(
        self,
        config_dir: str,
        wrapper: bool,
    ):
        super().__init__(config_dir=config_dir, wrapper=wrapper)

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("no-container.json")
            .read_text("utf-8")
        )

    def get_target(
        self,
        image: str,
        output_directory: str | None,
        network_access: bool,
        target: Target,
    ) -> Target:
        return target
