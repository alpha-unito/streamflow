from __future__ import annotations

from collections.abc import MutableMapping

from jinja2 import Template

from streamflow.core.exception import WorkflowDefinitionException


class CommandTemplateMap:
    def __init__(
        self,
        default: str,
        template_map: MutableMapping[str, str] | None = None,
    ):
        self.templates: MutableMapping[str, Template] = {
            "__DEFAULT__": Template(default)
        }
        if template_map:
            for name, template in template_map.items():
                self.templates[name] = Template(template)

    def get_command(
        self,
        command: str,
        template: str | None = None,
        environment: MutableMapping[str, str] | None = None,
        workdir: str | None = None,
        **kwargs,
    ) -> str:
        if template is not None and template not in self.templates:
            raise WorkflowDefinitionException(f'Template "{template}" is not defined.')
        return self.templates[
            template if template is not None else "__DEFAULT__"
        ].render(
            streamflow_command=command,
            streamflow_environment=(
                " && ".join(
                    [f'export {key}="{value}"' for (key, value) in environment.items()]
                )
                if environment is not None
                else ""
            ),
            streamflow_workdir=workdir,
            **kwargs,
        )

    def is_empty(self) -> bool:
        return len(self.templates) == 1
