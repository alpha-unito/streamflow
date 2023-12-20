from __future__ import annotations

from typing import MutableMapping

from jinja2 import Template


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
        environment: MutableMapping[str, str] = None,
        workdir: str = None,
        **kwargs,
    ) -> str:
        return self.templates.get(template, self.templates["__DEFAULT__"]).render(
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
