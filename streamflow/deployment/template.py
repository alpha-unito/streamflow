from __future__ import annotations

from collections.abc import MutableMapping

from jinja2 import Environment, Template, meta

from streamflow.core.exception import WorkflowDefinitionException


def _check_template(name: str, source: str) -> None:
    ast = Environment(autoescape=True).parse(source)
    referenced_vars = meta.find_undeclared_variables(ast)
    if "streamflow_command" not in referenced_vars:
        raise WorkflowDefinitionException(
            f"Template '{name}' does not contain the "
            f"mandatory placeholder 'streamflow_command'."
        )


class CommandTemplateMap:
    def __init__(
        self,
        default: str,
        template_map: MutableMapping[str, str] | None = None,
    ):
        self.templates: MutableMapping[str, Template] = {
            "__DEFAULT__": Template(default)
        }
        _check_template("default", default)
        if template_map:
            for name, template in template_map.items():
                self.templates[name] = Template(template)
                _check_template(name, template)

    def get_command(
        self,
        command: str,
        template: str | None = None,
        environment: MutableMapping[str, str] | None = None,
        workdir: str | None = None,
        **kwargs,
    ) -> str:
        return self.templates[
            (
                template
                if template is not None and template in self.templates.keys()
                else "__DEFAULT__"
            )
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
