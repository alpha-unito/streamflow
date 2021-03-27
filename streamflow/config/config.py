from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import MutableMapping, Any, Optional
    from typing_extensions import Text


def set_targets(current_node, target):
    for node in current_node['children'].values():
        if 'target' not in node:
            node['target'] = target
        set_targets(node, node['target'])


class WorkflowConfig(object):

    def __init__(self,
                 workflow_name: Text,
                 streamflow_config: MutableMapping[Text, Any]) -> None:
        super().__init__()
        workflow_config = streamflow_config['workflows'][workflow_name]
        self.type = workflow_config['type']
        self.config = workflow_config['config']
        self.models = streamflow_config.get('models', {})
        for name, model in self.models.items():
            model['name'] = name
        self.filesystem = {'children': {}}
        for binding in workflow_config.get('bindings', []):
            current_config = self._build_config(PurePosixPath(binding['step']))
            if 'target' in binding:
                current_config['target'] = binding['target']
            if 'workdir' in binding:
                current_config['workdir'] = binding['workdir']
        set_targets(self.filesystem, None)

    def _build_config(self, path: PurePosixPath) -> MutableMapping[Text, Any]:
        current_node = self.filesystem
        for part in path.parts:
            if part not in current_node['children']:
                current_node['children'][part] = {'children': {}}
            current_node = current_node['children'][part]
        return current_node

    def get(self, path: PurePosixPath, name: Text, default: Optional[Any] = None) -> Optional[Any]:
        current_node = self.filesystem
        for part in path.parts:
            if part not in current_node['children']:
                return default
            current_node = current_node['children'][part]
        return current_node.get(name)

    def propagate(self, path: PurePosixPath, name: Text) -> Optional[Any]:
        current_node = self.filesystem
        value = None
        for part in path.parts:
            if part not in current_node['children']:
                return value
            current_node = current_node['children'][part]
            if name in current_node:
                value = current_node[name]
        return value
