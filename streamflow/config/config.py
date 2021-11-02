from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING, MutableSequence

from streamflow.core import utils

if TYPE_CHECKING:
    from typing import MutableMapping, Any, Optional


def set_targets(current_node, target):
    for node in current_node['children'].values():
        if 'target' not in node:
            node['target'] = target
        set_targets(node, node['target'])


class WorkflowConfig(object):

    def __init__(self,
                 workflow_name: str,
                 streamflow_config: Optional[MutableMapping[str, Any]] = None) -> None:
        super().__init__()
        workflow_config = streamflow_config['workflows'][workflow_name]
        self.type = workflow_config['type']
        self.config = workflow_config['config']
        self.models = streamflow_config.get('models', {})
        self.scheduling_groups: MutableMapping[str, MutableSequence[str]] = {}
        for name, model in self.models.items():
            model['name'] = name
        self.filesystem = {'children': {}}
        for binding in workflow_config.get('bindings', []):
            if isinstance(binding, MutableSequence):
                for b in binding:
                    self._process_binding(b)
                self.scheduling_groups[utils.random_name()] = binding
            else:
                self._process_binding(binding)
        set_targets(self.filesystem, None)

    def _build_config(self, path: PurePosixPath) -> MutableMapping[str, Any]:
        current_node = self.filesystem
        for part in path.parts:
            if part not in current_node['children']:
                current_node['children'][part] = {'children': {}}
            current_node = current_node['children'][part]
        return current_node

    def _process_binding(self, binding: MutableMapping[str, Any]):
        current_config = self._build_config(PurePosixPath(binding['step']))
        if 'target' in binding:
            current_config['target'] = binding['target']
        if 'workdir' in binding:
            current_config['workdir'] = binding['workdir']

    def get(self, path: PurePosixPath, name: str, default: Optional[Any] = None) -> Optional[Any]:
        current_node = self.filesystem
        for part in path.parts:
            if part not in current_node['children']:
                return default
            current_node = current_node['children'][part]
        return current_node.get(name)

    def propagate(self, path: PurePosixPath, name: str) -> Optional[Any]:
        current_node = self.filesystem
        value = None
        for part in path.parts:
            if part not in current_node['children']:
                return value
            current_node = current_node['children'][part]
            if name in current_node:
                value = current_node[name]
        return value
