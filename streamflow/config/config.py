from pathlib import PurePosixPath
from typing import MutableMapping, Any, Optional


def set_targets(current_node, target):
    for node in current_node['nodes'].values():
        if 'target' not in node:
            node['target'] = target
        set_targets(node, node['target'])


class WorkflowConfig(object):

    def __init__(self, workflow_name: str,
                 streamflow_config: MutableMapping[str, Any]) -> None:
        super().__init__()
        workflow_config = streamflow_config['workflows'][workflow_name]
        self.config_file = streamflow_config['config_file']
        self.type = workflow_config['type']
        self.config = workflow_config['config']
        self.filesystem = {'nodes': {}}
        models = streamflow_config.get('models', {})
        for binding in workflow_config.get('bindings', []):
            current_config = self._build_config(PurePosixPath(binding['step']))
            if 'deployments' in binding:
                expanded_models = {}
                for model_name in binding['deployments']:
                    expanded_models[model_name] = models[model_name]
                    expanded_models[model_name]['name'] = model_name
                current_config['deployments'] = expanded_models
            if 'target' in binding:
                current_config['target'] = binding['target']
        set_targets(self.filesystem, None)

    def _build_config(self, path: PurePosixPath) -> MutableMapping[str, Any]:
        current_node = self.filesystem
        for part in path.parts:
            if part not in current_node['nodes']:
                current_node['nodes'][part] = {'nodes': {}}
            current_node = current_node['nodes'][part]
        return current_node

    def get(self, path: PurePosixPath, name: str) -> Optional[Any]:
        current_node = self.filesystem
        for part in path.parts:
            if part not in current_node['nodes']:
                return None
            current_node = current_node['nodes'][part]
        return current_node.get(name)

    def propagate(self, path: PurePosixPath, name: str) -> Optional[Any]:
        current_node = self.filesystem
        value = None
        for part in path.parts:
            if part not in current_node['nodes']:
                return value
            current_node = current_node['nodes'][part]
            if name in current_node:
                value = current_node[name]
        return value
