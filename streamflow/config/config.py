from __future__ import annotations

from pathlib import PurePosixPath
from typing import MutableSequence, TYPE_CHECKING

from streamflow.core import utils
from streamflow.core.config import Config
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from typing import MutableMapping, Any, Optional


def set_targets(current_node, target):
    for node in current_node['children'].values():
        if 'step' not in node:
            node['step'] = target
        set_targets(node, node['step'])


class WorkflowConfig(object):

    def __init__(self,
                 workflow_name: str,
                 streamflow_config: Optional[MutableMapping[str, Any]] = None) -> None:
        super().__init__()
        workflow_config = streamflow_config['workflows'][workflow_name]
        self.type = workflow_config['type']
        self.config = workflow_config['config']
        self.deplyoments = streamflow_config.get('deployments', {})
        self.policies = {k: Config(name=k, type=v['type'], config=v['config'])
                         for k, v in streamflow_config.get('scheduling', {}).get('policies', {}).items()}
        self.policies['__DEFAULT__'] = Config(name='__DEFAULT__', type='data_locality', config={})
        if not self.deplyoments:
            self.deplyoments = streamflow_config.get('models', {})
            if self.deplyoments:
                logger.warn(
                    "The `models` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                    "Use `deployments` instead.")
        self.scheduling_groups: MutableMapping[str, MutableSequence[str]] = {}
        for name, deployment in self.deplyoments.items():
            deployment['name'] = name
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
        current_config = self._build_config(PurePosixPath(
            binding['step'] if 'step' in binding else binding['port']))
        policy = binding['target'].get(
            'policy', self.deplyoments[binding['target'].get('deployment', binding['target'].get('model', {}))].get(
                'policy', '__DEFAULT__'))
        if policy not in self.policies:
            raise WorkflowDefinitionException("Policy {} is not defined".format(policy))
        binding['target']['policy'] = self.policies[policy]
        target_type = 'step' if 'step' in binding else 'port'
        if target_type == 'port' and 'workdir' not in binding['target']:
            raise WorkflowDefinitionException(
                "The `workdir` option is mandatory when specifying a `port` target.")
        current_config[target_type] = binding['target']

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
