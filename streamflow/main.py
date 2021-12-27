import argparse
import asyncio
import importlib
import logging
import os
import sys
from typing import MutableMapping, Any, Optional

from streamflow import report
from streamflow.config.config import WorkflowConfig
from streamflow.config.validator import SfValidator
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowException
from streamflow.core.utils import get_local_target
from streamflow.cwl.main import main as cwl_main
from streamflow.log_handler import logger, profile
from streamflow.parser import parser
from streamflow.scheduling.policy import DataLocalityPolicy

_DEFAULTS = {
    'checkpointManager': 'streamflow.recovery.checkpoint_manager.DefaultCheckpointManager',
    'dataManager': 'streamflow.data.data_manager.DefaultDataManager',
    'db': 'streamflow.persistence.sqlite.SqliteDatabase',
    'deploymentManager': 'streamflow.deployment.deployment_manager.DefaultDeploymentManager',
    'failureManager': 'streamflow.recovery.failure_manager.DefaultFailureManager',
    'persistenceManager': 'streamflow.persistence.persistence_manager.DefaultPersistenceManager',
    'scheduler': 'streamflow.scheduling.scheduler.DefaultScheduler'
}

_DISABLED = {
    'checkpointManager': 'streamflow.recovery.checkpoint_manager.DummyCheckpointManager',
    'failureManager': 'streamflow.recovery.failure_manager.DummyFailureManager'
}


@profile
async def _async_main(args: argparse.Namespace):
    streamflow_config = SfValidator().validate_file(args.streamflow_file)
    context = build_context(os.path.dirname(args.streamflow_file), streamflow_config, args.outdir)
    try:
        local_target = get_local_target()
        await context.deployment_manager.deploy(local_target.model)
        workflow_tasks = []
        for workflow in streamflow_config.get('workflows', {}):
            workflow_config = WorkflowConfig(workflow, streamflow_config)
            if workflow_config.type == 'cwl':
                workflow_tasks.append(asyncio.create_task(cwl_main(workflow_config, context, args)))
            await asyncio.gather(*workflow_tasks)
    except WorkflowException as e:
        logger.error(e)
        sys.exit(1)
    except BaseException as e:
        logger.exception(e)
        sys.exit(1)
    finally:
        await context.deployment_manager.undeploy_all()


def _get_instance_from_config(
        streamflow_config: MutableMapping[str, Any],
        instance_type: str,
        kwargs: MutableMapping[str, Any],
        enabled_by_default: bool = True) -> Any:
    config = streamflow_config.get(instance_type, None)
    if config is not None:
        enabled = config.get('enabled', enabled_by_default)
        class_name = config.get('type', _DEFAULTS[instance_type] if enabled else _DISABLED[instance_type])
        kwargs = {**kwargs, **config.get('config', {})}
    else:
        class_name = _DEFAULTS[instance_type] if enabled_by_default else _DISABLED[instance_type]
    if instance_type == 'persistenceManager':
        del kwargs['context']
        kwargs['db'] = _get_instance_from_config(
            config or {},
            'db',
            {'connection': os.path.join(kwargs['output_dir'], '.streamflow', 'sqlite.db')})
    module_name, _, class_simplename = class_name.rpartition('.')
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_simplename)
    return class_(**kwargs)


def build_context(config_dir: str,
                  streamflow_config: MutableMapping[str, Any],
                  output_dir: Optional[str]) -> StreamFlowContext:
    context = StreamFlowContext(config_dir)
    context.checkpoint_manager = _get_instance_from_config(
        streamflow_config, 'checkpointManager', {'context': context}, enabled_by_default=False)
    context.data_manager = _get_instance_from_config(
        streamflow_config, 'dataManager', {'context': context})
    context.deployment_manager = _get_instance_from_config(
        streamflow_config, 'deploymentManager', {'streamflow_config_dir': config_dir})
    context.failure_manager = _get_instance_from_config(
        streamflow_config, 'failureManager', {'context': context}, enabled_by_default=False)
    context.persistence_manager = _get_instance_from_config(
        streamflow_config, 'persistenceManager', {'context': context, 'output_dir': output_dir})
    context.scheduler = _get_instance_from_config(
        streamflow_config, 'scheduler', {'context': context, 'default_policy': DataLocalityPolicy()})
    return context


def main(args):
    args = parser.parse_args(args)
    if args.context == "version":
        from streamflow.version import VERSION
        print("StreamFlow version {version}".format(version=VERSION))
    elif args.context == "run":
        if args.quiet:
            logger.setLevel(logging.WARN)
        asyncio.run(_async_main(args))
    elif args.context == "report":
        report.create_report(args)
    else:
        raise Exception


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
