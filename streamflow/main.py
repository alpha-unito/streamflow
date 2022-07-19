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
from streamflow.core.workflow import Status
from streamflow.cwl.main import main as cwl_main
from streamflow.log_handler import logger
from streamflow.parser import parser
from streamflow.scheduling.policy import DataLocalityPolicy

_DEFAULTS = {
    'checkpointManager': 'streamflow.recovery.checkpoint_manager.DefaultCheckpointManager',
    'dataManager': 'streamflow.data.data_manager.DefaultDataManager',
    'db': 'streamflow.persistence.sqlite.SqliteDatabase',
    'deploymentManager': 'streamflow.deployment.deployment_manager.DefaultDeploymentManager',
    'failureManager': 'streamflow.recovery.failure_manager.DefaultFailureManager',
    'scheduler': 'streamflow.scheduling.scheduler.DefaultScheduler'
}

_DISABLED = {
    'checkpointManager': 'streamflow.recovery.checkpoint_manager.DummyCheckpointManager',
    'failureManager': 'streamflow.recovery.failure_manager.DummyFailureManager'
}


async def _async_main(args: argparse.Namespace):
    streamflow_config = SfValidator().validate_file(args.streamflow_file)
    context = build_context(os.path.dirname(args.streamflow_file), streamflow_config, args.outdir)
    try:
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
    context.database = _get_instance_from_config(
        streamflow_config, 'db', {'connection': os.path.join(output_dir, '.streamflow', 'sqlite.db')})
    context.data_manager = _get_instance_from_config(
        streamflow_config, 'dataManager', {'context': context})
    context.deployment_manager = _get_instance_from_config(
        streamflow_config, 'deploymentManager', {'context': context})
    context.failure_manager = _get_instance_from_config(
        streamflow_config, 'failureManager', {'context': context}, enabled_by_default=False)
    context.scheduler = _get_instance_from_config(
        streamflow_config, 'scheduler', {'context': context, 'default_policy': DataLocalityPolicy()})
    return context


def main(args):
    args = parser.parse_args(args)
    if args.context == "version":
        from streamflow.version import VERSION
        print("StreamFlow version {version}".format(version=VERSION))
    elif args.context == "list":
        db = _get_instance_from_config(
            {}, 'db', {'connection': os.path.join(args.dir, '.streamflow', 'sqlite.db')})
        workflows = db.get_workflows()
        workflows['status'] = workflows['status'].transform(lambda x: Status(x).name)
        print(workflows.to_string(index=False))
    elif args.context == "run":
        if args.quiet:
            logger.setLevel(logging.WARN)
        elif args.debug:
            logger.setLevel(logging.DEBUG)
        asyncio.run(_async_main(args))
    elif args.context == "report":
        report.create_report(args)
    else:
        raise Exception


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
