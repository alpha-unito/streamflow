import argparse
import asyncio
import logging
import os
import sys
from typing import Any, MutableMapping, Optional, Type

from streamflow import report
from streamflow.config.config import WorkflowConfig
from streamflow.config.validator import SfValidator
from streamflow.core import utils
from streamflow.core.config import Config
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowException
from streamflow.core.workflow import Status
from streamflow.cwl.main import main as cwl_main
from streamflow.data import data_manager_classes
from streamflow.deployment import deployment_manager_classes
from streamflow.log_handler import logger
from streamflow.parser import parser
from streamflow.persistence import database_classes
from streamflow.recovery import checkpoint_manager_classes, failure_manager_classes
from streamflow.scheduling import scheduler_classes


async def _async_main(args: argparse.Namespace):
    utils.load_extensions()
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
        classes: MutableMapping[str, Type],
        instance_type: str,
        kwargs: MutableMapping[str, Any],
        enabled_by_default: bool = True) -> Any:
    config = streamflow_config.get(instance_type, None)
    if config is not None:
        enabled = config.get('enabled', enabled_by_default)
        class_name = config.get('type', 'default' if enabled else 'dummy')
        kwargs = {**kwargs, **config.get('config', {})}
    else:
        class_name = 'default' if enabled_by_default else 'dummy'
    class_ = classes[class_name]
    return class_(**kwargs)


def build_context(config_dir: str,
                  streamflow_config: MutableMapping[str, Any],
                  output_dir: Optional[str]) -> StreamFlowContext:
    context = StreamFlowContext(config_dir)
    context.checkpoint_manager = _get_instance_from_config(
        streamflow_config, checkpoint_manager_classes, 'checkpointManager',
        {'context': context}, enabled_by_default=False)
    context.database = _get_instance_from_config(
        streamflow_config, database_classes, 'database',
        {'connection': os.path.join(output_dir, '.streamflow', 'sqlite.db')})
    context.data_manager = _get_instance_from_config(
        streamflow_config, data_manager_classes, 'dataManager', {'context': context})
    context.deployment_manager = _get_instance_from_config(
        streamflow_config, deployment_manager_classes, 'deploymentManager', {'context': context})
    context.failure_manager = _get_instance_from_config(
        streamflow_config, failure_manager_classes, 'failureManager',
        {'context': context}, enabled_by_default=False)
    context.scheduler = _get_instance_from_config(
        streamflow_config.get('scheduling', {}), scheduler_classes, 'scheduler', {
            'context': context})
    return context


def main(args):
    args = parser.parse_args(args)
    if args.context == "version":
        from streamflow.version import VERSION
        print("StreamFlow version {version}".format(version=VERSION))
    elif args.context == "list":
        db = _get_instance_from_config(
            {}, database_classes, 'database', {'connection': os.path.join(args.dir, '.streamflow', 'sqlite.db')})
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
