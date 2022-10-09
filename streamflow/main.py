import argparse
import asyncio
import logging
import os
import sys
import uuid
from typing import Any, MutableMapping, Optional, Type

from streamflow import report
from streamflow.config.config import WorkflowConfig
from streamflow.config.validator import SfValidator
from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowException
from streamflow.cwl.main import main as cwl_main
from streamflow.data import data_manager_classes
from streamflow.deployment import deployment_manager_classes
from streamflow.log_handler import logger
from streamflow.parser import parser
from streamflow.persistence import database_classes
from streamflow.recovery import checkpoint_manager_classes, failure_manager_classes
from streamflow.scheduling import scheduler_classes


async def _async_list(args: argparse.Namespace):
    if os.path.exists(args.streamflow_file):
        streamflow_config = SfValidator().validate_file(args.streamflow_file)
        context = build_context(os.path.dirname(args.streamflow_file), streamflow_config, args.outdir)
    else:
        context = build_context(os.getcwd(), {}, args.outdir)
    try:
        workflows = await context.database.list_workflows()
        if workflows:
            max_sizes = {k: len(max(w[k] for w in workflows)) for k in workflows[0].keys()}
            format_string = ("{:<" + str(max(max_sizes['name'], 4)) + "} " +
                             "{:<" + str(max(max_sizes['type'], 4)) + "} " +
                             "{:<" + str(max(max_sizes['status'], 6)) + "}")
            print(format_string.format('NAME', 'TYPE', 'STATUS'))
            for w in workflows:
                print(format_string.format(w['name'], w['type'], w['status']))
        else:
            print("No workflow objects found.")
    finally:
        await context.close()


async def _async_main(args: argparse.Namespace):
    args.name = args.name or str(uuid.uuid4())
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
    finally:
        await context.close()


async def _async_report(args: argparse.Namespace):
    if os.path.exists(args.streamflow_file):
        streamflow_config = SfValidator().validate_file(args.streamflow_file)
        context = build_context(os.path.dirname(args.streamflow_file), streamflow_config, args.outdir)
    else:
        context = build_context(os.getcwd(), {}, args.outdir)
    try:
        await report.create_report(context, args)
    finally:
        await context.close()


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
                  output_dir: Optional[str] = os.getcwd()) -> StreamFlowContext:
    context = StreamFlowContext(config_dir)
    context.checkpoint_manager = _get_instance_from_config(
        streamflow_config, checkpoint_manager_classes, 'checkpointManager',
        {'context': context}, enabled_by_default=False)
    context.database = _get_instance_from_config(
        streamflow_config, database_classes, 'database',
        {'context': context, 'connection': os.path.join(output_dir, '.streamflow', 'sqlite.db')})
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
    try:
        args = parser.parse_args(args)
        if args.context == "version":
            from streamflow.version import VERSION
            print("StreamFlow version {version}".format(version=VERSION))
        elif args.context == "list":
            asyncio.run(_async_list(args))
        elif args.context == "report":
            asyncio.run(_async_report(args))
        elif args.context == "run":
            if args.quiet:
                logger.setLevel(logging.WARN)
            elif args.debug:
                logger.setLevel(logging.DEBUG)
            asyncio.run(_async_main(args))
        else:
            raise Exception("Context {} not supported.".format(args.context))
        return 0
    except BaseException as e:
        logger.exception(e)
        return 1


def run():
    return main(sys.argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
