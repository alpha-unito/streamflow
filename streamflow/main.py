import asyncio
import os
import platform
import sys

import uvloop
from typing_extensions import Text

from streamflow.config.config import WorkflowConfig
from streamflow.config.validator import SfValidator
from streamflow.core.context import StreamflowContext
from streamflow.cwl.main import main as cwl_main
from streamflow.data.data_manager import DefaultDataManager
from streamflow.deployment.deployment_manager import DefaultDeploymentManager
from streamflow.log_handler import logger
from streamflow.parser import parser
from streamflow.scheduling.policy import DataLocalityPolicy
from streamflow.scheduling.scheduler import DefaultScheduler


async def _async_main(args):
    streamflow_config = SfValidator().validate(args.streamflow_file)
    context = _get_context(args.streamflow_file)
    try:
        workflow_tasks = []
        for workflow in streamflow_config.get('workflows', {}):
            workflow_config = WorkflowConfig(workflow, streamflow_config)
            if workflow_config.type == 'cwl':
                workflow_tasks.append(asyncio.create_task(cwl_main(workflow_config, context, args.outdir)))
            await asyncio.gather(*workflow_tasks)
    finally:
        await context.deployment_manager.undeploy_all()


def _get_context(streamflow_file: Text) -> StreamflowContext:
    config_dir = os.path.dirname(streamflow_file)
    context = StreamflowContext(config_dir)
    context.data_manager = DefaultDataManager(context)
    context.deployment_manager = DefaultDeploymentManager(config_dir)
    context.scheduler = DefaultScheduler(context, DataLocalityPolicy())
    return context


def main(args):
    args = parser.parse_args(args)
    if args.version:
        from streamflow.version import VERSION
        print("StreamFlow version {version}".format(version=VERSION))
        exit()
    if platform.python_implementation() == 'CPython':
        logger.info('CPython detected: using uvloop EventLoop implementation')
        uvloop.install()
    asyncio.run(_async_main(args))


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
