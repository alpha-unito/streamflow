import argparse
import asyncio
import logging
import os
import sys
import uuid

import streamflow.cwl.main
from streamflow.config.config import WorkflowConfig
from streamflow.config.validator import SfValidator
from streamflow.core import utils
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.log_handler import logger
from streamflow.main import build_context

parser = argparse.ArgumentParser(description='cwl-runner interface')
parser.add_argument('processfile', nargs='?', type=str,
                    help='The CommandLineTool, ExpressionTool, or Workflow description to run. '
                         'Optional if the jobfile has a `cwl:tool` field to indicate which process description to run')
parser.add_argument('jobfile', nargs='?', type=str,
                    help='The input job document')
parser.add_argument('--name', nargs='?', type=str,
                    help='Name of the current workflow. Will be used for search and indexing')
parser.add_argument('--outdir', default=os.getcwd(), type=str,
                    help='Output directory, defaults to the current directory')
parser.add_argument('--debug', action='store_true', help='Debug-level diagnostic output')
parser.add_argument('--quiet', action='store_true', help='No diagnostic output')
parser.add_argument('--validate', action='store_true', help='Validate CWL document only')
parser.add_argument('--version', action='store_true',
                    help='Report the name and version, then quit without further processing')
parser.add_argument('--streamflow-file', type=str,
                    help='The path to a StreamFlow file specifying deployments and bindings for the workflow steps')


async def _async_main(args: argparse.Namespace):
    validator = SfValidator()
    config_dir = os.getcwd()
    args.name = args.name or str(uuid.uuid4())
    if args.streamflow_file:
        utils.load_extensions()
        with open(args.streamflow_file) as f:
            streamflow_config = validator.yaml.load(f)
        config_dir = os.path.dirname(args.streamflow_file)
        workflows = streamflow_config.get('workflows', {})
        if len(workflows) == 1:
            workflow_name = list(workflows.keys())[0]
        elif len(workflows) == 0:
            workflow_name = 'cwl-workflow'
            streamflow_config['workflows'][workflow_name] = {}
        else:
            raise WorkflowDefinitionException(
                "A StreamFlow file must contain only one workflow definition when used with cwl-runner.")
    else:
        workflow_name = 'cwl-workflow'
        streamflow_config = {
            'version': 'v1.0',
            'workflows': {
                workflow_name: {}
            }
        }
    streamflow_config['workflows'][workflow_name]['type'] = 'cwl'
    streamflow_config['workflows'][workflow_name]['config'] = {
        'file': args.processfile
    }
    if args.jobfile:
        streamflow_config['workflows'][workflow_name]['config']['settings'] = args.jobfile
    validator.validate(streamflow_config)
    workflow_config = WorkflowConfig(workflow_name, streamflow_config)
    context = build_context(config_dir, streamflow_config, args.outdir)
    try:
        await streamflow.cwl.main.main(
            workflow_config=workflow_config,
            context=context,
            args=args)
    finally:
        await context.close()


def main(args):
    try:
        args = parser.parse_args(args)
        if args.version:
            from streamflow.version import VERSION
            print("StreamFlow version {version}".format(version=VERSION))
            return
        if args.quiet:
            logger.setLevel(logging.WARN)
        elif args.debug:
            logger.setLevel(logging.DEBUG)
        asyncio.run(_async_main(args))
        return 0
    except BaseException as e:
        logger.exception(e)
        return 1


def run():
    return main(sys.argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
